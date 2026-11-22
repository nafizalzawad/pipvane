"""
Pipvane v3 Trade Orchestrator & Execution Manager.

Implements core trade management functions:
1. TP Ladder Splitting (Hedging Mode):
   Splits multi-target master orders into discrete sub-orders according to
   pre-configured or dynamic volume ratios (e.g. 50% / 30% / 20% for 3 TPs).
2. Virtual Netting Engine (MT5 Netting Mode):
   In Netting accounts (supports_hedging = 0), holds Take Profit ladders virtually
   in memory to prevent broker-side position overwrites. Issues ClosePartial commands
   as market price ticks cross virtual TP thresholds.
3. Dynamic Break-Even & Risk Guard Modifications:
   Monitors tick updates and automatically dispatches OrderModify commands to
   secure Stop Loss at exact Entry Price (or entry + offset) when break-even criteria are met.
"""

from __future__ import annotations

import enum
import time
import logging
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Callable, Any, Tuple

from engine.ipc_protocol import (
    OrderIntent,
    OrderSide,
    ActionType,
    ExecutionStatus,
    ExecutionFeedback,
)
from engine.ipc_dispatcher import IPCDispatcher

logger = logging.getLogger("pipvane.engine.orchestrator")


class AccountMode(enum.Enum):
    """Broker account position architecture."""
    HEDGING = "HEDGING"   # Multiple independent positions per instrument permitted (MT4 & MT5 Hedging)
    NETTING = "NETTING"   # Single aggregate position per instrument (MT5 Netting)


class BreakEvenTriggerType(enum.Enum):
    """Break-Even activation rule."""
    PRICE = "PRICE"       # Specific price boundary crossed
    PIPS = "PIPS"         # Favorable movement in pips (e.g., +20 pips)
    TP1_HIT = "TP1_HIT"   # Move SL to Entry immediately upon TP1 execution


@dataclass
class TradeIntent:
    """Master trading intent prior to ladder decomposition and orchestration."""
    symbol: str
    side: OrderSide
    volume: float
    price: float = 0.0
    stop_loss: float = 0.0
    tp_ladder: List[float] = field(default_factory=list)
    slippage_cap_pips: float = 1.5
    magic_number: int = 999888
    signal_id: int = 0
    ladder_ratios: Optional[List[float]] = None
    break_even_pips: Optional[float] = None
    break_even_trigger_price: Optional[float] = None
    be_offset_pips: float = 0.0
    account_mode: Optional[AccountMode] = None


@dataclass
class ManagedPosition:
    """Tracks active and virtual state for an orchestrated trade."""
    ticket: int
    symbol: str
    side: OrderSide
    initial_volume: float
    current_volume: float
    entry_price: float
    stop_loss: float
    tp_ladder: List[float] = field(default_factory=list)
    tp_volumes: List[float] = field(default_factory=list)
    tp_triggered: List[bool] = field(default_factory=list)
    account_mode: AccountMode = AccountMode.HEDGING
    magic_number: int = 999888
    signal_id: int = 0
    # Break-even tracking
    break_even_enabled: bool = False
    break_even_trigger_price: Optional[float] = None
    break_even_pips: Optional[float] = None
    be_offset_pips: float = 0.0
    break_even_triggered: bool = False
    is_closed: bool = False


# Default standard ladder distribution ratios
DEFAULT_LADDER_RATIOS: Dict[int, List[float]] = {
    1: [1.0],
    2: [0.5, 0.5],
    3: [0.5, 0.3, 0.2],
    4: [0.4, 0.3, 0.2, 0.1],
    5: [0.3, 0.25, 0.2, 0.15, 0.1],
}

DEFAULT_PIP_SIZES: Dict[str, float] = {
    "EURUSD": 0.0001,
    "GBPUSD": 0.0001,
    "AUDUSD": 0.0001,
    "NZDUSD": 0.0001,
    "USDCAD": 0.0001,
    "USDCHF": 0.0001,
    "USDJPY": 0.01,
    "GBPJPY": 0.01,
    "EURJPY": 0.01,
    "XAUUSD": 0.10,
    "GOLD": 0.10,
    "BTCUSD": 1.0,
    "US30": 1.0,
    "NAS100": 1.0,
}


def calculate_ladder_volumes(total_volume: float, ratios: List[float]) -> List[float]:
    """
    Decomposes total volume across ladder ratios with 0.01 lot precision.
    Guarantees sum(volumes) == total_volume without rounding leakage.
    """
    if not ratios:
        return [round(total_volume, 2)]

    # Normalize ratios to sum to 1.0
    sum_ratios = sum(ratios)
    if sum_ratios <= 0:
        norm_ratios = [1.0 / len(ratios)] * len(ratios)
    else:
        norm_ratios = [r / sum_ratios for r in ratios]

    raw_volumes = [round(total_volume * r, 2) for r in norm_ratios]
    diff = round(total_volume - sum(raw_volumes), 2)
    if diff != 0:
        # Assign residual rounding discrepancy to first tranche
        raw_volumes[0] = round(raw_volumes[0] + diff, 2)

    return raw_volumes


class TradeOrchestrator:
    """
    Core Execution Orchestrator responsible for TP ladder splitting,
    MT5 netting virtualization, and automated break-even management.
    """

    def __init__(
        self,
        account_mode: AccountMode = AccountMode.HEDGING,
        dispatcher: Optional[IPCDispatcher] = None,
        terminal_id: str = "default_terminal",
        pip_sizes: Optional[Dict[str, float]] = None,
    ) -> None:
        self.account_mode = account_mode
        self.dispatcher = dispatcher
        self.terminal_id = terminal_id
        self.pip_sizes = dict(DEFAULT_PIP_SIZES)
        if pip_sizes:
            self.pip_sizes.update(pip_sizes)

        # Active managed positions: ticket -> ManagedPosition
        self._positions: Dict[int, ManagedPosition] = {}
        # Signal ID -> ManagedPosition (before ticket is assigned)
        self._pending_positions: Dict[int, ManagedPosition] = {}
        self._next_virtual_ticket = 900000

        # Dispatched broker calls history for inspection/testing
        self.dispatched_calls: List[OrderIntent] = []
        self._broker_call_listeners: List[Callable[[OrderIntent], None]] = []

    def add_broker_call_listener(self, listener: Callable[[OrderIntent], None]) -> None:
        """Subscribes a listener to receive dispatched broker OrderIntent objects."""
        self._broker_call_listeners.append(listener)

    def get_pip_size(self, symbol: str) -> float:
        """Returns standard pip value for instrument."""
        cleaned = symbol.upper().replace(".RAW", "").replace(".A", "").replace("!", "")
        return self.pip_sizes.get(cleaned, 0.0001)

    def orchestrate_order(
        self,
        intent: TradeIntent | OrderIntent,
        tp_ladder: Optional[List[float]] = None,
        ladder_ratios: Optional[List[float]] = None,
        account_mode: Optional[AccountMode] = None,
        break_even_pips: Optional[float] = None,
        break_even_trigger_price: Optional[float] = None,
        be_offset_pips: float = 0.0,
    ) -> List[OrderIntent]:
        """
        Decomposes and dispatches a trading intent.
        - If HEDGING: Splits order into multiple broker calls (one per TP ladder level).
        - If NETTING: Dispatches a single full-sized position and registers virtual TPs in memory.
        """
        mode = account_mode or self.account_mode

        # Normalize incoming intent parameters
        if isinstance(intent, OrderIntent):
            symbol = intent.symbol
            side = intent.side
            total_vol = intent.volume
            price = intent.price
            stop_loss = intent.stop_loss
            tps = list(tp_ladder or ([intent.take_profit] if intent.take_profit > 0 else []))
            ratios = ladder_ratios
            magic = intent.magic_number
            sig_id = intent.signal_id or int(time.time_ns() & 0x7FFFFFFFFFFFFFFF)
            slip = intent.slippage_cap_pips
            be_pips = break_even_pips
            be_price = break_even_trigger_price
            be_offset = be_offset_pips
        else:
            symbol = intent.symbol
            side = intent.side
            total_vol = intent.volume
            price = intent.price
            stop_loss = intent.stop_loss
            tps = list(tp_ladder or intent.tp_ladder)
            ratios = ladder_ratios or intent.ladder_ratios
            magic = intent.magic_number
            sig_id = intent.signal_id or int(time.time_ns() & 0x7FFFFFFFFFFFFFFF)
            slip = intent.slippage_cap_pips
            be_pips = break_even_pips or intent.break_even_pips
            be_price = break_even_trigger_price or intent.break_even_trigger_price
            be_offset = be_offset_pips or intent.be_offset_pips
            if intent.account_mode:
                mode = intent.account_mode

        # Fallback ratio selection
        num_tps = len(tps)
        if not ratios:
            ratios = DEFAULT_LADDER_RATIOS.get(num_tps, [1.0 / num_tps] * num_tps if num_tps > 0 else [1.0])

        volumes = calculate_ladder_volumes(total_vol, ratios)
        generated_calls: List[OrderIntent] = []

        if mode == AccountMode.HEDGING:
            # ==================================================================
            # Hedging Mode: Discrete Broker Calls per TP level
            # ==================================================================
            if num_tps == 0:
                # Single position without TP
                call = OrderIntent(
                    symbol=symbol,
                    side=side,
                    action_type=ActionType.MARKET_ENTRY,
                    volume=total_vol,
                    price=price,
                    stop_loss=stop_loss,
                    take_profit=0.0,
                    slippage_cap_pips=slip,
                    magic_number=magic,
                    signal_id=sig_id,
                    timestamp_ns=time.time_ns(),
                )
                generated_calls.append(call)
                self._dispatch(call)
            else:
                for idx, (vol, tp) in enumerate(zip(volumes, tps)):
                    sub_sig_id = int(f"{sig_id}{idx + 1}") if sig_id < 10_000_000_000 else (sig_id + idx + 1)
                    sub_call = OrderIntent(
                        symbol=symbol,
                        side=side,
                        action_type=ActionType.MARKET_ENTRY,
                        volume=vol,
                        price=price,
                        stop_loss=stop_loss,
                        take_profit=tp,
                        slippage_cap_pips=slip,
                        magic_number=magic,
                        signal_id=sub_sig_id,
                        timestamp_ns=time.time_ns(),
                    )
                    generated_calls.append(sub_call)
                    self._dispatch(sub_call)

                    # Track position state for break-even monitoring
                    pos = ManagedPosition(
                        ticket=sub_sig_id,  # Temporary ticket until confirmed
                        symbol=symbol,
                        side=side,
                        initial_volume=vol,
                        current_volume=vol,
                        entry_price=price,
                        stop_loss=stop_loss,
                        tp_ladder=[tp],
                        tp_volumes=[vol],
                        tp_triggered=[False],
                        account_mode=AccountMode.HEDGING,
                        magic_number=magic,
                        signal_id=sub_sig_id,
                        break_even_enabled=bool(be_pips or be_price or (idx == 0 and num_tps > 1)),
                        break_even_trigger_price=be_price or (tps[0] if (idx > 0 and len(tps) > 1) else None),
                        break_even_pips=be_pips,
                        be_offset_pips=be_offset,
                    )
                    self._positions[sub_sig_id] = pos

        else:
            # ==================================================================
            # Netting Mode: Single Broker Call + Virtual Take Profits in Memory
            # ==================================================================
            # Dispatches single order with full volume. TP is held virtually in memory!
            netting_call = OrderIntent(
                symbol=symbol,
                side=side,
                action_type=ActionType.MARKET_ENTRY,
                volume=total_vol,
                price=price,
                stop_loss=stop_loss,
                take_profit=0.0,  # Zero / unplaced on broker: held virtually by Orchestrator
                slippage_cap_pips=slip,
                magic_number=magic,
                signal_id=sig_id,
                timestamp_ns=time.time_ns(),
            )
            generated_calls.append(netting_call)
            self._dispatch(netting_call)

            # Virtual ticket allocation
            self._next_virtual_ticket += 1
            ticket = self._next_virtual_ticket

            # Register virtual position holding all TPs in memory
            virtual_pos = ManagedPosition(
                ticket=ticket,
                symbol=symbol,
                side=side,
                initial_volume=total_vol,
                current_volume=total_vol,
                entry_price=price,
                stop_loss=stop_loss,
                tp_ladder=tps,
                tp_volumes=volumes,
                tp_triggered=[False] * len(tps),
                account_mode=AccountMode.NETTING,
                magic_number=magic,
                signal_id=sig_id,
                break_even_enabled=bool(be_pips or be_price or len(tps) > 1),
                break_even_trigger_price=be_price or (tps[0] if len(tps) > 0 else None),
                break_even_pips=be_pips,
                be_offset_pips=be_offset,
            )
            self._positions[ticket] = virtual_pos
            self._pending_positions[sig_id] = virtual_pos

        return generated_calls

    def on_price_tick(self, symbol: str, bid: float, ask: float) -> List[OrderIntent]:
        """
        Evaluates incoming market prices against virtual TPs and Break-Even rules.
        Dispatches ClosePartial or OrderModify commands when thresholds are crossed.
        """
        triggered_commands: List[OrderIntent] = []
        pip_size = self.get_pip_size(symbol)

        for pos in list(self._positions.values()):
            if pos.symbol != symbol or pos.is_closed or pos.current_volume <= 0:
                continue

            # Relevant exit price: closing BUY uses BID; closing SELL uses ASK
            market_price = bid if pos.side == OrderSide.BUY else ask

            # ------------------------------------------------------------------
            # 1. Virtual Netting: Evaluate Untriggered Virtual Take Profits
            # ------------------------------------------------------------------
            if pos.account_mode == AccountMode.NETTING:
                for idx, (tp_price, tp_vol) in enumerate(zip(pos.tp_ladder, pos.tp_volumes)):
                    if pos.tp_triggered[idx]:
                        continue

                    # Check if market price crossed virtual TP threshold
                    is_hit = False
                    if pos.side == OrderSide.BUY and market_price >= tp_price:
                        is_hit = True
                    elif pos.side == OrderSide.SELL and market_price <= tp_price:
                        is_hit = True

                    if is_hit:
                        pos.tp_triggered[idx] = True
                        close_volume = min(tp_vol, pos.current_volume)

                        # Determine if this partial close leaves remainder
                        is_final = (pos.current_volume - close_volume) < 0.005 or (idx == len(pos.tp_ladder) - 1)
                        action = ActionType.FULL_CLOSE if is_final else ActionType.PARTIAL_CLOSE

                        close_cmd = OrderIntent(
                            symbol=pos.symbol,
                            side=pos.side,
                            action_type=action,
                            volume=round(close_volume, 2),
                            price=market_price,
                            order_ticket=pos.ticket,
                            magic_number=pos.magic_number,
                            signal_id=int(f"{pos.signal_id}0{idx + 1}"),
                            timestamp_ns=time.time_ns(),
                        )
                        pos.current_volume = round(max(0.0, pos.current_volume - close_volume), 2)
                        if is_final or pos.current_volume == 0:
                            pos.is_closed = True

                        triggered_commands.append(close_cmd)
                        self._dispatch(close_cmd)
                        logger.info(
                            "VIRTUAL NETTING TP%d HIT (%s): Issued %s %.2f Lots on ticket #%d",
                            idx + 1, pos.symbol, "FULL_CLOSE" if is_final else "ClosePartial",
                            close_volume, pos.ticket
                        )

            # ------------------------------------------------------------------
            # 2. Break-Even Evaluation: Move SL to exact Entry Price
            # ------------------------------------------------------------------
            if pos.break_even_enabled and not pos.break_even_triggered and not pos.is_closed:
                be_condition_met = False

                # A. Direct Price Threshold Check
                if pos.break_even_trigger_price is not None:
                    if pos.side == OrderSide.BUY and market_price >= pos.break_even_trigger_price:
                        be_condition_met = True
                    elif pos.side == OrderSide.SELL and market_price <= pos.break_even_trigger_price:
                        be_condition_met = True

                # B. Pip Gain Threshold Check
                if not be_condition_met and pos.break_even_pips is not None and pos.entry_price > 0:
                    pips_gain = (
                        (market_price - pos.entry_price) / pip_size
                        if pos.side == OrderSide.BUY
                        else (pos.entry_price - market_price) / pip_size
                    )
                    if pips_gain >= pos.break_even_pips:
                        be_condition_met = True

                # C. TP1 Hit Automatic Break-Even Trigger
                if not be_condition_met and len(pos.tp_triggered) > 0 and pos.tp_triggered[0]:
                    be_condition_met = True

                if be_condition_met:
                    # Calculate new Stop Loss: exact Entry Price (+ optional offset)
                    new_sl = round(
                        pos.entry_price + (pos.be_offset_pips * pip_size if pos.side == OrderSide.BUY else -pos.be_offset_pips * pip_size),
                        5
                    )
                    pos.break_even_triggered = True
                    pos.stop_loss = new_sl

                    modify_cmd = OrderIntent(
                        symbol=pos.symbol,
                        side=pos.side,
                        action_type=ActionType.MODIFY_SL_TP,
                        volume=pos.current_volume,
                        price=pos.entry_price,
                        stop_loss=new_sl,
                        take_profit=pos.tp_ladder[-1] if pos.tp_ladder else 0.0,
                        order_ticket=pos.ticket,
                        magic_number=pos.magic_number,
                        signal_id=pos.signal_id,
                        timestamp_ns=time.time_ns(),
                    )
                    triggered_commands.append(modify_cmd)
                    self._dispatch(modify_cmd)
                    logger.info(
                        "BREAK-EVEN TRIGGERED (%s): Dispatched OrderModify ticket #%d SL -> %.5f (Entry: %.5f)",
                        pos.symbol, pos.ticket, new_sl, pos.entry_price
                    )

        return triggered_commands

    def on_execution_feedback(self, terminal_id: str, feedback: ExecutionFeedback) -> None:
        """Updates internal ticket and execution states from broker feedback."""
        pos = self._pending_positions.pop(feedback.signal_id, None)
        if pos:
            pos.ticket = feedback.order_ticket
            if feedback.executed_price > 0:
                pos.entry_price = feedback.executed_price
            self._positions[feedback.order_ticket] = pos
        elif feedback.order_ticket in self._positions:
            pos = self._positions[feedback.order_ticket]
            if feedback.execution_status == ExecutionStatus.FILLED and feedback.executed_price > 0:
                pos.entry_price = feedback.executed_price

    def register_open_position(self, pos: ManagedPosition) -> None:
        """Manually registers an open position for testing / state synchronization."""
        self._positions[pos.ticket] = pos

    def get_position(self, ticket: int) -> Optional[ManagedPosition]:
        """Retrieves a managed position by ticket."""
        return self._positions.get(ticket)

    def _dispatch(self, order: OrderIntent) -> None:
        """Dispatches OrderIntent to broker listeners and IPCDispatcher."""
        self.dispatched_calls.append(order)
        for listener in self._broker_call_listeners:
            try:
                listener(order)
            except Exception as e:
                logger.error("Error in broker call listener: %s", e)

        if self.dispatcher and self.dispatcher.can_route_to_terminal(self.terminal_id):
            self.dispatcher.dispatch_order_intent(self.terminal_id, order)
