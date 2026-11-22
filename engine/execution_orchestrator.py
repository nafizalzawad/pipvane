"""
Pipvane v3 Execution Orchestrator (Sprint 12 & Advanced Trade Management).

Bridges the RiskEngine with the IPCDispatcher to provide:
1. Intent Translation: Formats OrderIntent specifically for target MT4 or MT5 terminals
   (symbol prefix/suffix/aliases, filling mode flags, ECN two-step compatibility,
   point vs pip slippage, and volume quantization).
2. Per-Account Serialization Queue: Dedicated asynchronous FIFO queue per terminal
   ensuring that concurrent bursts of signals (e.g. 3 signals in 1 second from a channel)
   are processed sequentially without race conditions or MT4 context busy errors.
3. Transient Rejection Retry & Exponential Backoff: Retries up to 3 times on broker
   requote (135, 138, 10004, 10020) or server/trade context busy (137, 146, 10024)
   with 500ms exponential backoff (0.5s, 1.0s, 2.0s) before logging an explicit SkipReason.
4. TP Ladders (Physical Multi-Order Dispatch): In Hedging accounts, decomposes multi-TP
   intents into distinct market orders with proportional volume splits (e.g. 50%, 30%, 20%).
5. Virtual TP Manager (MT5 Netting Accounts): In MT5 Netting accounts (where positions merge),
   tracks multiple TP milestones virtually within the Engine. Monitors live quotes and issues
   atomic partial close commands as each virtual TP level is crossed.
6. Automated Break-Even Logic: Monitors position price progression. When a Break-Even
   trigger is crossed (e.g., price reaches TP1 or configured profit pips), dispatches an
   OrderModify command moving the Stop Loss to Entry Price plus a configurable offset.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import copy
import logging
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from enum import Enum, IntEnum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, Set

from engine.ipc_protocol import (
    ActionType,
    ExecutionFeedback,
    ExecutionFlags,
    ExecutionStatus,
    OrderIntent,
    OrderSide,
    TerminalStatus,
    AccountState,
    PositionItem,
    PositionsSyncPayload,
    SymbolsSyncPayload,
    CapabilitiesPayload,
)
from engine.ipc_dispatcher import IPCDispatcher
from engine.orphan_manager import (
    OrphanManager,
    OrphanResolutionPolicy,
    OrphanAlert,
    DEFAULT_PIPVANE_MAGIC,
)
from engine.failover_manager import (
    EngineRole,
    ROLE_ACTIVE,
    ROLE_STANDBY,
    StandbyExecutionBlockedError,
)
from engine.risk_engine import (
    AccountProfile,
    FreezePolicy,
    MarketQuote,
    RiskEngine,
    SkipReason,
    SKIP_SYMBOL,
    SKIP_FIFO_HEDGE,
    SKIP_STOPS_LEVEL,
    SKIP_FREEZE_LEVEL,
    get_default_pip_size,
    quantize_lot,
)
from engine.symbol_mapper import (
    AssetClass,
    PipConverter,
    SymbolMapper,
    SymbolMappingResult,
    default_pip_converter,
)

logger = logging.getLogger("pipvane.engine.execution_orchestrator")

# Transient MT4 & MT5 Broker Return Codes Eligible for Retry
TRANSIENT_RETRYABLE_RETCODES = {
    # MT4
    135,  # ERR_PRICE_CHANGED
    136,  # ERR_OFF_QUOTES
    137,  # ERR_BROKER_BUSY
    138,  # ERR_REQUOTE
    146,  # ERR_TRADE_CONTEXT_BUSY
    # MT5
    10004,  # TRADE_RETCODE_REQUOTE
    10012,  # TRADE_RETCODE_REQUEST_TIMEOUT
    10020,  # TRADE_RETCODE_PRICE_CHANGED
    10021,  # TRADE_RETCODE_PRICE_OFF
    10024,  # TRADE_RETCODE_TOO_MANY_REQUESTS
}

TRANSIENT_KEYWORDS = (
    "requote",
    "busy",
    "off quote",
    "price change",
    "timeout",
    "trade context",
    "too many requests",
)


class OrchestratorError(Exception):
    """Base exception for ExecutionOrchestrator failures."""
    pass


class QueueFullError(OrchestratorError):
    """Raised when an account's serialization queue exceeds capacity."""
    pass


@dataclass
class BreakEvenConfig:
    """Configuration for automated Break-Even stop loss modification."""
    enabled: bool = True
    trigger_mode: str = "tp1"            # "tp1", "pips", or "price"
    trigger_pips: float = 20.0           # Minimum profit in pips to trigger BE (if trigger_mode == "pips")
    trigger_price: float = 0.0           # Specific price level triggering BE (if trigger_mode == "price")
    offset_pips: float = 2.0             # Pips added beyond entry price to cover spread/commission
    offset_price: float = 0.0            # Direct price offset added to entry


@dataclass
class VirtualTPLevel:
    """A virtual Take Profit milestone managed in the Engine for MT5 Netting accounts."""
    level_index: int                     # 0, 1, 2...
    target_price: float                  # Price at which this TP triggers
    volume_to_close: float               # Quantized lot size to partially close
    is_triggered: bool = False
    triggered_at: float = 0.0


@dataclass
class ManagedPosition:
    """Tracks a live broker position for Virtual TP and Break-Even management."""
    ticket: int                          # Broker order/position ticket
    terminal_id: str                     # Target broker terminal
    symbol: str                          # Trading pair (e.g. XAUUSD)
    side: OrderSide                      # BUY or SELL
    entry_price: float                   # Executed fill price
    original_volume: float               # Initial opened lot size
    current_volume: float                # Remaining active lot size
    current_sl: float                    # Active Stop Loss
    current_tp: float                    # Active Take Profit (or 0.0 if virtual)
    pip_size: float = 0.0001             # Pip size for symbol
    is_netting: bool = False             # MT5 Netting mode
    group_id: Optional[str] = None       # Group identifier for multi-order TP ladders
    virtual_tps: List[VirtualTPLevel] = field(default_factory=list)
    break_even_config: Optional[BreakEvenConfig] = None
    is_be_activated: bool = False
    be_activated_at: float = 0.0
    created_at: float = field(default_factory=time.time)
    closed: bool = False


@dataclass
class TerminalExecutionConfig:
    """Per-terminal execution configuration for MT4/MT5 translation."""
    terminal_id: str
    platform_type: int = 1               # 0: MT4, 1: MT5
    supports_hedging: bool = True        # True: Hedging account, False: MT5 Netting account
    is_netting: bool = False             # True: MT5 Netting account (overrides supports_hedging)
    symbol_suffix: str = ""              # e.g., ".m", "_pro"
    symbol_prefix: str = ""              # e.g., "fx."
    symbol_map: Dict[str, str] = field(default_factory=dict) # e.g. {"GOLD": "XAUUSD"}
    is_ecn: bool = False                 # MT4 ECN: requires two-step SL/TP execution
    default_filling_mode: int = int(ExecutionFlags.IOC) # MT5: IOC=1, FOK=2, RETURN=0
    min_lot: float = 0.01
    max_lot: float = 100.0
    lot_step: float = 0.01
    pip_multiplier: float = 10.0         # 5-digit broker pip multiplier
    max_retries: int = 3
    base_backoff_s: float = 0.5          # 500ms initial exponential backoff
    backoff_multiplier: float = 2.0      # Multiplier (0.5s -> 1.0s -> 2.0s)
    execution_timeout_s: float = 3.0     # Max wait for broker feedback
    quote_provider: Optional[Callable[[str], Optional[MarketQuote]]] = None
    fifo_compliant: bool = False         # Sprint 20: US NFA Rule 2-43(b) FIFO compliance
    is_fifo: bool = False                # Alias for fifo_compliant
    stops_level_pips: float = 0.0        # Broker minimum distance in pips for SL and TP
    stops_level_points: int = 0          # Broker minimum distance in points
    freeze_level_pips: float = 0.0       # Broker freeze distance in pips
    freeze_level_points: int = 0         # Broker freeze distance in points
    freeze_policy: Union[FreezePolicy, str] = FreezePolicy.ADJUST_TO_MINIMUM # Sprint 20: FreezePolicy
    stops_level_policy: str = "ADJUST"   # "ADJUST" to widen SL/TP to meet stops level, or "REJECT"
    account_profile: Optional[AccountProfile] = None

    def __post_init__(self):
        if self.is_fifo:
            self.fifo_compliant = True
        elif self.fifo_compliant:
            self.is_fifo = True

        p_val = self.freeze_policy.value if isinstance(self.freeze_policy, Enum) else str(self.freeze_policy)
        p_upper = str(p_val).strip().upper()
        s_upper = str(self.stops_level_policy).strip().upper()

        if s_upper == "REJECT" or p_upper in ("SKIP_TRADE", "SKIP", "REJECT"):
            self.freeze_policy = FreezePolicy.SKIP_TRADE
            self.stops_level_policy = "REJECT"
        else:
            self.freeze_policy = FreezePolicy.ADJUST_TO_MINIMUM
            self.stops_level_policy = "ADJUST"

        if self.is_netting:
            self.supports_hedging = False
        elif not self.supports_hedging:
            self.is_netting = True

        if self.fifo_compliant or (self.account_profile and getattr(self.account_profile, "fifo_compliant", False)):
            self.supports_hedging = False

        mult = self.pip_multiplier if self.pip_multiplier > 0 else 10.0
        if self.stops_level_pips <= 0.0 and self.stops_level_points > 0:
            self.stops_level_pips = self.stops_level_points / mult
        if self.freeze_level_pips <= 0.0 and self.freeze_level_points > 0:
            self.freeze_level_pips = self.freeze_level_points / mult


@dataclass
class ExecutionResult:
    """Comprehensive execution telemetry returned after order orchestration."""
    terminal_id: str
    signal_id: int
    success: bool
    status: ExecutionStatus
    ticket: int = 0
    executed_volume: float = 0.0
    executed_price: float = 0.0
    slippage_pips: float = 0.0
    latency_us: int = 0
    attempts: int = 1
    skip_reason: Optional[SkipReason] = None
    feedback: Optional[ExecutionFeedback] = None
    created_at: float = field(default_factory=time.time)
    completed_at: float = 0.0


@dataclass
class _QueuedOrderTask:
    """Internal task item enqueued in per-account serialization queue."""
    intent: OrderIntent
    terminal_id: str
    config: TerminalExecutionConfig
    future: asyncio.Future[ExecutionResult]
    enqueued_at: float = field(default_factory=time.time)


class SkipCode(str):
    """String subclass supporting bidirectional equality between skip code aliases."""
    def __eq__(self, other: Any) -> bool:
        s = str(self)
        o = str(other)
        if s == o:
            return True
        if s in ("SKIP_STOPS_LEVEL", "SKIP_STOPS_LEVEL_BREACH") and o in ("SKIP_STOPS_LEVEL", "SKIP_STOPS_LEVEL_BREACH"):
            return True
        if s in ("SKIP_FREEZE_LEVEL", "SKIP_FREEZE_LEVEL_BREACH") and o in ("SKIP_FREEZE_LEVEL", "SKIP_FREEZE_LEVEL_BREACH"):
            return True
        return False

    def __hash__(self) -> int:
        return hash(str(self))


class FreezeLevelGuard:
    """
    Sprint 20 Directive: Broker Freeze & Stops Level Guard.
    Prevents broker rejections caused by tight stops or entry drift.

    Tasks:
    1. Level Query: Reads SYMBOL_TRADE_STOPS_LEVEL and SYMBOL_TRADE_FREEZE_LEVEL
       from the terminal via the IPC bridge before order execution.
    2. Dynamic Adjustment: If an incoming signal requests an SL that sits inside the Stops Level,
       applies the user's FreezePolicy (either SKIP_TRADE or ADJUST_TO_MINIMUM).
       If set to adjust, widens the SL mathematically to exactly match the broker's
       minimum allowable distance.
    3. Modification Guard: Intercepts SL/TP modifications inside the Freeze Level.
    """

    def __init__(
        self,
        orchestrator: Any,
        default_policy: Union[FreezePolicy, str] = FreezePolicy.ADJUST_TO_MINIMUM,
    ) -> None:
        self.orchestrator = orchestrator
        self.default_policy = (
            default_policy
            if isinstance(default_policy, FreezePolicy)
            else (
                FreezePolicy.SKIP_TRADE
                if str(default_policy).strip().upper() in ("SKIP_TRADE", "SKIP", "REJECT")
                else FreezePolicy.ADJUST_TO_MINIMUM
            )
        )
        self._symbol_levels: Dict[Tuple[str, str], Dict[str, Any]] = {}

    def register_symbol_levels(
        self,
        terminal_id: str,
        symbol: str,
        stops_level_points: int,
        freeze_level_points: int,
    ) -> None:
        """Dynamically registers symbol-specific stops and freeze levels received via IPC."""
        clean_sym = SymbolMapper.clean_token(symbol)
        entry = {
            "stops_level_points": int(stops_level_points),
            "freeze_level_points": int(freeze_level_points),
        }
        self._symbol_levels[(terminal_id, clean_sym)] = entry
        self._symbol_levels[(terminal_id, symbol.upper())] = entry

    def resolve_policy(
        self,
        terminal_id: Optional[str] = None,
        account_profile: Optional[AccountProfile] = None,
    ) -> FreezePolicy:
        """Determines active FreezePolicy from account profile, terminal config, or default."""
        if account_profile and hasattr(account_profile, "freeze_policy") and account_profile.freeze_policy:
            p = account_profile.freeze_policy
            if isinstance(p, FreezePolicy):
                return p
            p_str = str(p).strip().upper()
            return FreezePolicy.SKIP_TRADE if p_str in ("SKIP_TRADE", "SKIP", "REJECT") else FreezePolicy.ADJUST_TO_MINIMUM

        if terminal_id:
            cfg = self.orchestrator._terminal_configs.get(terminal_id)
            if cfg:
                if cfg.account_profile and getattr(cfg.account_profile, "freeze_policy", None):
                    p = cfg.account_profile.freeze_policy
                    return p if isinstance(p, FreezePolicy) else (FreezePolicy.SKIP_TRADE if str(p).upper() in ("SKIP_TRADE", "SKIP", "REJECT") else FreezePolicy.ADJUST_TO_MINIMUM)
                if cfg.freeze_policy:
                    p = cfg.freeze_policy
                    return p if isinstance(p, FreezePolicy) else (FreezePolicy.SKIP_TRADE if str(p).upper() in ("SKIP_TRADE", "SKIP", "REJECT") else FreezePolicy.ADJUST_TO_MINIMUM)
                if getattr(cfg, "stops_level_policy", "").upper() == "REJECT":
                    return FreezePolicy.SKIP_TRADE

        default_prof = getattr(self.orchestrator, "default_account_profile", None)
        if default_prof and getattr(default_prof, "freeze_policy", None):
            p = default_prof.freeze_policy
            return p if isinstance(p, FreezePolicy) else (FreezePolicy.SKIP_TRADE if str(p).upper() in ("SKIP_TRADE", "SKIP", "REJECT") else FreezePolicy.ADJUST_TO_MINIMUM)

        return self.default_policy

    def query_levels(
        self,
        terminal_id: str,
        symbol: str,
    ) -> Dict[str, Any]:
        """
        Sprint 20 Task 1:
        Reads SYMBOL_TRADE_STOPS_LEVEL and SYMBOL_TRADE_FREEZE_LEVEL from the terminal via IPC.
        Returns point size, pip size, points, and pips values.
        """
        clean_sym = SymbolMapper.clean_token(symbol)
        cfg = self.orchestrator.get_terminal_config(terminal_id)

        # 1. Check cached symbol-specific levels
        sym_key = (terminal_id, clean_sym)
        sym_entry = self._symbol_levels.get(sym_key) or self._symbol_levels.get((terminal_id, symbol.upper()))

        if sym_entry:
            stops_points = sym_entry["stops_level_points"]
            freeze_points = sym_entry["freeze_level_points"]
        else:
            # 2. Check IPCDispatcher capabilities query
            ipc_stops = 0
            ipc_freeze = 0
            if self.orchestrator.ipc_dispatcher and hasattr(self.orchestrator.ipc_dispatcher, "get_stops_level"):
                try:
                    res_stops = self.orchestrator.ipc_dispatcher.get_stops_level(terminal_id, symbol)
                    if isinstance(res_stops, (int, float)):
                        ipc_stops = int(res_stops)
                except Exception:
                    ipc_stops = 0

            if self.orchestrator.ipc_dispatcher and hasattr(self.orchestrator.ipc_dispatcher, "get_freeze_level"):
                try:
                    res_freeze = self.orchestrator.ipc_dispatcher.get_freeze_level(terminal_id, symbol)
                    if isinstance(res_freeze, (int, float)):
                        ipc_freeze = int(res_freeze)
                except Exception:
                    ipc_freeze = 0

            if ipc_stops > 0 or ipc_freeze > 0:
                stops_points = ipc_stops
                freeze_points = ipc_freeze
            else:
                # 3. Fall back to TerminalExecutionConfig capabilities
                stops_points = getattr(cfg, "stops_level_points", 0)
                freeze_points = getattr(cfg, "freeze_level_points", 0)

        # Calculate pip size and point size
        pip_conv = getattr(self.orchestrator, "pip_converter", default_pip_converter)
        pip_size = float(pip_conv.get_pip_size(symbol))
        asset_class = pip_conv.classify_asset(symbol)

        # Digits and point size
        broker_spec = pip_conv.get_broker_spec(symbol)
        if broker_spec:
            point_size = float(broker_spec.point)
            digits = int(broker_spec.digits)
        elif asset_class == AssetClass.JPY_PAIR:
            digits = 3
            point_size = 0.001
        elif asset_class in (AssetClass.METALS,):
            digits = 2 if "XAU" in clean_sym or "GOLD" in clean_sym else 3
            point_size = 0.01 if digits == 2 else 0.001
        elif asset_class == AssetClass.INDICES:
            digits = 2
            point_size = 0.01
        else:
            digits = 5
            point_size = 0.00001

        mult = cfg.pip_multiplier if cfg.pip_multiplier > 0 else (10.0 if digits in (3, 5) else 1.0)
        stops_pips = (stops_points / mult) if stops_points > 0 else cfg.stops_level_pips
        freeze_pips = (freeze_points / mult) if freeze_points > 0 else cfg.freeze_level_pips

        return {
            "stops_level_points": stops_points,
            "freeze_level_points": freeze_points,
            "stops_level_pips": stops_pips,
            "freeze_level_pips": freeze_pips,
            "stops_level_price_offset": (stops_points * point_size) if stops_points > 0 else (stops_pips * pip_size),
            "freeze_level_price_offset": (freeze_points * point_size) if freeze_points > 0 else (freeze_pips * pip_size),
            "point_size": point_size,
            "pip_size": pip_size,
            "digits": digits,
        }

    def evaluate(
        self,
        terminal_id: str,
        intent: OrderIntent,
        account_profile: Optional[AccountProfile] = None,
        quote: Optional[MarketQuote] = None,
    ) -> Union[OrderIntent, SkipReason]:
        """
        Sprint 20 Task 2:
        Evaluates an OrderIntent against broker Stops Level and Freeze Level.
        If an SL sits inside Stops Level:
          - Under SKIP_TRADE: returns SkipReason(code=SkipCode("SKIP_STOPS_LEVEL")).
          - Under ADJUST_TO_MINIMUM: widens SL mathematically to exactly match the broker minimum.
        """
        levels = self.query_levels(terminal_id, intent.symbol)
        stops_points = levels["stops_level_points"]
        stops_pips = levels["stops_level_pips"]
        min_dist = levels["stops_level_price_offset"]
        pip_size = levels["pip_size"]
        digits = levels["digits"]

        policy = self.resolve_policy(terminal_id, account_profile)

        # Reference market / entry price
        ref_price = float(intent.price)
        cfg = self.orchestrator.get_terminal_config(terminal_id)
        if ref_price <= 0.0:
            if quote and quote.symbol.upper() == intent.symbol.upper():
                ref_price = quote.ask if intent.side == OrderSide.BUY else quote.bid
            elif cfg.quote_provider:
                q = cfg.quote_provider(intent.symbol)
                if q:
                    ref_price = q.ask if intent.side == OrderSide.BUY else q.bid
            if ref_price <= 0.0 and intent.symbol in self.orchestrator._latest_quotes:
                bid, ask = self.orchestrator._latest_quotes[intent.symbol]
                ref_price = ask if intent.side == OrderSide.BUY else bid

        # 1. Stop Loss Evaluation (for entry orders and modifications)
        if (stops_points > 0 or stops_pips > 0) and intent.stop_loss > 0.0 and ref_price > 0.0:
            sl_dist = abs(ref_price - intent.stop_loss)
            if sl_dist < (min_dist - 1e-7):
                sl_dist_pips = sl_dist / pip_size if pip_size > 0 else (sl_dist * 10000.0)
                if policy == FreezePolicy.SKIP_TRADE:
                    code_val = SkipCode("SKIP_STOPS_LEVEL_BREACH" if getattr(cfg, "stops_level_policy", "").upper() == "REJECT" else "SKIP_STOPS_LEVEL")
                    reason_msg = (
                        f"Stop Loss distance ({sl_dist_pips:.1f} pips) is less than broker stops level "
                        f"({stops_pips:.1f} pips)."
                    )
                    logger.warning("%s: %s", code_val, reason_msg)
                    return SkipReason(
                        code=code_val,
                        reason=reason_msg,
                        evaluated_at=time.time(),
                        details={
                            "stops_level_pips": stops_pips,
                            "stops_level_points": stops_points,
                            "sl_dist_pips": sl_dist_pips,
                            "terminal_id": terminal_id,
                            "policy": policy.value,
                        },
                    )
                else:
                    # ADJUST_TO_MINIMUM: Widen SL mathematically to exactly match the broker's minimum allowable distance
                    if intent.side == OrderSide.BUY:
                        widened_sl = round(ref_price - min_dist, digits)
                    else:
                        widened_sl = round(ref_price + min_dist, digits)

                    logger.info(
                        "FREEZE LEVEL GUARD: Widened SL on %s from %.5f to %.5f (%.1f pips -> %.1f pips) "
                        "to satisfy SYMBOL_TRADE_STOPS_LEVEL on %s",
                        intent.symbol,
                        intent.stop_loss,
                        widened_sl,
                        sl_dist_pips,
                        stops_pips,
                        terminal_id,
                    )
                    intent.stop_loss = widened_sl

        # 2. Take Profit Evaluation
        if (stops_points > 0 or stops_pips > 0) and intent.take_profit > 0.0 and ref_price > 0.0:
            tp_dist = abs(ref_price - intent.take_profit)
            if tp_dist < (min_dist - 1e-7):
                tp_dist_pips = tp_dist / pip_size if pip_size > 0 else (tp_dist * 10000.0)
                if policy == FreezePolicy.SKIP_TRADE:
                    code_val = SkipCode("SKIP_STOPS_LEVEL_BREACH" if getattr(cfg, "stops_level_policy", "").upper() == "REJECT" else "SKIP_STOPS_LEVEL")
                    reason_msg = (
                        f"Take Profit distance ({tp_dist_pips:.1f} pips) is less than broker stops level "
                        f"({stops_pips:.1f} pips)."
                    )
                    logger.warning("%s: %s", code_val, reason_msg)
                    return SkipReason(
                        code=code_val,
                        reason=reason_msg,
                        evaluated_at=time.time(),
                        details={
                            "stops_level_pips": stops_pips,
                            "stops_level_points": stops_points,
                            "tp_dist_pips": tp_dist_pips,
                            "terminal_id": terminal_id,
                            "policy": policy.value,
                        },
                    )
                else:
                    if intent.side == OrderSide.BUY:
                        widened_tp = round(ref_price + min_dist, digits)
                    else:
                        widened_tp = round(ref_price - min_dist, digits)

                    logger.info(
                        "FREEZE LEVEL GUARD: Widened TP on %s from %.5f to %.5f to satisfy SYMBOL_TRADE_STOPS_LEVEL on %s",
                        intent.symbol, intent.take_profit, widened_tp, terminal_id,
                    )
                    intent.take_profit = widened_tp

        # 3. Freeze Level Evaluation (ActionType.MODIFY_SL_TP or CANCEL_PENDING)
        freeze_points = levels["freeze_level_points"]
        freeze_pips = levels["freeze_level_pips"]
        freeze_min_dist = levels["freeze_level_price_offset"]
        if (freeze_points > 0 or freeze_pips > 0) and intent.action_type in (ActionType.MODIFY_SL_TP, ActionType.CANCEL_PENDING) and ref_price > 0.0:
            target_ticket = intent.order_ticket
            pos = self.orchestrator._managed_positions.get(target_ticket)
            order_rec = self.orchestrator.get_order_by_ticket(target_ticket) if not pos else None

            # Check distance to active SL/TP
            target_sl = pos.current_sl if pos else (float(order_rec["stop_loss"]) if order_rec and "stop_loss" in order_rec else 0.0)
            target_tp = pos.current_tp if pos else (float(order_rec["take_profit"]) if order_rec and "take_profit" in order_rec else 0.0)

            is_frozen = False
            frozen_dist = 0.0
            if target_sl > 0.0 and abs(ref_price - target_sl) < (freeze_min_dist - 1e-7):
                is_frozen = True
                frozen_dist = abs(ref_price - target_sl)
            elif target_tp > 0.0 and abs(ref_price - target_tp) < (freeze_min_dist - 1e-7):
                is_frozen = True
                frozen_dist = abs(ref_price - target_tp)

            if is_frozen:
                dist_pips = frozen_dist / pip_size if pip_size > 0 else (frozen_dist * 10000.0)
                code_val = SkipCode("SKIP_FREEZE_LEVEL_BREACH")
                reason_msg = (
                    f"Current market price ({ref_price:.5f}) is within freeze level "
                    f"({freeze_pips:.1f} pips) of active order stops. Modification prohibited by broker."
                )
                logger.warning("%s: %s", code_val, reason_msg)
                return SkipReason(
                    code=code_val,
                    reason=reason_msg,
                    evaluated_at=time.time(),
                    details={
                        "code": code_val,
                        "ticket": target_ticket,
                        "terminal_id": terminal_id,
                        "freeze_level_pips": freeze_pips,
                        "freeze_level_points": freeze_points,
                        "dist_pips": dist_pips,
                    },
                )

        return intent


class ExecutionOrchestrator:
    """
    Central Orchestrator bridging RiskEngine with IPCDispatcher.

    Guarantees:
    - MT4 vs MT5 platform-specific formatting and ECN two-step handling.
    - Strictly serialized sequential order submission per account queue.
    - Transient rejection handling with 3-retry 500ms exponential backoff.
    - Multi-order TP ladder execution for hedging accounts.
    - Virtual TP management for MT5 Netting accounts with live partial closes.
    - Automated Break-Even SL modification to entry price plus offset.
    - Zero-allocation high-speed dispatch (<50 µs) to Shared Memory.
    """

    def __init__(
        self,
        ipc_dispatcher: IPCDispatcher,
        risk_engine: Optional[RiskEngine] = None,
        default_max_retries: int = 3,
        default_base_backoff_s: float = 0.5,
        default_execution_timeout_s: float = 3.0,
        max_queue_depth: int = 100,
        db_path: Optional[str] = None,
        reconciliation_interval_s: float = 30.0,
        orphan_manager: Optional[OrphanManager] = None,
        orphan_policy: OrphanResolutionPolicy = OrphanResolutionPolicy.CLOSE_IMMEDIATELY,
        default_magic_number: int = DEFAULT_PIPVANE_MAGIC,
        failover_manager: Optional[Any] = None,
        initial_role: EngineRole = EngineRole.ROLE_ACTIVE,
        reconciliation_engine: Optional[Any] = None,
        notification_engine: Optional[Any] = None,
        symbol_mapper: Optional[SymbolMapper] = None,
        account_profile: Optional[AccountProfile] = None,
    ) -> None:
        self.ipc_dispatcher = ipc_dispatcher
        self.risk_engine = risk_engine
        self.reconciliation_engine = reconciliation_engine
        self.notification_engine = notification_engine
        self.symbol_mapper = symbol_mapper or SymbolMapper()
        self.pip_converter = PipConverter()
        self.default_account_profile = account_profile
        self._account_profiles: Dict[str, AccountProfile] = {}
        self.freeze_level_guard = FreezeLevelGuard(orchestrator=self)
        self._trade_callbacks: List[Callable[[ExecutionResult], Any]] = []
        self.default_max_retries = default_max_retries
        self.default_base_backoff_s = default_base_backoff_s
        self.default_execution_timeout_s = default_execution_timeout_s
        self.max_queue_depth = max_queue_depth
        self.db_path = db_path or ":memory:"
        self.reconciliation_interval_s = float(reconciliation_interval_s)
        self.default_magic_number = default_magic_number

        # Sprint 15 Failover & Role State
        self.role: EngineRole = initial_role
        self.failover_manager = failover_manager
        if self.failover_manager:
            self.failover_manager.orchestrator = self
            self.role = self.failover_manager.role

        self._terminal_configs: Dict[str, TerminalExecutionConfig] = {}
        self._account_queues: Dict[str, asyncio.Queue[_QueuedOrderTask]] = {}
        self._worker_tasks: Dict[str, asyncio.Task] = {}
        self._pending_feedback: Dict[Tuple[str, int], asyncio.Future[ExecutionFeedback]] = {}
        self._retry_logs: List[Dict[str, Any]] = []

        # Trade Management State
        self._managed_positions: Dict[int, ManagedPosition] = {}
        self._position_groups: Dict[str, List[int]] = {}  # group_id -> list of tickets
        self._latest_quotes: Dict[str, Tuple[float, float]] = {} # symbol -> (bid, ask)

        # Sprint 14 Reconciliation State & Orphan Manager
        self.orphan_manager = orphan_manager or OrphanManager(
            default_policy=orphan_policy,
            pipvane_magic_number=default_magic_number,
            orchestrator=self,
            ipc_dispatcher=self.ipc_dispatcher,
        )
        self._reconciliation_task: Optional[asyncio.Task] = None
        self._discovered_open_signals: Dict[int, int] = {}  # signal_id -> ticket (Sprint 16 SKIP_DUP)
        self._reconciliation_stats: Dict[str, Any] = {
            "total_cycles": 0,
            "last_run_timestamp": 0.0,
            "externally_closed_count": 0,
            "orphans_detected_count": 0,
            "last_reconciliation_summary": {},
        }
        self._terminal_account_states: Dict[str, AccountState] = {}

        # Sprint 21: Dedicated Asynchronous Database Writer ThreadPool
        self._db_lock = threading.RLock()
        self._db_executor: Optional[concurrent.futures.ThreadPoolExecutor] = None
        self._pending_storage_tasks: Set[asyncio.Task] = set()

        # Initialize SQLite Internal Engine Store
        self._init_sqlite_db()

        # Sprint 17 Mobile Override & Remote Command State
        self._channel_name_to_id: Dict[str, int] = {}
        self._channel_id_to_name: Dict[int, str] = {}
        self._is_paused: bool = False

        # Sprint 18 Event-Driven Notification Listeners
        self._event_listeners: List[Callable[[Dict[str, Any]], Any]] = []

        self._is_running = False

        try:
            _ = asyncio.get_running_loop()
            self._lock = asyncio.Lock()
        except RuntimeError:
            self._lock = None

        # Wire IPCDispatcher execution feedback, account state, and symbol sync callbacks
        self.ipc_dispatcher.on_execution_feedback(self._handle_execution_feedback)
        self.ipc_dispatcher.on_account_state(self._handle_account_state_telemetry)
        if hasattr(self.ipc_dispatcher, "on_symbols_sync"):
            self.ipc_dispatcher.on_symbols_sync(self._handle_symbols_sync)
        if hasattr(self.ipc_dispatcher, "on_capabilities"):
            self.ipc_dispatcher.on_capabilities(self._handle_capabilities_telemetry)

    def _handle_capabilities_telemetry(self, terminal_id: str, caps: CapabilitiesPayload) -> None:
        """Configures TerminalExecutionConfig based on broker capabilities negotiation."""
        cfg = self.get_terminal_config(terminal_id)
        cfg.supports_hedging = caps.supports_hedging
        cfg.is_netting = not caps.supports_hedging
        cfg.is_fifo = caps.is_fifo_enforced
        cfg.fifo_compliant = caps.is_fifo_enforced
        cfg.min_lot = caps.min_lot
        cfg.max_lot = caps.max_lot
        cfg.lot_step = caps.lot_step
        cfg.stops_level_points = caps.stop_level_points
        cfg.freeze_level_points = caps.freeze_level_points
        mult = cfg.pip_multiplier if cfg.pip_multiplier > 0 else 10.0
        cfg.stops_level_pips = caps.stop_level_points / mult
        cfg.freeze_level_pips = caps.freeze_level_points / mult
        logger.info(
            "Configured terminal %s from capabilities: Hedging=%s, FIFO=%s, StopsLevel=%.1f pips, FreezeLevel=%.1f pips",
            terminal_id, cfg.supports_hedging, cfg.is_fifo, cfg.stops_level_pips, cfg.freeze_level_pips,
        )

    def _handle_symbols_sync(self, terminal_id: str, payload: SymbolsSyncPayload) -> None:
        """Populates SymbolMapper catalog when symbols dump arrives from broker terminal."""
        if self.symbol_mapper and payload and payload.symbols:
            self.symbol_mapper.set_broker_symbols(payload.symbols, terminal_id=terminal_id)

    # -------------------------------------------------------------------------
    # Internal SQLite Storage & Journey Log (Sprint 14 & 21 Async Storage)
    # -------------------------------------------------------------------------

    @property
    def db_executor(self) -> concurrent.futures.ThreadPoolExecutor:
        """
        Sprint 21 Directive: Dedicated background worker thread pool for SQLite writes.
        Prevents synchronous disk I/O from stalling the main asyncio event loop.
        """
        if self._db_executor is None:
            self._db_executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=1,
                thread_name_prefix="PipvaneDBWriter",
            )
        return self._db_executor

    async def _run_db_write_async(self, fn: Callable[..., Any], *args: Any) -> Any:
        """
        Sprint 21: Runs a database mutation function off the event loop thread
        in the serialized SQLite worker thread.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.db_executor, fn, *args)

    def _schedule_storage_task(self, coro: Any) -> asyncio.Task:
        """
        Sprint 21 Directive: Schedules a non-blocking background task for SQLite writes.
        Maintains an internal reference set to prevent premature garbage collection.
        """
        task = asyncio.create_task(coro)
        self._pending_storage_tasks.add(task)
        task.add_done_callback(self._pending_storage_tasks.discard)
        return task

    async def flush_storage(self) -> None:
        """
        Sprint 21: Flushes all pending asynchronous storage writes to SQLite.
        Awaits completion of all background disk write tasks and worker thread.
        """
        if self._pending_storage_tasks:
            await asyncio.gather(*list(self._pending_storage_tasks), return_exceptions=True)
        await self._run_db_write_async(lambda: None)

    def prune_closed_positions(self, max_age_s: float = 0.0) -> int:
        """
        Sprint 21 Directive: Prunes closed ManagedPosition instances from memory
        to ensure memory profile stays perfectly flat during burst spikes.
        """
        now = time.time()
        to_prune = []
        for ticket, pos in list(self._managed_positions.items()):
            if pos.closed:
                if max_age_s <= 0.0 or (now - pos.created_at) >= max_age_s:
                    to_prune.append(ticket)
        for ticket in to_prune:
            self._managed_positions.pop(ticket, None)
        return len(to_prune)

    def _init_sqlite_db(self) -> None:
        """Initializes internal SQLite storage for active OrderIntents and Journey log."""
        self._db_conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._db_conn.row_factory = sqlite3.Row
        with self._db_lock:
            cur = self._db_conn.cursor()
            cur.execute("PRAGMA journal_mode = WAL;")
            cur.execute("PRAGMA synchronous = NORMAL;")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS order_intents (
                    signal_id INTEGER PRIMARY KEY,
                    ticket INTEGER NOT NULL,
                    magic_number INTEGER NOT NULL,
                    terminal_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side INTEGER NOT NULL,
                    volume REAL NOT NULL,
                    open_price REAL NOT NULL,
                    stop_loss REAL NOT NULL,
                    take_profit REAL NOT NULL,
                    status TEXT NOT NULL DEFAULT 'OPEN',
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    closed_at REAL
                );
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_order_intents_ticket_magic
                ON order_intents (ticket, magic_number);
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_order_intents_terminal_status
                ON order_intents (terminal_id, status);
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS journey_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp REAL NOT NULL,
                    signal_id INTEGER,
                    ticket INTEGER,
                    magic_number INTEGER,
                    terminal_id TEXT,
                    event_type TEXT NOT NULL,
                    status TEXT,
                    details TEXT,
                    source TEXT DEFAULT 'engine'
                );
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_journey_log_ticket
                ON journey_log (ticket);
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_journey_log_signal
                ON journey_log (signal_id);
            """)
            self._db_conn.commit()

    def record_order_intent(
        self,
        signal_id: int,
        ticket: int,
        magic_number: int,
        terminal_id: str,
        symbol: str,
        side: int,
        volume: float,
        open_price: float,
        stop_loss: float = 0.0,
        take_profit: float = 0.0,
        status: str = "OPEN",
    ) -> None:
        """Persists or updates an OrderIntent record in SQLite internal state."""
        now = time.time()
        with self._db_lock:
            cur = self._db_conn.cursor()
            cur.execute(
                """
                INSERT INTO order_intents (
                    signal_id, ticket, magic_number, terminal_id, symbol, side,
                    volume, open_price, stop_loss, take_profit, status,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(signal_id) DO UPDATE SET
                    ticket = excluded.ticket,
                    magic_number = excluded.magic_number,
                    terminal_id = excluded.terminal_id,
                    symbol = excluded.symbol,
                    side = excluded.side,
                    volume = excluded.volume,
                    open_price = excluded.open_price,
                    stop_loss = excluded.stop_loss,
                    take_profit = excluded.take_profit,
                    status = excluded.status,
                    updated_at = excluded.updated_at;
                """,
                (
                    signal_id, ticket, magic_number, terminal_id, symbol, side,
                    volume, open_price, stop_loss, take_profit, status,
                    now, now,
                ),
            )
            self._db_conn.commit()

    async def record_order_intent_async(
        self,
        signal_id: int,
        ticket: int,
        magic_number: int,
        terminal_id: str,
        symbol: str,
        side: int,
        volume: float,
        open_price: float,
        stop_loss: float = 0.0,
        take_profit: float = 0.0,
        status: str = "OPEN",
    ) -> None:
        """
        Sprint 21: Asynchronously persists or updates an OrderIntent record in SQLite internal state.
        Executes via the dedicated database worker thread pool.
        """
        await self._run_db_write_async(
            self.record_order_intent,
            signal_id,
            ticket,
            magic_number,
            terminal_id,
            symbol,
            side,
            volume,
            open_price,
            stop_loss,
            take_profit,
            status,
        )

    def record_journey_event(
        self,
        signal_id: int,
        ticket: int,
        magic_number: int,
        terminal_id: str,
        event_type: str,
        status: Optional[str] = None,
        details: str = "",
        source: str = "engine",
    ) -> int:
        """Appends an event to the internal Journey audit log."""
        now = time.time()
        with self._db_lock:
            cur = self._db_conn.cursor()
            cur.execute(
                """
                INSERT INTO journey_log (
                    timestamp, signal_id, ticket, magic_number, terminal_id,
                    event_type, status, details, source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    now, signal_id, ticket, magic_number, terminal_id,
                    event_type, status or "", details, source,
                ),
            )
            row_id = cur.lastrowid
            self._db_conn.commit()

        # Notify event listeners (e.g. NotificationEngine)
        if self._event_listeners:
            evt_dict = {
                "event_type": event_type,
                "signal_id": signal_id,
                "ticket": ticket,
                "magic_number": magic_number,
                "terminal_id": terminal_id,
                "status": status,
                "details": details,
                "source": source,
                "timestamp": now,
            }
            pos = self._managed_positions.get(ticket)
            if pos:
                evt_dict["symbol"] = pos.symbol
                evt_dict["volume"] = pos.original_volume
                evt_dict["price"] = pos.entry_price
                evt_dict["side"] = pos.side.value if hasattr(pos.side, "value") else pos.side
                evt_dict["stop_loss"] = pos.current_sl
                evt_dict["take_profit"] = pos.current_tp
            for cb in self._event_listeners:
                try:
                    res = cb(evt_dict)
                    if asyncio.iscoroutine(res):
                        asyncio.create_task(res)
                except Exception as e:
                    logger.debug("Error in event listener: %s", e)

        return row_id

    async def record_journey_event_async(
        self,
        signal_id: int,
        ticket: int,
        magic_number: int,
        terminal_id: str,
        event_type: str,
        status: Optional[str] = None,
        details: str = "",
        source: str = "engine",
    ) -> int:
        """
        Sprint 21: Asynchronously appends an event to the internal Journey audit log
        without blocking the event loop thread during high-frequency execution bursts.
        """
        return await self._run_db_write_async(
            self.record_journey_event,
            signal_id,
            ticket,
            magic_number,
            terminal_id,
            event_type,
            status,
            details,
            source,
        )

    def add_event_listener(self, cb: Callable[[Dict[str, Any]], Any]) -> None:
        """Registers a listener for trade and lifecycle events."""
        self._event_listeners.append(cb)

    def get_active_orders(self, terminal_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Queries SQLite for all currently OPEN OrderIntents."""
        with self._db_conn:
            if terminal_id:
                cur = self._db_conn.execute(
                    "SELECT * FROM order_intents WHERE terminal_id = ? AND status = 'OPEN' ORDER BY created_at ASC",
                    (terminal_id,),
                )
            else:
                cur = self._db_conn.execute(
                    "SELECT * FROM order_intents WHERE status = 'OPEN' ORDER BY created_at ASC"
                )
            return [dict(row) for row in cur.fetchall()]

    def get_order_by_ticket(self, ticket: int) -> Optional[Dict[str, Any]]:
        """Retrieves an order record by broker ticket number."""
        with self._db_conn:
            cur = self._db_conn.execute(
                "SELECT * FROM order_intents WHERE ticket = ?",
                (ticket,),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def get_journey_logs(
        self,
        ticket: Optional[int] = None,
        signal_id: Optional[int] = None,
        event_type: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Queries the Journey log with optional filtering."""
        query = "SELECT * FROM journey_log WHERE 1=1"
        params: List[Any] = []
        if ticket is not None:
            query += " AND ticket = ?"
            params.append(ticket)
        if signal_id is not None:
            query += " AND signal_id = ?"
            params.append(signal_id)
        if event_type is not None:
            query += " AND event_type = ?"
            params.append(event_type)
        query += " ORDER BY id DESC LIMIT ?"
        params.append(limit)

        with self._db_conn:
            cur = self._db_conn.execute(query, params)
            return [dict(row) for row in cur.fetchall()]

    # -------------------------------------------------------------------------
    # Configuration & Terminal Registration
    # -------------------------------------------------------------------------

    def register_terminal_config(self, config: TerminalExecutionConfig) -> None:
        """Register or update execution configuration for a terminal."""
        self._terminal_configs[config.terminal_id] = config
        if self.symbol_mapper and config.symbol_map:
            for k, v in config.symbol_map.items():
                self.symbol_mapper.config.add_override(k, v, terminal_id=config.terminal_id)
        logger.info(
            "Registered terminal execution config for %s (platform=%s, netting=%s, ecn=%s, max_retries=%d)",
            config.terminal_id,
            "MT4" if config.platform_type == 0 else "MT5",
            config.is_netting,
            config.is_ecn,
            config.max_retries,
        )

    def get_terminal_config(self, terminal_id: str) -> TerminalExecutionConfig:
        """Retrieve existing config or generate default based on terminal state."""
        if terminal_id in self._terminal_configs:
            return self._terminal_configs[terminal_id]

        platform = 1  # Default to MT5
        ctx = getattr(self.ipc_dispatcher, "_terminals", {}).get(terminal_id)
        if ctx and ctx.hello_info:
            platform = ctx.hello_info.platform_type

        cfg = TerminalExecutionConfig(
            terminal_id=terminal_id,
            platform_type=platform,
            max_retries=self.default_max_retries,
            base_backoff_s=self.default_base_backoff_s,
            execution_timeout_s=self.default_execution_timeout_s,
        )
        self._terminal_configs[terminal_id] = cfg
        return cfg

    # -------------------------------------------------------------------------
    # Sprint 20: FIFO Compliance & Position Management (US NFA Rule 2-43(b))
    # -------------------------------------------------------------------------

    def set_account_profile(
        self,
        account_profile: AccountProfile,
        terminal_id: Optional[str] = None,
    ) -> None:
        """Registers an AccountProfile globally or specifically for a terminal."""
        if terminal_id:
            self._account_profiles[terminal_id] = account_profile
            if terminal_id in self._terminal_configs:
                self._terminal_configs[terminal_id].account_profile = account_profile
                if getattr(account_profile, "fifo_compliant", False):
                    self._terminal_configs[terminal_id].fifo_compliant = True
                    self._terminal_configs[terminal_id].supports_hedging = False
        else:
            self.default_account_profile = account_profile

    def get_account_profile(self, terminal_id: Optional[str] = None) -> Optional[AccountProfile]:
        """Retrieves AccountProfile registered for terminal, falling back to default."""
        if terminal_id and terminal_id in self._account_profiles:
            return self._account_profiles[terminal_id]
        if terminal_id and terminal_id in self._terminal_configs:
            cfg = self._terminal_configs[terminal_id]
            if cfg.account_profile:
                return cfg.account_profile
        return self.default_account_profile

    def is_fifo_compliant(
        self,
        terminal_id: Optional[str] = None,
        account_profile: Optional[AccountProfile] = None,
    ) -> bool:
        """
        Sprint 20: Evaluates whether FIFO compliance (NFA Rule 2-43(b)) is active
        for the given terminal or execution context.
        """
        # 1. Directly supplied account profile
        if account_profile is not None and getattr(account_profile, "fifo_compliant", False):
            return True

        # 2. Terminal-specific registered AccountProfile
        if terminal_id and terminal_id in self._account_profiles:
            if getattr(self._account_profiles[terminal_id], "fifo_compliant", False):
                return True

        # 3. Terminal-specific TerminalExecutionConfig
        if terminal_id and terminal_id in self._terminal_configs:
            cfg = self._terminal_configs[terminal_id]
            if cfg.fifo_compliant or getattr(cfg, "is_fifo", False):
                return True
            if cfg.account_profile and getattr(cfg.account_profile, "fifo_compliant", False):
                return True

        # 4. Global default AccountProfile
        if self.default_account_profile and getattr(self.default_account_profile, "fifo_compliant", False):
            return True

        return False

    def get_open_positions_for_symbol(
        self,
        symbol: str,
        terminal_id: Optional[str] = None,
        side: Optional[Union[OrderSide, int]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Sprint 20: Queries local database for active/open positions for a given symbol,
        sorted strictly chronologically by creation timestamp (created_at ASC, ticket ASC).
        Normalizes symbol comparison (handling prefixes, suffixes, and aliases).
        """
        target_clean = SymbolMapper.clean_token(symbol)
        target_roots = set(SymbolMapper._extract_possible_roots(symbol)) if hasattr(SymbolMapper, "_extract_possible_roots") else set()
        target_roots.add(target_clean)
        target_roots.add(symbol.upper())
        side_val = int(side) if side is not None else None

        results: List[Dict[str, Any]] = []
        try:
            with self._db_conn:
                if terminal_id:
                    cursor = self._db_conn.execute(
                        """
                        SELECT * FROM order_intents
                        WHERE terminal_id = ? AND status IN ('OPEN', 'PARTIAL')
                        ORDER BY created_at ASC, ticket ASC
                        """,
                        (terminal_id,),
                    )
                else:
                    cursor = self._db_conn.execute(
                        """
                        SELECT * FROM order_intents
                        WHERE status IN ('OPEN', 'PARTIAL')
                        ORDER BY created_at ASC, ticket ASC
                        """
                    )
                rows = [dict(r) for r in cursor.fetchall()]

            for r in rows:
                r_sym = str(r.get("symbol") or "")
                r_clean = SymbolMapper.clean_token(r_sym)
                r_roots = set(SymbolMapper._extract_possible_roots(r_sym)) if hasattr(SymbolMapper, "_extract_possible_roots") else set()
                r_roots.add(r_clean)
                r_roots.add(r_sym.upper())

                # Match symbols
                matched = False
                if r_sym.upper() == symbol.upper() or r_clean == target_clean:
                    matched = True
                elif bool(target_roots & r_roots):
                    matched = True
                elif self.symbol_mapper:
                    res1 = self.symbol_mapper.resolve(r_sym, terminal_id=terminal_id)
                    res2 = self.symbol_mapper.resolve(symbol, terminal_id=terminal_id)
                    if res1.success and res2.success and res1.mapped_symbol == res2.mapped_symbol:
                        matched = True

                if not matched:
                    continue

                if side_val is not None and int(r.get("side", 0)) != side_val:
                    continue

                results.append(r)

            # Also inspect _managed_positions to ensure complete state coverage
            existing_tickets = {int(r["ticket"]) for r in results if "ticket" in r}
            for pos in self._managed_positions.values():
                if pos.ticket in existing_tickets or pos.closed or pos.current_volume <= 0.0:
                    continue
                if terminal_id and pos.terminal_id != terminal_id:
                    continue
                pos_clean = SymbolMapper.clean_token(pos.symbol)
                matched = False
                if pos.symbol.upper() == symbol.upper() or pos_clean == target_clean:
                    matched = True
                elif self.symbol_mapper:
                    res1 = self.symbol_mapper.resolve(pos.symbol, terminal_id=terminal_id)
                    res2 = self.symbol_mapper.resolve(symbol, terminal_id=terminal_id)
                    if res1.success and res2.success and res1.mapped_symbol == res2.mapped_symbol:
                        matched = True
                if matched:
                    if side_val is None or int(pos.side) == side_val:
                        results.append({
                            "ticket": pos.ticket,
                            "signal_id": getattr(pos, "signal_id", 0),
                            "magic_number": getattr(pos, "magic_number", self.default_magic_number),
                            "terminal_id": pos.terminal_id,
                            "symbol": pos.symbol,
                            "side": int(pos.side),
                            "volume": pos.current_volume,
                            "open_price": pos.entry_price,
                            "stop_loss": pos.current_sl,
                            "take_profit": pos.current_tp,
                            "status": "OPEN",
                            "created_at": pos.created_at,
                        })

            results.sort(key=lambda r: (float(r.get("created_at") or 0.0), int(r.get("ticket") or 0)))
        except Exception as e:
            logger.error("Error querying open positions for symbol %s: %s", symbol, e)

        return results

    def get_oldest_open_position(
        self,
        symbol: str,
        terminal_id: Optional[str] = None,
        side: Optional[Union[OrderSide, int]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Sprint 20: Returns the single oldest open position for symbol, or None."""
        open_pos = self.get_open_positions_for_symbol(symbol=symbol, terminal_id=terminal_id, side=side)
        return open_pos[0] if open_pos else None

    async def execute_fifo_closure(
        self,
        terminal_id: str,
        symbol: str,
        volume: float,
        side: Optional[Union[OrderSide, int]] = None,
        account_profile: Optional[AccountProfile] = None,
    ) -> List[ExecutionResult]:
        """
        Sprint 20: Executes the closure of a specific portion (or full liquidation)
        of an accumulated position under FIFO rules. Queries local SQLite DB, sorts
        positions by timestamp, and executes closes strictly sequentially against the
        oldest active ticket IDs.
        """
        remaining_vol = round(float(volume), 5)
        results: List[ExecutionResult] = []

        while remaining_vol > 0.00001:
            open_positions = self.get_open_positions_for_symbol(
                symbol=symbol,
                terminal_id=terminal_id,
                side=side,
            )
            if not open_positions:
                logger.info(
                    "execute_fifo_closure: No more open positions for %s on terminal %s (remaining_unclosed=%.2f)",
                    symbol,
                    terminal_id,
                    remaining_vol,
                )
                break

            # Oldest position
            oldest = open_positions[0]
            ticket = int(oldest["ticket"])
            ticket_vol = float(oldest["volume"])
            pos_side = int(oldest["side"])
            magic = int(oldest.get("magic_number") or self.default_magic_number)

            close_vol = min(remaining_vol, ticket_vol)
            is_full = close_vol >= ticket_vol - 0.00001
            action = ActionType.FULL_CLOSE if is_full else ActionType.PARTIAL_CLOSE
            close_side = OrderSide.SELL if pos_side == int(OrderSide.BUY) else OrderSide.BUY

            close_intent = OrderIntent(
                symbol=str(oldest.get("symbol") or symbol),
                side=close_side,
                action_type=action,
                volume=round(close_vol, 5),
                magic_number=magic,
                order_ticket=ticket,
                flags=int(ExecutionFlags.IOC),
                signal_id=int(time.time_ns() & 0x7FFFFFFFFFFFFFFF),
                timestamp_ns=time.time_ns(),
            )

            res = await self.submit_order(terminal_id, close_intent, account_profile=account_profile)
            results.append(res)

            if not res.success:
                logger.warning(
                    "execute_fifo_closure: Failed to close ticket #%d (vol=%.2f): %s",
                    ticket,
                    close_vol,
                    res.skip_reason or res.status,
                )
                break

            remaining_vol = max(0.0, round(remaining_vol - close_vol, 5))

        return results

    # -------------------------------------------------------------------------
    # Task 1: Intent Translation (MT4 vs MT5)
    # -------------------------------------------------------------------------

    def translate_intent(
        self,
        intent: OrderIntent,
        terminal_id_or_config: Union[str, TerminalExecutionConfig],
    ) -> OrderIntent:
        """
        Translates and formats an OrderIntent for the exact target terminal:
        1. Applies broker symbol prefix, suffix, and custom symbol mappings.
        2. Adjusts execution flags (MT5 filling modes IOC/FOK vs MT4 standard).
        3. Quantizes lot volume to broker lot_step, min_lot, and max_lot.
        4. Adjusts slippage points vs pips.
        5. Handles MT4 ECN two-step SL/TP masking if required.
        """
        config = (
            terminal_id_or_config
            if isinstance(terminal_id_or_config, TerminalExecutionConfig)
            else self.get_terminal_config(terminal_id_or_config)
        )

        # 1. Symbol Translation
        sym = intent.symbol.strip()
        if self.symbol_mapper:
            map_res = self.symbol_mapper.resolve(sym, terminal_id=config.terminal_id)
            if map_res.success and map_res.mapped_symbol:
                sym = map_res.mapped_symbol
            elif sym.upper() in config.symbol_map:
                sym = config.symbol_map[sym.upper()]
            else:
                sym = sym.upper()
        elif sym.upper() in config.symbol_map:
            sym = config.symbol_map[sym.upper()]
        else:
            sym = sym.upper()

        if config.symbol_prefix and not sym.startswith(config.symbol_prefix):
            sym = f"{config.symbol_prefix}{sym}"
        if config.symbol_suffix and not sym.endswith(config.symbol_suffix):
            sym = f"{sym}{config.symbol_suffix}"
        # Struct limitation: 31 characters max (ASCII null-terminated)
        formatted_symbol = sym[:31]

        # 2. Execution Flags Translation
        flags = int(intent.flags)
        if config.platform_type == 1:
            # MT5: Ensure filling mode flag is assigned if not already set
            if not (flags & (ExecutionFlags.IOC | ExecutionFlags.FOK)):
                flags |= (config.default_filling_mode & (ExecutionFlags.IOC | ExecutionFlags.FOK))
        else:
            # MT4: MT4 does not support IOC/FOK flags (these are MT5 CTrade flags). Mask them out.
            flags &= ~(int(ExecutionFlags.IOC) | int(ExecutionFlags.FOK))

        # 3. Volume Quantization
        quantized_volume = quantize_lot(
            lot=float(intent.volume),
            min_lot=config.min_lot,
            max_lot=config.max_lot,
            lot_step=config.lot_step,
        )

        # 4. Stop Loss & Take Profit Formatting & Stops Level Adjustment
        sl = float(intent.stop_loss)
        tp = float(intent.take_profit)
        if config.platform_type == 0 and config.is_ecn and intent.action_type == ActionType.MARKET_ENTRY:
            # In MT4 ECN mode, initial market order must be sent with SL=0, TP=0
            # to avoid broker rejection ERR_INVALID_STOPS (130).
            sl = 0.0
            tp = 0.0
        else:
            stops_pips = config.stops_level_pips
            if stops_pips <= 0.0 and config.stops_level_points > 0:
                mult = config.pip_multiplier if config.pip_multiplier > 0 else 10.0
                stops_pips = config.stops_level_points / mult

            if stops_pips > 0.0 and str(config.stops_level_policy).upper() == "ADJUST":
                pip_sz = float(self.pip_converter.get_pip_size(formatted_symbol))
                ref_price = float(intent.price)
                if ref_price <= 0.0 and config.quote_provider:
                    q = config.quote_provider(formatted_symbol)
                    if q:
                        ref_price = q.ask if intent.side == OrderSide.BUY else q.bid
                if ref_price <= 0.0 and formatted_symbol in self._latest_quotes:
                    bid, ask = self._latest_quotes[formatted_symbol]
                    ref_price = ask if intent.side == OrderSide.BUY else bid

                if ref_price > 0.0:
                    digits = 3 if pip_sz == 0.01 else (2 if pip_sz >= 0.1 else 5)
                    # Check and adjust Stop Loss
                    if sl > 0.0:
                        sl_dist = abs(ref_price - sl)
                        sl_dist_pips = sl_dist / pip_sz
                        if sl_dist_pips < (stops_pips - 1e-6):
                            if intent.side == OrderSide.BUY:
                                sl = round(ref_price - (stops_pips * pip_sz), digits)
                            else:
                                sl = round(ref_price + (stops_pips * pip_sz), digits)
                            logger.info(
                                "STOPS LEVEL ADJUST: Adjusted SL from %.5f to %.5f (%.1f pips) to satisfy broker stops level %.1f pips on %s",
                                intent.stop_loss, sl, stops_pips, stops_pips, config.terminal_id,
                            )
                    # Check and adjust Take Profit
                    if tp > 0.0:
                        tp_dist = abs(ref_price - tp)
                        tp_dist_pips = tp_dist / pip_sz
                        if tp_dist_pips < (stops_pips - 1e-6):
                            if intent.side == OrderSide.BUY:
                                tp = round(ref_price + (stops_pips * pip_sz), digits)
                            else:
                                tp = round(ref_price - (stops_pips * pip_sz), digits)
                            logger.info(
                                "STOPS LEVEL ADJUST: Adjusted TP from %.5f to %.5f (%.1f pips) to satisfy broker stops level %.1f pips on %s",
                                intent.take_profit, tp, stops_pips, stops_pips, config.terminal_id,
                            )

        # 5. FIFO Target Overwrite for Closes
        target_ticket = int(intent.order_ticket)
        is_fifo = getattr(config, "is_fifo", False) or getattr(config, "fifo_compliant", False)
        if is_fifo and intent.action_type in (ActionType.FULL_CLOSE, ActionType.PARTIAL_CLOSE) and target_ticket > 0:
            matching_open = self.get_open_positions_for_symbol(
                symbol=formatted_symbol,
                terminal_id=config.terminal_id,
            )
            if matching_open:
                oldest_pos = matching_open[0]
                oldest_ticket = int(oldest_pos["ticket"])
                if target_ticket != oldest_ticket:
                    logger.warning(
                        "FIFO ENFORCEMENT: Overwriting close target ticket #%d to oldest open ticket #%d for %s on %s",
                        target_ticket, oldest_ticket, formatted_symbol, config.terminal_id,
                    )
                    self.record_journey_event(
                        signal_id=intent.signal_id,
                        ticket=oldest_ticket,
                        magic_number=intent.magic_number,
                        terminal_id=config.terminal_id,
                        event_type="FIFO_TARGET_OVERWRITE",
                        status="MODIFIED",
                        details=f"FIFO Enforced: Overwrote target ticket #{target_ticket} to oldest open ticket #{oldest_ticket}",
                        source="engine_fifo_guard",
                    )
                    target_ticket = oldest_ticket
                    intent.order_ticket = oldest_ticket

        # 6. Signal ID and Timestamp
        now_ns = intent.timestamp_ns or time.time_ns()
        sig_id = intent.signal_id or int(now_ns & 0x7FFFFFFFFFFFFFFF)

        # 7. Construct translated OrderIntent
        return OrderIntent(
            symbol=formatted_symbol,
            side=intent.side,
            action_type=intent.action_type,
            volume=quantized_volume,
            price=float(intent.price),
            stop_loss=sl,
            take_profit=tp,
            slippage_cap_pips=float(intent.slippage_cap_pips),
            magic_number=int(intent.magic_number) if intent.magic_number else 123456,
            order_ticket=target_ticket,
            signal_id=sig_id,
            timestamp_ns=now_ns,
            flags=flags,
            crc32=0,
            reserved=int(intent.reserved),
        )

    # -------------------------------------------------------------------------
    # TP Ladder Decomposition & Management
    # -------------------------------------------------------------------------

    def decompose_tp_ladder(
        self,
        intent: OrderIntent,
        tp_ladder: List[float],
        weights: Optional[List[float]] = None,
        config: Optional[TerminalExecutionConfig] = None,
    ) -> List[OrderIntent]:
        """
        Decomposes a single OrderIntent with multiple Take Profits into laddered sub-intents.
        Each sub-intent carries its designated portion of the total volume and individual TP.
        """
        if not tp_ladder:
            return [intent]

        n = len(tp_ladder)
        if weights is None or len(weights) != n:
            if n == 3:
                norm_weights = [0.5, 0.3, 0.2]
            elif n == 2:
                norm_weights = [0.5, 0.5]
            else:
                norm_weights = [1.0 / n] * n
        else:
            total_w = sum(weights)
            norm_weights = [w / total_w for w in weights]

        min_lot = config.min_lot if config else 0.01
        max_lot = config.max_lot if config else 100.0
        lot_step = config.lot_step if config else 0.01

        sub_intents = []
        base_sig_id = intent.signal_id or int(time.time_ns() & 0x7FFFFFFFFFFFFFFF)

        for i, (tp_level, weight) in enumerate(zip(tp_ladder, norm_weights)):
            part_vol = quantize_lot(
                lot=intent.volume * weight,
                min_lot=min_lot,
                max_lot=max_lot,
                lot_step=lot_step,
            )
            sub_sig_id = base_sig_id + (i * 1000)
            sub_order = copy.copy(intent)
            sub_order.volume = part_vol
            sub_order.take_profit = tp_level
            sub_order.signal_id = sub_sig_id
            sub_intents.append(sub_order)

        return sub_intents

    async def submit_tp_ladder(
        self,
        terminal_id: str,
        intent: OrderIntent,
        tp_ladder: List[float],
        weights: Optional[List[float]] = None,
        break_even_config: Optional[BreakEvenConfig] = None,
    ) -> List[ExecutionResult]:
        """
        Orchestrates TP Ladder execution:
        - Hedging Account: Dispatches N separate orders to the broker, each with its
          respective TP target and volume split (e.g. 50%, 30%, 20%).
        - MT5 Netting Account: Dispatches a single combined parent order, and activates
          the Virtual TP Manager to issue partial closes as each TP price level is crossed.
        """
        config = self.get_terminal_config(terminal_id)
        if not tp_ladder:
            res = await self.submit_order(terminal_id, intent)
            return [res]

        pip_size = get_default_pip_size(intent.symbol)

        # ---------------------------------------------------------------------
        # Case A: MT5 Netting Account (Virtual TP Manager)
        # ---------------------------------------------------------------------
        if config.is_netting:
            logger.info(
                "Terminal %s is MT5 Netting account. Submitting single aggregated order "
                "with Virtual TP Manager active for %d TP milestones.",
                terminal_id, len(tp_ladder),
            )
            # Parent order submitted with full volume; broker TP can be 0.0 or final TP
            parent_intent = copy.copy(intent)
            parent_result = await self.submit_order(terminal_id, parent_intent)

            if not parent_result.success or parent_result.ticket == 0:
                return [parent_result]

            # Calculate volume distribution for virtual milestones
            n = len(tp_ladder)
            if weights is None or len(weights) != n:
                norm_weights = [0.5, 0.3, 0.2] if n == 3 else ([0.5, 0.5] if n == 2 else [1.0 / n] * n)
            else:
                total_w = sum(weights)
                norm_weights = [w / total_w for w in weights]

            virtual_tps: List[VirtualTPLevel] = []
            for i, (tp_price, w) in enumerate(zip(tp_ladder, norm_weights)):
                vol = quantize_lot(
                    lot=parent_result.executed_volume * w,
                    min_lot=config.min_lot,
                    max_lot=config.max_lot,
                    lot_step=config.lot_step,
                )
                virtual_tps.append(VirtualTPLevel(
                    level_index=i,
                    target_price=tp_price,
                    volume_to_close=vol,
                ))

            # Register position under Engine's Virtual TP & Break-Even tracker
            managed_pos = ManagedPosition(
                ticket=parent_result.ticket,
                terminal_id=terminal_id,
                symbol=intent.symbol,
                side=OrderSide(intent.side),
                entry_price=parent_result.executed_price,
                original_volume=parent_result.executed_volume,
                current_volume=parent_result.executed_volume,
                current_sl=intent.stop_loss,
                current_tp=0.0, # Managed virtually
                pip_size=pip_size,
                is_netting=True,
                virtual_tps=virtual_tps,
                break_even_config=break_even_config,
            )
            self.register_managed_position(managed_pos)
            return [parent_result]

        # ---------------------------------------------------------------------
        # Case B: Hedging Account (Physical Multi-Order Dispatch)
        # ---------------------------------------------------------------------
        sub_orders = self.decompose_tp_ladder(
            intent=intent,
            tp_ladder=tp_ladder,
            weights=weights,
            config=config,
        )

        results: List[ExecutionResult] = []
        group_id = f"GRP-{intent.signal_id}-{int(time.time())}"
        group_tickets: List[int] = []

        for sub_intent in sub_orders:
            res = await self.submit_order(terminal_id, sub_intent)
            results.append(res)
            if res.success and res.ticket > 0:
                group_tickets.append(res.ticket)
                # Register individual sub-order in managed positions
                managed_pos = ManagedPosition(
                    ticket=res.ticket,
                    terminal_id=terminal_id,
                    symbol=intent.symbol,
                    side=OrderSide(intent.side),
                    entry_price=res.executed_price,
                    original_volume=res.executed_volume,
                    current_volume=res.executed_volume,
                    current_sl=sub_intent.stop_loss,
                    current_tp=sub_intent.take_profit,
                    pip_size=pip_size,
                    is_netting=False,
                    group_id=group_id,
                    virtual_tps=[VirtualTPLevel(level_index=0, target_price=tp_ladder[0], volume_to_close=res.executed_volume)],
                    break_even_config=break_even_config,
                )
                self.register_managed_position(managed_pos)

        if group_tickets:
            self._position_groups[group_id] = group_tickets

        return results

    # -------------------------------------------------------------------------
    # Trade Management: Virtual TP Manager & Break-Even Logic
    # -------------------------------------------------------------------------

    def register_managed_position(self, position: ManagedPosition) -> None:
        """Register an active position for Virtual TP and Break-Even management."""
        self._managed_positions[position.ticket] = position
        logger.info(
            "Registered managed position #%d on %s (Symbol=%s, Vol=%.2f, Netting=%s, BE=%s)",
            position.ticket, position.terminal_id, position.symbol, position.current_volume,
            position.is_netting, bool(position.break_even_config and position.break_even_config.enabled),
        )

    def get_managed_position(self, ticket: int) -> Optional[ManagedPosition]:
        """Retrieve tracking record for a managed position."""
        return self._managed_positions.get(ticket)

    def get_all_managed_positions(self) -> List[ManagedPosition]:
        """Return all currently managed positions."""
        return list(self._managed_positions.values())

    async def on_price_update(
        self,
        symbol: str,
        bid: float,
        ask: float,
        terminal_id: Optional[str] = None,
    ) -> List[ExecutionResult]:
        """
        Evaluates incoming live quote telemetry across all managed positions:
        1. Virtual TP Manager: Checks if price crossed any virtual TP milestone and issues partial closes.
        2. Break-Even Manager: Checks if price reached BE threshold and moves SL to entry + offset.
        """
        self._latest_quotes[symbol] = (bid, ask)
        results: List[ExecutionResult] = []

        # 1. Evaluate Virtual TPs
        vtp_results = await self.process_virtual_tps(symbol, bid, ask, terminal_id)
        results.extend(vtp_results)

        # 2. Evaluate Break-Even Modifications
        be_results = await self.process_break_even(symbol, bid, ask, terminal_id)
        results.extend(be_results)

        return results

    async def process_virtual_tps(
        self,
        symbol: str,
        bid: float,
        ask: float,
        terminal_id: Optional[str] = None,
    ) -> List[ExecutionResult]:
        """
        Virtual TP Manager for MT5 Netting Accounts:
        Iterates over active netting positions and executes partial/full closes
        as each virtual TP level is reached.
        """
        results: List[ExecutionResult] = []

        for pos in list(self._managed_positions.values()):
            if pos.closed or not pos.is_netting or pos.symbol != symbol:
                continue
            if terminal_id and pos.terminal_id != terminal_id:
                continue

            for vtp in pos.virtual_tps:
                if vtp.is_triggered:
                    continue

                # Check hit condition
                is_hit = False
                close_price = bid if pos.side == OrderSide.BUY else ask
                if pos.side == OrderSide.BUY and bid >= vtp.target_price:
                    is_hit = True
                elif pos.side == OrderSide.SELL and ask <= vtp.target_price:
                    is_hit = True

                if not is_hit:
                    # In ladder, targets are sequential; stop evaluating further unhit levels
                    break

                # Target reached! Determine if partial close or final full close
                close_vol = min(vtp.volume_to_close, pos.current_volume)
                is_final = (vtp == pos.virtual_tps[-1]) or (close_vol >= pos.current_volume)
                action = ActionType.FULL_CLOSE if is_final else ActionType.PARTIAL_CLOSE

                logger.info(
                    "Virtual TP %d triggered for netting position #%d on %s at %.5f (target=%.5f, vol=%.2f, action=%s)",
                    vtp.level_index + 1, pos.ticket, pos.terminal_id, close_price,
                    vtp.target_price, close_vol, action.name,
                )

                # Construct and dispatch close command
                close_intent = OrderIntent(
                    symbol=pos.symbol,
                    side=OrderSide.SELL if pos.side == OrderSide.BUY else OrderSide.BUY,
                    action_type=action,
                    volume=close_vol,
                    price=close_price,
                    order_ticket=pos.ticket,
                    signal_id=int(time.time_ns() & 0x7FFFFFFFFFFFFFFF),
                    timestamp_ns=time.time_ns(),
                )

                res = await self.submit_order(pos.terminal_id, close_intent)
                results.append(res)

                if res.success:
                    vtp.is_triggered = True
                    vtp.triggered_at = time.time()
                    pos.current_volume = max(0.0, round(pos.current_volume - close_vol, 5))
                    if is_final or pos.current_volume <= 0.0:
                        pos.closed = True
                        logger.info("Managed netting position #%d completely closed.", pos.ticket)
                    break

        return results

    async def process_break_even(
        self,
        symbol: str,
        bid: float,
        ask: float,
        terminal_id: Optional[str] = None,
    ) -> List[ExecutionResult]:
        """
        Break-Even Manager:
        Evaluates active positions against their Break-Even rules.
        When triggered, dispatches an OrderModify command to move SL to entry + offset.
        """
        results: List[ExecutionResult] = []

        for pos in list(self._managed_positions.values()):
            if pos.closed or pos.is_be_activated or pos.symbol != symbol:
                continue
            if terminal_id and pos.terminal_id != terminal_id:
                continue
            if not pos.break_even_config or not pos.break_even_config.enabled:
                continue

            be_cfg = pos.break_even_config
            curr_price = bid if pos.side == OrderSide.BUY else ask

            # 1. Evaluate Trigger Condition
            is_triggered = False
            if be_cfg.trigger_mode == "tp1" and pos.virtual_tps:
                tp1_target = pos.virtual_tps[0].target_price
                if pos.side == OrderSide.BUY and bid >= tp1_target:
                    is_triggered = True
                elif pos.side == OrderSide.SELL and ask <= tp1_target:
                    is_triggered = True

            elif be_cfg.trigger_mode == "pips":
                pip_sz = pos.pip_size
                if pos.side == OrderSide.BUY:
                    profit_pips = (bid - pos.entry_price) / pip_sz
                else:
                    profit_pips = (pos.entry_price - ask) / pip_sz
                if profit_pips >= be_cfg.trigger_pips:
                    is_triggered = True

            elif be_cfg.trigger_mode == "price":
                if pos.side == OrderSide.BUY and bid >= be_cfg.trigger_price:
                    is_triggered = True
                elif pos.side == OrderSide.SELL and ask <= be_cfg.trigger_price:
                    is_triggered = True

            if not is_triggered:
                continue

            # 2. Compute New Stop Loss Level
            offset_dist = (be_cfg.offset_pips * pos.pip_size) + be_cfg.offset_price
            if pos.side == OrderSide.BUY:
                new_sl = round(pos.entry_price + offset_dist, 5)
                # Must be an improvement over current SL
                if pos.current_sl > 0.0 and new_sl <= pos.current_sl:
                    continue
            else:
                new_sl = round(pos.entry_price - offset_dist, 5)
                if pos.current_sl > 0.0 and new_sl >= pos.current_sl:
                    continue

            logger.info(
                "Break-Even triggered for position #%d on %s at %.5f! Moving SL from %.5f to %.5f (Entry=%.5f + offset=%.1f pips)",
                pos.ticket, pos.terminal_id, curr_price, pos.current_sl, new_sl, pos.entry_price, be_cfg.offset_pips,
            )

            # 3. Dispatch OrderModify Intent
            modify_intent = OrderIntent(
                symbol=pos.symbol,
                side=pos.side,
                action_type=ActionType.MODIFY_SL_TP,
                volume=pos.current_volume,
                price=pos.entry_price,
                stop_loss=new_sl,
                take_profit=pos.current_tp,
                order_ticket=pos.ticket,
                signal_id=int(time.time_ns() & 0x7FFFFFFFFFFFFFFF),
                timestamp_ns=time.time_ns(),
            )

            res = await self.submit_order(pos.terminal_id, modify_intent)
            results.append(res)

            if res.success:
                pos.current_sl = new_sl
                pos.is_be_activated = True
                pos.be_activated_at = time.time()

                # If this position belongs to a Hedging group (e.g. multi-order TP ladder),
                # also advance Break-Even on all peer positions in the same group!
                if pos.group_id and pos.group_id in self._position_groups:
                    peer_tickets = self._position_groups[pos.group_id]
                    for ptick in peer_tickets:
                        if ptick != pos.ticket and ptick in self._managed_positions:
                            peer_pos = self._managed_positions[ptick]
                            if not peer_pos.closed and not peer_pos.is_be_activated:
                                peer_modify = OrderIntent(
                                    symbol=peer_pos.symbol,
                                    side=peer_pos.side,
                                    action_type=ActionType.MODIFY_SL_TP,
                                    volume=peer_pos.current_volume,
                                    price=peer_pos.entry_price,
                                    stop_loss=new_sl,
                                    take_profit=peer_pos.current_tp,
                                    order_ticket=peer_pos.ticket,
                                    signal_id=int(time.time_ns() & 0x7FFFFFFFFFFFFFFF),
                                    timestamp_ns=time.time_ns(),
                                )
                                peer_res = await self.submit_order(peer_pos.terminal_id, peer_modify)
                                results.append(peer_res)
                                if peer_res.success:
                                    peer_pos.current_sl = new_sl
                                    peer_pos.is_be_activated = True
                                    peer_pos.be_activated_at = time.time()

        return results

    def _handle_account_state_telemetry(self, terminal_id: str, state: AccountState) -> None:
        """Process incoming account state telemetry streamed over Named Pipe."""
        self._terminal_account_states[terminal_id] = state

    def get_account_states(self) -> Dict[str, AccountState]:
        """Returns the latest AccountState telemetry per terminal."""
        return dict(self._terminal_account_states)

    def get_total_equity(self) -> float:
        """Returns aggregate equity across all connected terminals."""
        return sum(st.equity for st in self._terminal_account_states.values())

    def get_total_balance(self) -> float:
        """Returns aggregate balance across all connected terminals."""
        return sum(st.balance for st in self._terminal_account_states.values())

    # -------------------------------------------------------------------------
    # Sprint 15: Failover Role & Standby Lockout
    # -------------------------------------------------------------------------

    def set_engine_role(self, role: Union[EngineRole, str]) -> None:
        """Sets the active execution role (ROLE_ACTIVE, ROLE_STANDBY, ROLE_SAFE_HALT)."""
        self.role = EngineRole(role)
        logger.info("ExecutionOrchestrator role updated to: %s", self.role.value)

    def unlock(self) -> None:
        """Step D Takeover: Unlocks the orchestrator for live trading execution."""
        self.set_engine_role(EngineRole.ROLE_ACTIVE)

    def lock(self) -> None:
        """Locks the orchestrator, enforcing Standby Lockout on all OrderSend operations."""
        self.set_engine_role(EngineRole.ROLE_STANDBY)

    def is_standby(self) -> bool:
        """Returns True if the orchestrator is locked in Standby mode."""
        return self.role == EngineRole.ROLE_STANDBY

    def is_active(self) -> bool:
        """Returns True if the orchestrator is in Active execution mode."""
        return self.role == EngineRole.ROLE_ACTIVE

    def is_signal_already_open(self, signal_id: int, terminal_id: Optional[str] = None) -> bool:
        """
        Sprint 16 Deduplication Gate:
        Returns True if a trade corresponding to signal_id is already active/open
        in the SQLite database or has been discovered open on the broker terminal.
        """
        if signal_id <= 0:
            return False

        # 1. Check in-memory discovered signals map from reconciliation
        if signal_id in self._discovered_open_signals:
            return True

        # 2. Query SQLite order_intents table
        try:
            with self._db_conn:
                cursor = self._db_conn.cursor()
                if terminal_id:
                    cursor.execute(
                        "SELECT signal_id, status FROM order_intents WHERE signal_id = ? AND terminal_id = ?",
                        (signal_id, terminal_id),
                    )
                else:
                    cursor.execute(
                        "SELECT signal_id, status FROM order_intents WHERE signal_id = ?",
                        (signal_id,),
                    )
                row = cursor.fetchone()
                if row and str(row["status"]).upper() in ("OPEN", "PENDING", "FILLED", "ADOPTED"):
                    return True
        except Exception as e:
            logger.debug("Error querying order_intents for signal %d: %s", signal_id, e)

        # 3. Check live/cached broker positions via IPCDispatcher / Reconciliation
        if getattr(self, "reconciliation_engine", None) and hasattr(self.reconciliation_engine, "is_signal_open"):
            if self.reconciliation_engine.is_signal_open(signal_id):
                return True

        if self.ipc_dispatcher and hasattr(self.ipc_dispatcher, "get_broker_positions"):
            try:
                broker_positions = self.ipc_dispatcher.get_broker_positions(terminal_id)
                for p in broker_positions:
                    if p.magic_number == self.default_magic_number:
                        from engine.orphan_manager import OrphanManager
                        extracted = OrphanManager.extract_signal_id(p)
                        if extracted == signal_id:
                            self._discovered_open_signals[signal_id] = p.ticket
                            return True
            except Exception as e:
                logger.debug("Error inspecting broker positions for signal %d: %s", signal_id, e)

        return False

    # -------------------------------------------------------------------------
    # Per-Account Serialization Queue & Concurrency Logic
    # -------------------------------------------------------------------------

    def _notify_skip(self, intent: OrderIntent, skip: SkipReason, terminal_id: str) -> None:
        """Notifies registered event listeners of a skipped signal."""
        if self._event_listeners:
            evt_dict = {
                "event_type": "SIGNAL_SKIPPED",
                "signal_id": intent.signal_id,
                "symbol": intent.symbol,
                "side": intent.side.name if hasattr(intent.side, "name") else str(intent.side),
                "volume": intent.volume,
                "skip_reason": skip,
                "details": skip.reason,
                "terminal_id": terminal_id,
                "timestamp": time.time(),
            }
            for cb in self._event_listeners:
                try:
                    res = cb(evt_dict)
                    if asyncio.iscoroutine(res):
                        asyncio.create_task(res)
                except Exception:
                    pass

    async def submit_order(
        self,
        terminal_id: str,
        intent: OrderIntent,
        account_profile: Optional[AccountProfile] = None,
    ) -> ExecutionResult:
        """
        Enqueues an OrderIntent into the account's dedicated serialization queue.
        Guarantees that multiple concurrent signals for this terminal are processed
        strictly sequentially without race conditions.

        Standby Lockout Guard (Sprint 15):
        If operating in ROLE_STANDBY mode, all OrderSend commands are strictly locked
        and rejected with code SKIP_STANDBY_LOCK to prevent split-brain execution.

        Duplicate Execution Guard (Sprint 16):
        If an order for intent.signal_id is already open on the broker (discovered via
        Reconciliation loop or local database), strictly rejects with code SKIP_DUP.

        FIFO Compliance Guard (Sprint 20):
        If account is FIFO compliant (US NFA Rule 2-43(b)):
        1. Intercepts and blocks hedging attempts with SKIP_FIFO_HEDGE.
        2. Routes closures strictly against the oldest ticket ID.
        """
        # Standby / Partition-Locked Guard (Sprint 15 / FR-7.4 & FR-7.5)
        if self.role in (EngineRole.ROLE_STANDBY, EngineRole.ROLE_LOCKED, EngineRole.ROLE_SAFE_HALT) or (
            self.failover_manager and not self.failover_manager.can_execute_orders
        ):
            role_val = self.role.value if hasattr(self.role, "value") else str(self.role)
            logger.warning(
                "STANDBY LOCKOUT ENFORCED: Engine is in %s mode. Blocking OrderSend for signal %d on %s",
                role_val,
                intent.signal_id,
                terminal_id,
            )
            skip = SkipReason(
                code="SKIP_STANDBY_LOCK",
                reason=f"Order dispatch blocked: Local Engine is operating in {role_val} mode. Standby Lockout enforced.",
                evaluated_at=time.time(),
                details={"role": role_val, "signal_id": intent.signal_id, "terminal_id": terminal_id},
            )
            self._notify_skip(intent, skip, terminal_id)
            return ExecutionResult(
                terminal_id=terminal_id,
                signal_id=intent.signal_id,
                success=False,
                status=ExecutionStatus.REJECTED,
                skip_reason=skip,
            )

        # Remote Command Paused Guard (Sprint 17)
        if self._is_paused and intent.action_type not in (ActionType.FULL_CLOSE, ActionType.PARTIAL_CLOSE):
            logger.warning("TRADING PAUSED: Rejecting entry order for signal %d on %s", intent.signal_id, terminal_id)
            skip = SkipReason(
                code="SKIP_TRADING_PAUSED",
                reason="Trading is paused via remote mobile command.",
                evaluated_at=time.time(),
                details={"signal_id": intent.signal_id, "terminal_id": terminal_id},
            )
            self._notify_skip(intent, skip, terminal_id)
            return ExecutionResult(
                terminal_id=terminal_id,
                signal_id=intent.signal_id,
                success=False,
                status=ExecutionStatus.REJECTED,
                skip_reason=skip,
            )

        # Duplicate Execution Guard (Sprint 16: Takeover Backfill Deduplication)
        if intent.signal_id > 0 and self.is_signal_already_open(intent.signal_id, terminal_id):
            logger.warning(
                "SKIP_DUP ENFORCED: Signal %d is already open on broker terminal %s. Blocking duplicate OrderSend.",
                intent.signal_id,
                terminal_id,
            )
            skip = SkipReason(
                code="SKIP_DUP",
                reason=f"SKIP_DUP: Signal {intent.signal_id} is already open on broker terminal {terminal_id}. Takeover reconciliation deduplication enforced.",
                evaluated_at=time.time(),
                details={"code": "SKIP_DUP", "signal_id": intent.signal_id, "terminal_id": terminal_id},
            )
            self._notify_skip(intent, skip, terminal_id)
            return ExecutionResult(
                terminal_id=terminal_id,
                signal_id=intent.signal_id,
                success=False,
                status=ExecutionStatus.REJECTED,
                skip_reason=skip,
            )

        # Symbol Auto-Mapping Guard (Sprint 19)
        if self.symbol_mapper and intent.symbol:
            map_res = self.symbol_mapper.resolve(intent.symbol, terminal_id=terminal_id)
            if not map_res.success:
                skip = map_res.skip_reason or SkipReason(
                    code=SKIP_SYMBOL,
                    reason=f"Symbol '{intent.symbol}' could not be mapped to any tradable broker asset on terminal {terminal_id}.",
                    evaluated_at=time.time(),
                    details={"terminal_id": terminal_id, "symbol": intent.symbol},
                )
                logger.warning("SKIP_SYMBOL ENFORCED: %s", skip.reason)
                self._notify_skip(intent, skip, terminal_id)
                return ExecutionResult(
                    terminal_id=terminal_id,
                    signal_id=intent.signal_id,
                    success=False,
                    status=ExecutionStatus.REJECTED,
                    skip_reason=skip,
                )

        # FIFO Compliance Guard (Sprint 20: US NFA Rule 2-43(b))
        is_fifo = self.is_fifo_compliant(terminal_id=terminal_id, account_profile=account_profile)
        if is_fifo:
            # Task 2: Hedge Blocking: Prohibit opposite side entry on same symbol
            if intent.action_type in (
                ActionType.MARKET_ENTRY,
                ActionType.LIMIT_ENTRY,
                ActionType.STOP_ENTRY,
            ) or int(intent.action_type) == 0:
                opposing_side = 1 if int(intent.side) == 0 else 0
                opposing_positions = self.get_open_positions_for_symbol(
                    symbol=intent.symbol,
                    terminal_id=terminal_id,
                    side=opposing_side,
                )
                if opposing_positions:
                    side_str = "SELL" if int(intent.side) == 1 else "BUY"
                    opp_str = "BUY" if opposing_side == 0 else "SELL"
                    logger.warning(
                        "SKIP_FIFO_HEDGE ENFORCED: Rejecting %s %s signal. Opposing %s position(s) [%s] already open on terminal %s.",
                        side_str,
                        intent.symbol,
                        opp_str,
                        ", ".join(str(p["ticket"]) for p in opposing_positions),
                        terminal_id,
                    )
                    skip = SkipReason(
                        code=SKIP_FIFO_HEDGE,
                        reason=f"SKIP_FIFO_HEDGE: Cannot execute {side_str} on {intent.symbol} because opposing {opp_str} position(s) exist on {terminal_id} under US FIFO rules.",
                        evaluated_at=time.time(),
                        details={
                            "code": SKIP_FIFO_HEDGE,
                            "symbol": intent.symbol,
                            "side": side_str,
                            "terminal_id": terminal_id,
                            "opposing_tickets": [p["ticket"] for p in opposing_positions],
                        },
                    )
                    self._notify_skip(intent, skip, terminal_id)
                    self.record_journey_event(
                        signal_id=intent.signal_id,
                        ticket=0,
                        magic_number=intent.magic_number,
                        terminal_id=terminal_id,
                        event_type=SKIP_FIFO_HEDGE,
                        status="REJECTED",
                        details=skip.reason,
                        source="fifo_compliance_guard",
                    )
                    return ExecutionResult(
                        terminal_id=terminal_id,
                        signal_id=intent.signal_id,
                        success=False,
                        status=ExecutionStatus.REJECTED,
                        skip_reason=skip,
                    )

            # Task 3: Closure Routing
            # When the orchestrator attempts to close a portion of an accumulated position,
            # query local DB, sort open positions by timestamp, execute strictly against oldest ticket ID.
            elif intent.action_type in (ActionType.PARTIAL_CLOSE, ActionType.FULL_CLOSE):
                matching_open = self.get_open_positions_for_symbol(
                    symbol=intent.symbol,
                    terminal_id=terminal_id,
                )
                if matching_open:
                    oldest_pos = matching_open[0]
                    oldest_ticket = int(oldest_pos["ticket"])
                    oldest_vol = float(oldest_pos["volume"])

                    if intent.order_ticket != oldest_ticket:
                        original_ticket = intent.order_ticket
                        logger.info(
                            "FIFO CLOSURE ROUTING: Re-targeting close command for %s from ticket #%s to oldest ticket #%s (created_at=%.3f)",
                            intent.symbol,
                            original_ticket,
                            oldest_ticket,
                            float(oldest_pos.get("created_at") or 0.0),
                        )
                        intent.order_ticket = oldest_ticket
                        self.record_journey_event(
                            signal_id=intent.signal_id,
                            ticket=oldest_ticket,
                            magic_number=intent.magic_number,
                            terminal_id=terminal_id,
                            event_type="FIFO_TARGET_OVERWRITE",
                            status="MODIFIED",
                            details=f"FIFO Enforced: Overwrote target ticket #{original_ticket} to oldest open ticket #{oldest_ticket}",
                            source="engine_fifo_guard",
                        )

                    if intent.volume < oldest_vol:
                        intent.action_type = ActionType.PARTIAL_CLOSE
                    elif intent.volume >= oldest_vol and intent.action_type == ActionType.FULL_CLOSE:
                        intent.volume = oldest_vol

        config = self.get_terminal_config(terminal_id)

        # Freeze Level & Stops Level Guard (Sprint 20 Directive)
        guard_res = self.freeze_level_guard.evaluate(
            terminal_id=terminal_id,
            intent=intent,
            account_profile=account_profile,
        )
        if isinstance(guard_res, SkipReason):
            logger.warning("%s: %s", guard_res.code, guard_res.reason)
            self._notify_skip(intent, guard_res, terminal_id)
            return ExecutionResult(
                terminal_id=terminal_id,
                signal_id=intent.signal_id,
                success=False,
                status=ExecutionStatus.REJECTED,
                skip_reason=guard_res,
            )
        intent = guard_res

        queue = self._get_or_create_queue(terminal_id)

        if queue.qsize() >= self.max_queue_depth:
            skip = SkipReason(
                code="SKIP_QUEUE_FULL",
                reason=f"Serialization queue full for terminal {terminal_id} (depth={queue.qsize()})",
                evaluated_at=time.time(),
                details={"terminal_id": terminal_id, "depth": queue.qsize()},
            )
            self._notify_skip(intent, skip, terminal_id)
            return ExecutionResult(
                terminal_id=terminal_id,
                signal_id=intent.signal_id,
                success=False,
                status=ExecutionStatus.REJECTED,
                skip_reason=skip,
            )

        loop = asyncio.get_running_loop()
        future: asyncio.Future[ExecutionResult] = loop.create_future()
        task = _QueuedOrderTask(
            intent=intent,
            terminal_id=terminal_id,
            config=config,
            future=future,
        )

        await queue.put(task)
        # Ensure worker is actively polling this queue
        self._ensure_worker_running(terminal_id)

        # Await completion of this specific sequential order
        return await future

    async def orchestrate_signal(
        self,
        terminal_id: str,
        intent: OrderIntent,
        account_profile: AccountProfile,
        quote: MarketQuote,
    ) -> ExecutionResult:
        """
        Full end-to-end pipeline:
        1. Evaluates signal against RiskEngine rules and entry guards.
        2. If evaluated successfully, formats for target terminal.
        3. Enqueues in per-account serialization queue for sequential execution.
        """
        # Step 1: Risk Engine Evaluation
        if self.risk_engine is not None:
            eval_result = self.risk_engine.evaluate(intent, account_profile, quote)
            if isinstance(eval_result, SkipReason):
                logger.warning(
                    "Order %d rejected by RiskEngine: %s (%s)",
                    intent.signal_id, eval_result.code, eval_result.reason,
                )
                return ExecutionResult(
                    terminal_id=terminal_id,
                    signal_id=intent.signal_id,
                    success=False,
                    status=ExecutionStatus.REJECTED,
                    skip_reason=eval_result,
                )
            evaluated_intent = eval_result
        else:
            evaluated_intent = intent

        # Step 2 & 3: Translation and Sequenced Queue Submission
        return await self.submit_order(terminal_id, evaluated_intent, account_profile=account_profile)

    def _get_or_create_queue(self, terminal_id: str) -> asyncio.Queue[_QueuedOrderTask]:
        """Retrieve or allocate per-account serialization queue."""
        if terminal_id not in self._account_queues:
            self._account_queues[terminal_id] = asyncio.Queue(maxsize=self.max_queue_depth)
        return self._account_queues[terminal_id]

    def _ensure_worker_running(self, terminal_id: str) -> None:
        """Starts background queue worker task if not currently running."""
        worker = self._worker_tasks.get(terminal_id)
        if worker is None or worker.done():
            self._worker_tasks[terminal_id] = asyncio.create_task(
                self._account_worker_loop(terminal_id),
                name=f"ExecutionWorker-{terminal_id}",
            )

    async def _account_worker_loop(self, terminal_id: str) -> None:
        """
        Dedicated per-terminal worker loop.
        Dequeues orders strictly one-at-a-time, dispatches them through the IPC
        Shared Memory pipeline, and awaits feedback / retry completion before
        proceeding to the next order.
        """
        queue = self._get_or_create_queue(terminal_id)
        logger.debug("Started serialization worker loop for terminal %s", terminal_id)

        while True:
            try:
                task = await queue.get()
                try:
                    result = await self._execute_single_order_with_retry(
                        terminal_id=task.terminal_id,
                        intent=task.intent,
                        config=task.config,
                    )
                    if not task.future.done():
                        task.future.set_result(result)
                except Exception as e:
                    logger.error("Unexpected error in worker loop for %s: %s", terminal_id, e)
                    if not task.future.done():
                        task.future.set_exception(e)
                finally:
                    queue.task_done()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Fatal error in queue worker for %s: %s", terminal_id, e)
                await asyncio.sleep(0.1)

    # -------------------------------------------------------------------------
    # Retry & Backoff Mechanism
    # -------------------------------------------------------------------------

    async def _execute_single_order_with_retry(
        self,
        terminal_id: str,
        intent: OrderIntent,
        config: TerminalExecutionConfig,
    ) -> ExecutionResult:
        """
        Executes a single OrderIntent with transient rejection retry logic:
        1. Translates intent for platform (MT4 vs MT5).
        2. Dispatches to IPC Shared Memory ring buffer.
        3. Awaits ExecutionFeedback over Named Pipe.
        4. If broker returns requote or busy server, retries up to max_retries (3)
           with 500ms exponential backoff (0.5s, 1.0s, 2.0s).
        5. If retries exhausted or permanent error, returns failed ExecutionResult
           with formatted SkipReason.
        """
        max_retries = config.max_retries
        base_backoff_s = config.base_backoff_s
        loop = asyncio.get_running_loop()

        translated = self.translate_intent(intent, config)
        current_intent = translated

        for attempt in range(max_retries + 1):
            # 1. Check terminal routing availability
            if hasattr(self.ipc_dispatcher, "can_route_to_terminal") and not self.ipc_dispatcher.can_route_to_terminal(terminal_id):
                skip = SkipReason(
                    code="SKIP_TERMINAL_UNAVAILABLE",
                    reason=f"Terminal {terminal_id} routing is paused, disconnected, or unready.",
                    evaluated_at=time.time(),
                    details={"terminal_id": terminal_id, "attempt": attempt + 1},
                )
                return ExecutionResult(
                    terminal_id=terminal_id,
                    signal_id=current_intent.signal_id,
                    success=False,
                    status=ExecutionStatus.REJECTED,
                    attempts=attempt + 1,
                    skip_reason=skip,
                    completed_at=time.time(),
                )

            # 2. Refresh quote on retry if quote_provider is registered
            if attempt > 0 and config.quote_provider:
                fresh_quote = config.quote_provider(current_intent.symbol)
                if fresh_quote:
                    fresh_price = (
                        fresh_quote.ask
                        if int(current_intent.side) == int(OrderSide.BUY)
                        else fresh_quote.bid
                    )
                    current_intent.price = fresh_price

            # 3. Register future for incoming Named Pipe execution feedback
            feedback_future: asyncio.Future[ExecutionFeedback] = loop.create_future()
            key = (terminal_id, current_intent.signal_id)
            self._pending_feedback[key] = feedback_future

            # 4. Dispatch to SPSC Shared Memory ring buffer (<50 µs zero-copy operation)
            dispatched = self.ipc_dispatcher.dispatch_order_intent(terminal_id, current_intent)
            if asyncio.iscoroutine(dispatched) or hasattr(dispatched, "__await__"):
                dispatched = await dispatched
            if not dispatched:
                self._pending_feedback.pop(key, None)
                skip = SkipReason(
                    code="SKIP_DISPATCH_FAILED",
                    reason=f"Failed to push OrderIntent into Shared Memory ring buffer for {terminal_id}.",
                    evaluated_at=time.time(),
                    details={"terminal_id": terminal_id, "signal_id": current_intent.signal_id},
                )
                return ExecutionResult(
                    terminal_id=terminal_id,
                    signal_id=current_intent.signal_id,
                    success=False,
                    status=ExecutionStatus.REJECTED,
                    attempts=attempt + 1,
                    skip_reason=skip,
                    completed_at=time.time(),
                )

            # 5. Await broker execution confirmation
            try:
                feedback = await asyncio.wait_for(
                    feedback_future,
                    timeout=config.execution_timeout_s,
                )
            except asyncio.TimeoutError:
                self._pending_feedback.pop(key, None)
                if attempt < max_retries:
                    backoff = base_backoff_s * (config.backoff_multiplier ** attempt)
                    logger.warning(
                        "Order %d timed out on %s. Retrying (%d/%d) in %.2fs...",
                        current_intent.signal_id, terminal_id, attempt + 1, max_retries, backoff
                    )
                    await asyncio.sleep(backoff)
                    continue
                else:
                    skip = SkipReason(
                        code="SKIP_BROKER_TIMEOUT",
                        reason=(
                            f"Execution timed out waiting for feedback from terminal {terminal_id} "
                            f"after {max_retries} retries."
                        ),
                        evaluated_at=time.time(),
                        details={
                            "signal_id": current_intent.signal_id,
                            "terminal_id": terminal_id,
                            "attempts": attempt + 1,
                        },
                    )
                    return ExecutionResult(
                        terminal_id=terminal_id,
                        signal_id=current_intent.signal_id,
                        success=False,
                        status=ExecutionStatus.EXPIRED,
                        attempts=attempt + 1,
                        skip_reason=skip,
                        completed_at=time.time(),
                    )
            finally:
                self._pending_feedback.pop(key, None)

            # 6. Evaluate broker execution outcome
            if feedback.execution_status in (ExecutionStatus.FILLED, ExecutionStatus.PARTIAL):
                # Successful execution
                # Record in internal SQLite state and Journey log (Sprint 14)
                if intent.action_type in (ActionType.MARKET_ENTRY, ActionType.LIMIT_ENTRY, ActionType.STOP_ENTRY):
                    await self.record_order_intent_async(
                        signal_id=current_intent.signal_id,
                        ticket=feedback.order_ticket,
                        magic_number=current_intent.magic_number,
                        terminal_id=terminal_id,
                        symbol=current_intent.symbol,
                        side=int(current_intent.side),
                        volume=feedback.executed_volume,
                        open_price=feedback.executed_price,
                        stop_loss=current_intent.stop_loss,
                        take_profit=current_intent.take_profit,
                        status="OPEN",
                    )
                    await self.record_journey_event_async(
                        signal_id=current_intent.signal_id,
                        ticket=feedback.order_ticket,
                        magic_number=current_intent.magic_number,
                        terminal_id=terminal_id,
                        event_type="ORDER_FILLED",
                        status="OPEN",
                        details=f"Order #{feedback.order_ticket} filled on {terminal_id} (Volume={feedback.executed_volume:.2f}, Price={feedback.executed_price:.5f})",
                        source="execution_feedback",
                    )
                elif intent.action_type == ActionType.FULL_CLOSE:
                    target_ticket = current_intent.order_ticket or intent.order_ticket
                    if target_ticket in self._managed_positions:
                        self._managed_positions[target_ticket].closed = True
                        self._managed_positions[target_ticket].current_volume = 0.0
                    def _do_full_close():
                        with self._db_lock:
                            cur = self._db_conn.cursor()
                            cur.execute(
                                "UPDATE order_intents SET status = 'CLOSED', updated_at = ?, closed_at = ? WHERE ticket = ?",
                                (time.time(), time.time(), target_ticket),
                            )
                            self._db_conn.commit()
                    await self._run_db_write_async(_do_full_close)
                    await self.record_journey_event_async(
                        signal_id=current_intent.signal_id,
                        ticket=target_ticket,
                        magic_number=current_intent.magic_number,
                        terminal_id=terminal_id,
                        event_type="CLOSED_BY_ENGINE",
                        status="CLOSED",
                        details=f"Order #{target_ticket} closed by engine at {feedback.executed_price:.5f}",
                        source="engine_orchestrator",
                    )
                elif intent.action_type == ActionType.PARTIAL_CLOSE:
                    now = time.time()
                    target_ticket = current_intent.order_ticket or intent.order_ticket
                    def _do_partial_close():
                        with self._db_lock:
                            cur = self._db_conn.cursor()
                            cur.execute(
                                "SELECT volume FROM order_intents WHERE ticket = ?",
                                (target_ticket,),
                            )
                            row = cur.fetchone()
                            if row:
                                curr_vol = float(row["volume"])
                                rem_vol = max(0.0, round(curr_vol - feedback.executed_volume, 5))
                                if rem_vol <= 0.00001:
                                    cur.execute(
                                        "UPDATE order_intents SET status = 'CLOSED', volume = 0.0, updated_at = ?, closed_at = ? WHERE ticket = ?",
                                        (now, now, target_ticket),
                                    )
                                else:
                                    cur.execute(
                                        "UPDATE order_intents SET volume = ?, updated_at = ? WHERE ticket = ?",
                                        (rem_vol, now, target_ticket),
                                    )
                                self._db_conn.commit()
                    await self._run_db_write_async(_do_partial_close)
                    await self.record_journey_event_async(
                        signal_id=current_intent.signal_id,
                        ticket=target_ticket,
                        magic_number=current_intent.magic_number,
                        terminal_id=terminal_id,
                        event_type="PARTIAL_CLOSED_BY_ENGINE",
                        status="PARTIAL",
                        details=f"Order #{target_ticket} partially closed by engine at {feedback.executed_price:.5f} (closed_vol={feedback.executed_volume:.2f})",
                        source="engine_orchestrator",
                    )
                elif intent.action_type == ActionType.MODIFY_SL_TP:
                    target_ticket = current_intent.order_ticket or intent.order_ticket
                    if target_ticket in self._managed_positions:
                        self._managed_positions[target_ticket].current_sl = current_intent.stop_loss
                        self._managed_positions[target_ticket].current_tp = current_intent.take_profit
                    def _do_modify():
                        with self._db_lock:
                            cur = self._db_conn.cursor()
                            cur.execute(
                                "UPDATE order_intents SET stop_loss = ?, take_profit = ?, updated_at = ? WHERE ticket = ?",
                                (current_intent.stop_loss, current_intent.take_profit, time.time(), target_ticket),
                            )
                            self._db_conn.commit()
                    await self._run_db_write_async(_do_modify)
                    await self.record_journey_event_async(
                        signal_id=current_intent.signal_id,
                        ticket=target_ticket,
                        magic_number=current_intent.magic_number,
                        terminal_id=terminal_id,
                        event_type="SL_MODIFIED",
                        status="OPEN",
                        details=f"Modified SL={current_intent.stop_loss:.5f}, TP={current_intent.take_profit:.5f} for ticket #{target_ticket}",
                        source="engine_orchestrator",
                    )

                # Handle MT4 ECN Two-Step Modify if necessary
                if (
                    config.platform_type == 0
                    and config.is_ecn
                    and intent.action_type == ActionType.MARKET_ENTRY
                    and (intent.stop_loss > 0 or intent.take_profit > 0)
                ):
                    await self._execute_mt4_ecn_modify(
                        terminal_id=terminal_id,
                        original_intent=intent,
                        order_ticket=feedback.order_ticket,
                        config=config,
                    )

                final_ticket = feedback.order_ticket if feedback.order_ticket > 0 else (current_intent.order_ticket or intent.order_ticket)
                res = ExecutionResult(
                    terminal_id=terminal_id,
                    signal_id=current_intent.signal_id,
                    success=True,
                    status=feedback.execution_status,
                    ticket=final_ticket,
                    executed_volume=feedback.executed_volume,
                    executed_price=feedback.executed_price,
                    slippage_pips=feedback.slippage_pips,
                    latency_us=feedback.execution_latency_us,
                    attempts=attempt + 1,
                    feedback=feedback,
                    completed_at=time.time(),
                )
                if self.notification_engine and hasattr(self.notification_engine, "notify_trade"):
                    try:
                        asyncio.create_task(
                            self.notification_engine.notify_trade({
                                "signal_id": current_intent.signal_id,
                                "ticket": feedback.order_ticket,
                                "symbol": current_intent.symbol,
                                "side": current_intent.side.name if hasattr(current_intent.side, "name") else str(current_intent.side),
                                "volume": feedback.executed_volume or current_intent.volume,
                                "price": feedback.executed_price or current_intent.price,
                                "terminal_id": terminal_id,
                            })
                        )
                    except Exception as e:
                        logger.debug("Failed to dispatch trade notification: %s", e)
                for cb in getattr(self, "_trade_callbacks", []):
                    try:
                        cb(res)
                    except Exception:
                        pass
                return res

            # Check if rejection is transient and eligible for retry
            is_retryable = self._is_retryable_rejection(feedback)

            if is_retryable and attempt < max_retries:
                backoff = base_backoff_s * (config.backoff_multiplier ** attempt)
                logger.warning(
                    "Order %d rejected by %s (%s, code=%d: %s). "
                    "Retrying attempt (%d/%d) with %.2fs exponential backoff...",
                    current_intent.signal_id,
                    terminal_id,
                    feedback.execution_status.name,
                    feedback.broker_retcode,
                    feedback.error_description,
                    attempt + 1,
                    max_retries,
                    backoff,
                )
                self._record_retry_event(
                    terminal_id=terminal_id,
                    signal_id=current_intent.signal_id,
                    attempt=attempt + 1,
                    max_retries=max_retries,
                    backoff_s=backoff,
                    feedback=feedback,
                )
                await asyncio.sleep(backoff)
                continue
            else:
                # Permanent rejection or retries exhausted
                reason_code = "SKIP_RETRY_EXHAUSTED" if is_retryable else "SKIP_BROKER_REJECTED"
                skip = SkipReason(
                    code=reason_code,
                    reason=(
                        f"Broker rejected order: {feedback.error_description} "
                        f"(retcode={feedback.broker_retcode}, status={feedback.execution_status.name})"
                    ),
                    evaluated_at=time.time(),
                    details={
                        "signal_id": current_intent.signal_id,
                        "terminal_id": terminal_id,
                        "broker_retcode": feedback.broker_retcode,
                        "platform": "MT4" if config.platform_type == 0 else "MT5",
                        "execution_status": feedback.execution_status.name,
                        "error_description": feedback.error_description,
                        "attempts": attempt + 1,
                    },
                )
                return ExecutionResult(
                    terminal_id=terminal_id,
                    signal_id=current_intent.signal_id,
                    success=False,
                    status=feedback.execution_status,
                    ticket=feedback.order_ticket,
                    attempts=attempt + 1,
                    skip_reason=skip,
                    feedback=feedback,
                    completed_at=time.time(),
                )

        # Fallback (safety guard)
        return ExecutionResult(
            terminal_id=terminal_id,
            signal_id=current_intent.signal_id,
            success=False,
            status=ExecutionStatus.REJECTED,
            attempts=max_retries + 1,
            completed_at=time.time(),
        )

    def _is_retryable_rejection(self, feedback: ExecutionFeedback) -> bool:
        """Identify if a broker rejection is transient (requote or busy server)."""
        if feedback.execution_status == ExecutionStatus.REQUOTE:
            return True
        if feedback.broker_retcode in TRANSIENT_RETRYABLE_RETCODES:
            return True

        desc_lower = feedback.error_description.lower()
        return any(kw in desc_lower for kw in TRANSIENT_KEYWORDS)

    async def _execute_mt4_ecn_modify(
        self,
        terminal_id: str,
        original_intent: OrderIntent,
        order_ticket: int,
        config: TerminalExecutionConfig,
    ) -> None:
        """Dispatches an immediate OrderModify intent to attach SL and TP for MT4 ECN fills."""
        modify_intent = OrderIntent(
            symbol=original_intent.symbol,
            side=original_intent.side,
            action_type=ActionType.MODIFY_SL_TP,
            volume=original_intent.volume,
            price=original_intent.price,
            stop_loss=original_intent.stop_loss,
            take_profit=original_intent.take_profit,
            order_ticket=order_ticket,
            signal_id=int(time.time_ns() & 0x7FFFFFFFFFFFFFFF),
            timestamp_ns=time.time_ns(),
        )
        self.ipc_dispatcher.dispatch_order_intent(terminal_id, modify_intent)
        logger.info(
            "Dispatched MT4 ECN OrderModify for ticket #%d (SL=%.5f, TP=%.5f)",
            order_ticket, original_intent.stop_loss, original_intent.take_profit,
        )

    # -------------------------------------------------------------------------
    # Inbound Execution Feedback Dispatcher
    # -------------------------------------------------------------------------

    def _handle_execution_feedback(self, terminal_id: str, feedback: ExecutionFeedback) -> None:
        """Invoked when IPCDispatcher receives MSG_EXECUTION_FEEDBACK over Named Pipe."""
        key = (terminal_id, feedback.signal_id)
        future = self._pending_feedback.get(key)
        if future is not None and not future.done():
            future.set_result(feedback)
            logger.debug(
                "Resolved feedback future for terminal %s signal %d (ticket=%d, status=%s)",
                terminal_id, feedback.signal_id, feedback.order_ticket, feedback.execution_status.name,
            )

    def _record_retry_event(
        self,
        terminal_id: str,
        signal_id: int,
        attempt: int,
        max_retries: int,
        backoff_s: float,
        feedback: ExecutionFeedback,
    ) -> None:
        """Maintain telemetry history of order retry occurrences."""
        self._retry_logs.append({
            "timestamp": time.time(),
            "terminal_id": terminal_id,
            "signal_id": signal_id,
            "attempt": attempt,
            "max_retries": max_retries,
            "backoff_s": backoff_s,
            "retcode": feedback.broker_retcode,
            "status": feedback.execution_status.name,
            "description": feedback.error_description,
        })

    def get_retry_history(self) -> List[Dict[str, Any]]:
        """Return history of all transient retry events."""
        return list(self._retry_logs)

    # -------------------------------------------------------------------------
    # Sprint 14: Position Reconciliation Loop & Manual Close Detection
    # -------------------------------------------------------------------------

    async def reconcile_terminal(
        self,
        terminal_id: str,
        broker_positions: Optional[List[PositionItem]] = None,
        timeout_s: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Audits internal engine state for a single terminal against live broker positions.
        Task 2: Ticket Matching - Compares Ticket ID and Magic Number.
        Task 3: Manual Close Detection - If DB shows OPEN but broker reports it is gone,
                marks trade as CLOSED_EXTERNALLY in Journey log and stops TP/SL modifications.
        """
        # Step 1: Obtain position dump if not explicitly provided
        if broker_positions is None:
            t_out = timeout_s if timeout_s is not None else min(3.0, max(0.01, self.reconciliation_interval_s * 0.4))
            sync_payload = await self.ipc_dispatcher.fetch_positions_dump(terminal_id, timeout_s=t_out)
            if sync_payload is None:
                logger.warning("Positions dump timed out for terminal %s. Skipping audit.", terminal_id)
                open_count = len(self.get_active_orders(terminal_id=terminal_id))
                return {
                    "terminal_id": terminal_id,
                    "audited_orders": 0,
                    "externally_closed": 0,
                    "active_remaining": open_count,
                    "closed_tickets": [],
                    "status": "TIMEOUT",
                }
            broker_positions = sync_payload.positions

        # Step 2: Query database for all orders marked OPEN for this terminal
        open_orders = self.get_active_orders(terminal_id=terminal_id)

        # Step 3: Index broker positions by (ticket, magic_number)
        broker_map = {(p.ticket, p.magic_number): p for p in broker_positions}

        closed_tickets: List[int] = []
        now = time.time()

        # Check 1: Manual External Close Detection
        for order in open_orders:
            ticket = order["ticket"]
            magic = order["magic_number"]
            sig_id = order["signal_id"]
            symbol = order["symbol"]

            # If ticket is 0 or negative, skip (unconfirmed fill)
            if ticket <= 0:
                continue

            # Check if broker position matching (ticket, magic_number) is still present
            if (ticket, magic) not in broker_map:
                # MANUAL CLOSE DETECTED: DB says OPEN, broker reports gone!
                logger.warning(
                    "RECONCILIATION: Ticket #%d (Magic=%d, Symbol=%s, SignalId=%d) on %s "
                    "is OPEN in Engine DB but GONE from broker open positions! Marking CLOSED_EXTERNALLY.",
                    ticket, magic, symbol, sig_id, terminal_id,
                )

                # 1. Update SQLite order_intents
                with self._db_conn:
                    self._db_conn.execute(
                        """
                        UPDATE order_intents
                        SET status = 'CLOSED_EXTERNALLY', updated_at = ?, closed_at = ?
                        WHERE signal_id = ?
                        """,
                        (now, now, sig_id),
                    )

                # 2. Append CLOSED_EXTERNALLY event to Journey log
                self.record_journey_event(
                    signal_id=sig_id,
                    ticket=ticket,
                    magic_number=magic,
                    terminal_id=terminal_id,
                    event_type="CLOSED_EXTERNALLY",
                    status="CLOSED_EXTERNALLY",
                    details=(
                        f"Manual/External close detected: Ticket #{ticket} (Magic: {magic}, "
                        f"Symbol: {symbol}) is no longer reported in broker open positions dump. "
                        f"Marked CLOSED_EXTERNALLY."
                    ),
                    source="reconciliation_loop",
                )

                # 3. Stop attempting to modify its TP/SL in ManagedPosition
                pos = self._managed_positions.get(ticket)
                if pos:
                    pos.closed = True
                    if pos.break_even_config:
                        pos.break_even_config.enabled = False
                    for vtp in pos.virtual_tps:
                        vtp.is_triggered = True
                    logger.info(
                        "Deactivated automated TP/SL management for externally closed ticket #%d",
                        ticket,
                    )

                closed_tickets.append(ticket)
                self._reconciliation_stats["externally_closed_count"] += 1

        # Step 4: Check 2 - Orphan Detection & Resolution (Sprint 14) and Deduplication Indexing (Sprint 16)
        orphans_handled: List[Dict[str, Any]] = []
        for b_pos in broker_positions:
            if b_pos.magic_number == self.default_magic_number:
                # Deduplication indexing: record discovered signal_id from broker position
                from engine.orphan_manager import OrphanManager
                sig_id = OrphanManager.extract_signal_id(b_pos)
                if sig_id is not None:
                    self._discovered_open_signals[sig_id] = b_pos.ticket

                existing_order = self.get_order_by_ticket(b_pos.ticket)
                if self.orphan_manager.is_orphan(b_pos, existing_order):
                    logger.critical(
                        "RECONCILIATION ORPHAN: Terminal %s has orphan position #%d (Magic=%d, Sym=%s, Vol=%.2f)! Applying policy: %s",
                        terminal_id,
                        b_pos.ticket,
                        b_pos.magic_number,
                        b_pos.symbol,
                        b_pos.volume,
                        self.orphan_manager.get_policy(terminal_id).value,
                    )
                    orphan_res = await self.orphan_manager.handle_orphan(
                        terminal_id=terminal_id,
                        position=b_pos,
                    )
                    orphans_handled.append(orphan_res)
                    self._reconciliation_stats["orphans_detected_count"] += 1

        active_remaining = len(open_orders) - len(closed_tickets)
        summary = {
            "terminal_id": terminal_id,
            "audited_orders": len(open_orders),
            "externally_closed": len(closed_tickets),
            "active_remaining": active_remaining,
            "closed_tickets": closed_tickets,
            "orphans_detected": len(orphans_handled),
            "orphans_handled": orphans_handled,
        }
        logger.info(
            "Reconciliation complete for %s: %d audited, %d closed externally, %d active, %d orphans resolved",
            terminal_id,
            len(open_orders),
            len(closed_tickets),
            active_remaining,
            len(orphans_handled),
        )
        return summary

    async def reconcile_all_terminals(self) -> Dict[str, Any]:
        """
        Executes a complete reconciliation audit cycle across all active terminals.
        """
        registered = self.ipc_dispatcher.get_registered_terminals()
        summaries: Dict[str, Any] = {}

        for tid in registered:
            ctx = getattr(self.ipc_dispatcher, "_terminals", {}).get(tid)
            # Only audit connected terminals with confirmed broker link
            if ctx and ctx.status == TerminalStatus.CONNECTED:
                try:
                    res = await self.reconcile_terminal(tid)
                    summaries[tid] = res
                except Exception as e:
                    logger.error("Error reconciling terminal %s: %s", tid, e, exc_info=True)

        self._reconciliation_stats["total_cycles"] += 1
        self._reconciliation_stats["last_run_timestamp"] = time.time()
        self._reconciliation_stats["last_reconciliation_summary"] = summaries
        return summaries

    async def reconciliation_loop(self) -> None:
        """
        Asynchronous 30-second reconciliation audit loop (Sprint 14).
        Audits internal engine state against live broker positions across all
        connected terminals every 30 seconds.
        """
        logger.info(
            "Started 30-second reconciliation audit loop (interval=%.1fs)",
            self.reconciliation_interval_s,
        )
        while self._is_running:
            try:
                await self.reconcile_all_terminals()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Unexpected error in reconciliation loop: %s", e, exc_info=True)

            try:
                await asyncio.sleep(self.reconciliation_interval_s)
            except asyncio.CancelledError:
                break

    # Alias matching PascalCase deliverable directive specification
    ReconciliationLoop = reconciliation_loop

    def set_orphan_policy(
        self,
        policy: Union[OrphanResolutionPolicy, str],
        terminal_id: Optional[str] = None,
    ) -> None:
        """Configures orphan resolution policy (IGNORE, ADOPT, CLOSE_IMMEDIATELY)."""
        self.orphan_manager.set_policy(policy, terminal_id=terminal_id)

    def get_reconciliation_stats(self) -> Dict[str, Any]:
        """Returns statistics and latest audit metrics from the reconciliation loop."""
        return dict(self._reconciliation_stats)

    async def panic_close_all(
        self,
        reason: str = "EMERGENCY_MOBILE_PANIC",
        terminal_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Emergency liquidation: Immediately closes all active positions across registered terminals.
        Can be triggered locally or via RemoteCommandReceiver from Mobile Companion App.
        """
        now = time.time()
        liquidated: List[Dict[str, Any]] = []

        active_orders = self.get_active_orders(terminal_id=terminal_id)
        for order in active_orders:
            ticket = int(order.get("ticket") or 0)
            sig_id = int(order.get("signal_id") or 0)
            t_id = str(order.get("terminal_id") or terminal_id or "terminal_default")
            sym = str(order.get("symbol") or "EURUSD")
            vol = float(order.get("volume") or 0.10)
            magic = int(order.get("magic_number") or self.default_magic_number)

            close_intent = OrderIntent(
                symbol=sym,
                side=OrderSide.BUY,
                action_type=ActionType.FULL_CLOSE,
                volume=vol,
                magic_number=magic,
                order_ticket=ticket,
                flags=int(ExecutionFlags.IOC),
            )

            if self.ipc_dispatcher and hasattr(self.ipc_dispatcher, "dispatch_order_intent"):
                try:
                    await self.ipc_dispatcher.dispatch_order_intent(t_id, close_intent)
                except Exception as e:
                    logger.warning("Error dispatching panic close to %s for ticket %d: %s", t_id, ticket, e)

            with self._db_conn:
                self._db_conn.execute(
                    "UPDATE order_intents SET status = 'CLOSED', updated_at = ?, closed_at = ? WHERE ticket = ?",
                    (now, now, ticket),
                )

            self.record_journey_event(
                signal_id=sig_id,
                ticket=ticket,
                magic_number=magic,
                terminal_id=t_id,
                event_type="EMERGENCY_PANIC_CLOSE",
                status="CLOSED",
                details=f"Panic close executed ({reason})",
                source="panic_close_all",
            )

            pos = self._managed_positions.get(ticket)
            if pos:
                pos.closed = True

            liquidated.append({
                "ticket": ticket,
                "signal_id": sig_id,
                "terminal_id": t_id,
                "symbol": sym,
                "volume": vol,
                "status": "CLOSED",
            })

        logger.critical(
            "PANIC_CLOSE_ALL completed (%s): %d positions liquidated.",
            reason, len(liquidated),
        )
        return liquidated

    # -------------------------------------------------------------------------
    # Sprint 17: Remote Manual Override Functions (Mobile Command Executor)
    # -------------------------------------------------------------------------

    def register_channel_mapping(self, channel_id: int, channel_name: str) -> None:
        """Registers a mapping between an integer channel ID and a friendly channel name."""
        self._channel_name_to_id[channel_name.lower().strip()] = int(channel_id)
        self._channel_id_to_name[int(channel_id)] = channel_name.strip()

    def pause_trading(self) -> bool:
        """Pauses automated trade execution (manual overrides remain enabled)."""
        self._is_paused = True
        logger.warning("TRADING PAUSED by remote mobile command.")
        return True

    def resume_trading(self) -> bool:
        """Resumes automated trade execution."""
        self._is_paused = False
        logger.info("TRADING RESUMED by remote mobile command.")
        return True

    def is_paused(self) -> bool:
        """Checks if trading is currently paused."""
        return self._is_paused

    async def manual_close_all(
        self,
        channel: Optional[Union[str, int]] = None,
        symbol: Optional[str] = None,
        terminal_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Sprint 17 Directive: Closes active positions matching optional channel,
        symbol, or terminal filters. If no filter is specified, liquidates all.
        """
        now = time.time()
        liquidated: List[Dict[str, Any]] = []

        # Determine target channel ID if channel is provided
        target_channel_id: Optional[int] = None
        if channel is not None:
            if isinstance(channel, int):
                target_channel_id = abs(channel) & 0x7FFFFFFF
            elif isinstance(channel, str):
                ch_clean = channel.strip()
                if ch_clean.lstrip("-").isdigit():
                    target_channel_id = abs(int(ch_clean)) & 0x7FFFFFFF
                else:
                    mapped = self._channel_name_to_id.get(ch_clean.lower())
                    if mapped is not None:
                        target_channel_id = abs(mapped) & 0x7FFFFFFF

        active_orders = self.get_active_orders(terminal_id=terminal_id)
        for order in active_orders:
            ticket = int(order.get("ticket") or 0)
            sig_id = int(order.get("signal_id") or 0)
            t_id = str(order.get("terminal_id") or terminal_id or "terminal_default")
            sym = str(order.get("symbol") or "EURUSD")
            vol = float(order.get("volume") or 0.10)
            side = int(order.get("side") or 0)
            magic = int(order.get("magic_number") or self.default_magic_number)

            # Symbol filter
            if symbol and sym.upper() != symbol.strip().upper():
                continue

            # Channel filter
            if channel is not None:
                order_cid = (sig_id >> 32) & 0x7FFFFFFF
                if target_channel_id is not None:
                    if order_cid != target_channel_id:
                        continue
                else:
                    # String channel could not be mapped to an ID, skip
                    continue

            close_side = OrderSide.SELL if side == int(OrderSide.BUY) else OrderSide.BUY

            close_intent = OrderIntent(
                symbol=sym,
                side=close_side,
                action_type=ActionType.FULL_CLOSE,
                volume=vol,
                magic_number=magic,
                order_ticket=ticket,
                flags=int(ExecutionFlags.IOC),
            )

            res = None
            if self.ipc_dispatcher and hasattr(self.ipc_dispatcher, "dispatch_order_intent"):
                try:
                    res = await self.ipc_dispatcher.dispatch_order_intent(t_id, close_intent)
                except Exception as e:
                    logger.warning("Error dispatching manual close for ticket %d: %s", ticket, e)

            with self._db_conn:
                self._db_conn.execute(
                    "UPDATE order_intents SET status = 'CLOSED', updated_at = ?, closed_at = ? WHERE ticket = ?",
                    (now, now, ticket),
                )

            self.record_journey_event(
                signal_id=sig_id,
                ticket=ticket,
                magic_number=magic,
                terminal_id=t_id,
                event_type="MANUAL_OVERRIDE_CLOSE",
                status="CLOSED",
                details=f"Manual close executed (channel={channel}, symbol={symbol})",
                source="manual_close_all",
            )

            pos = self._managed_positions.get(ticket)
            if pos:
                pos.closed = True

            liquidated.append({
                "ticket": ticket,
                "signal_id": sig_id,
                "terminal_id": t_id,
                "symbol": sym,
                "volume": vol,
                "success": getattr(res, "success", True) if res is not None else True,
                "status": "CLOSED",
            })

        logger.info(
            "MANUAL_CLOSE_ALL completed: %d matching positions closed (channel=%s, symbol=%s).",
            len(liquidated), channel, symbol,
        )
        return liquidated

    async def manual_close_position(
        self,
        ticket: int,
        terminal_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Closes a single specific position ticket."""
        now = time.time()
        order = None
        active_orders = self.get_active_orders(terminal_id=terminal_id)
        for o in active_orders:
            if int(o.get("ticket") or 0) == ticket:
                order = o
                break

        if not order:
            logger.warning("Position #%d not found among active orders.", ticket)
            return None

        sig_id = int(order.get("signal_id") or 0)
        t_id = str(order.get("terminal_id") or terminal_id or "terminal_default")
        sym = str(order.get("symbol") or "EURUSD")
        vol = float(order.get("volume") or 0.10)
        side = int(order.get("side") or 0)
        magic = int(order.get("magic_number") or self.default_magic_number)

        close_side = OrderSide.SELL if side == int(OrderSide.BUY) else OrderSide.BUY
        close_intent = OrderIntent(
            symbol=sym,
            side=close_side,
            action_type=ActionType.FULL_CLOSE,
            volume=vol,
            magic_number=magic,
            order_ticket=ticket,
            flags=int(ExecutionFlags.IOC),
        )

        res = None
        if self.ipc_dispatcher and hasattr(self.ipc_dispatcher, "dispatch_order_intent"):
            try:
                res = await self.ipc_dispatcher.dispatch_order_intent(t_id, close_intent)
            except Exception as e:
                logger.warning("Error dispatching manual close for ticket %d: %s", ticket, e)

        with self._db_conn:
            self._db_conn.execute(
                "UPDATE order_intents SET status = 'CLOSED', updated_at = ?, closed_at = ? WHERE ticket = ?",
                (now, now, ticket),
            )

        self.record_journey_event(
            signal_id=sig_id,
            ticket=ticket,
            magic_number=magic,
            terminal_id=t_id,
            event_type="MANUAL_OVERRIDE_CLOSE",
            status="CLOSED",
            details=f"Single ticket #{ticket} closed via remote manual override",
            source="manual_close_position",
        )

        pos = self._managed_positions.get(ticket)
        if pos:
            pos.closed = True

        return {
            "ticket": ticket,
            "signal_id": sig_id,
            "terminal_id": t_id,
            "symbol": sym,
            "volume": vol,
            "success": getattr(res, "success", True) if res is not None else True,
            "status": "CLOSED",
        }

    async def manual_close_symbol(
        self,
        symbol: str,
        terminal_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Closes all active positions matching the given symbol."""
        return await self.manual_close_all(symbol=symbol, terminal_id=terminal_id)

    async def manual_modify_position(
        self,
        ticket: int,
        stop_loss: Optional[float] = None,
        take_profit: Optional[float] = None,
        terminal_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Modifies SL and/or TP for an active position."""
        now = time.time()
        order = None
        active_orders = self.get_active_orders(terminal_id=terminal_id)
        for o in active_orders:
            if int(o.get("ticket") or 0) == ticket:
                order = o
                break

        if not order:
            logger.warning("Position #%d not found for SL/TP modification.", ticket)
            return None

        t_id = str(order.get("terminal_id") or terminal_id or "terminal_default")
        sym = str(order.get("symbol") or "EURUSD")
        magic = int(order.get("magic_number") or self.default_magic_number)
        sig_id = int(order.get("signal_id") or 0)

        new_sl = stop_loss if stop_loss is not None else float(order.get("stop_loss") or 0.0)
        new_tp = take_profit if take_profit is not None else float(order.get("take_profit") or 0.0)

        mod_intent = OrderIntent(
            symbol=sym,
            action_type=ActionType.MODIFY_SL_TP,
            order_ticket=ticket,
            stop_loss=new_sl,
            take_profit=new_tp,
            magic_number=magic,
        )

        res = None
        if self.ipc_dispatcher and hasattr(self.ipc_dispatcher, "dispatch_order_intent"):
            try:
                res = await self.ipc_dispatcher.dispatch_order_intent(t_id, mod_intent)
            except Exception as e:
                logger.warning("Error dispatching SL/TP modification for ticket %d: %s", ticket, e)

        with self._db_conn:
            self._db_conn.execute(
                "UPDATE order_intents SET stop_loss = ?, take_profit = ?, updated_at = ? WHERE ticket = ?",
                (new_sl, new_tp, now, ticket),
            )

        self.record_journey_event(
            signal_id=sig_id,
            ticket=ticket,
            magic_number=magic,
            terminal_id=t_id,
            event_type="MANUAL_OVERRIDE_MODIFY",
            status="OPEN",
            details=f"Modified SL={new_sl}, TP={new_tp}",
            source="manual_modify_position",
        )

        pos = self._managed_positions.get(ticket)
        if pos:
            pos.current_sl = new_sl
            pos.current_tp = new_tp

        return {
            "ticket": ticket,
            "stop_loss": new_sl,
            "take_profit": new_tp,
            "success": getattr(res, "success", True) if res is not None else True,
        }

    # -------------------------------------------------------------------------
    # Lifecycle Management
    # -------------------------------------------------------------------------

    async def start(self) -> None:
        """Starts orchestrator workers, reconciliation loop, and initializes state."""
        if self._is_running:
            return
        self._is_running = True

        # Start 30-second reconciliation audit loop
        if self._reconciliation_task is None or self._reconciliation_task.done():
            self._reconciliation_task = asyncio.create_task(
                self.reconciliation_loop(),
                name="ReconciliationLoop",
            )

        logger.info("ExecutionOrchestrator started with 30s reconciliation loop.")

    async def stop(self) -> None:
        """Gracefully cancels all per-account workers, reconciliation loop, and cleans pending queues."""
        self._is_running = False

        if self._reconciliation_task and not self._reconciliation_task.done():
            self._reconciliation_task.cancel()
            try:
                await self._reconciliation_task
            except asyncio.CancelledError:
                pass
            self._reconciliation_task = None

        for tid, task in list(self._worker_tasks.items()):
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._worker_tasks.clear()
        self._account_queues.clear()
        self._pending_feedback.clear()
        self._managed_positions.clear()
        self._position_groups.clear()
        if hasattr(self, "_db_executor") and self._db_executor:
            self._db_executor.shutdown(wait=False)
            self._db_executor = None
        if hasattr(self, "_db_conn") and self._db_conn:
            with self._db_lock:
                try:
                    self._db_conn.close()
                except Exception:
                    pass
                self._db_conn = None
        logger.info("ExecutionOrchestrator stopped.")

    def close(self) -> None:
        """Cleanly releases storage and worker threads (synchronous interface)."""
        if hasattr(self, "_db_executor") and self._db_executor:
            self._db_executor.shutdown(wait=False)
            self._db_executor = None
        if hasattr(self, "_db_conn") and self._db_conn:
            with self._db_lock:
                try:
                    self._db_conn.close()
                except Exception:
                    pass
                self._db_conn = None
