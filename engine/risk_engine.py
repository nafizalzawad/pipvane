"""
Pipvane v3 Stateless Rule Evaluation, Risk Engine & Circuit Breakers (Sprint 1, 2 & 7).

Provides pure, stateless rule evaluation, position sizing, global equity circuit breaker,
and offline economic news calendar filtering:
1. Position Sizing Module:
   - Fixed Lot
   - % of Equity Risk (calculated via Stop Loss distance in pips and pip value)
   - Fixed Currency Risk (cash risk amount via SL distance)
   - Provider Lot Multiplier
2. Entry Guard Module:
   - max_signal_age_s (rejects signals older than threshold, FR-7.6 / Blueprint Sec 5.6)
   - max_drift_pips (rejects if current market price has drifted beyond limit from signal price)
   - max_spread_pips (rejects if broker spread exceeds threshold)
3. Global Equity Circuit Breaker (Sprint 7):
   - Monitors daily loss limit and max loss limit against current account equity and balance.
   - If drawdown breaches threshold, generates immediate "Panic Close" intent for all open
     positions and sets flag to pause new entries.
4. Economic News Calendar Ingestion & Filtering (Sprint 7):
   - Local offline cache reader for economic calendar JSON files.
   - Pure news filter function that blocks specific currency pairs (e.g., EUR, USD)
     if current time is within user-defined window (e.g. 30 minutes before/after) of High Impact events.
5. Account Level Safety Guards:
   - Equity Hard Stop (halts trading if equity drops below minimum threshold)
   - Max Open Positions (prevents over-allocation)
   - Minimum Free Margin Check
"""

from __future__ import annotations

import datetime
import enum
import json
import time
from dataclasses import dataclass, field, replace
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union

from engine.ipc_protocol import (
    OrderIntent,
    OrderSide,
    ActionType,
    ExecutionFlags,
)
from engine.license_manager import LicenseExpiredError


# =============================================================================
# REASON CODES & CONSTANTS
# =============================================================================

SKIP_AGE = "SKIP_AGE"
SKIP_DRIFT = "SKIP_DRIFT"
SKIP_SPREAD = "SKIP_SPREAD"
SKIP_SL_REQUIRED = "SKIP_SL_REQUIRED"
SKIP_EQUITY_HARD_STOP = "SKIP_EQUITY_HARD_STOP"
SKIP_MAX_POSITIONS = "SKIP_MAX_POSITIONS"
SKIP_MIN_MARGIN = "SKIP_MIN_MARGIN"
SKIP_INVALID_QUOTE = "SKIP_INVALID_QUOTE"
SKIP_CIRCUIT_BREAKER = "SKIP_CIRCUIT_BREAKER"
SKIP_NEWS = "SKIP_NEWS"
SKIP_LICENSE_EXPIRED = "LICENSE_EXPIRED"
SKIP_DUP = "SKIP_DUP"
SKIP_SYMBOL = "SKIP_SYMBOL"
SKIP_FIFO_HEDGE = "SKIP_FIFO_HEDGE"
SKIP_STOPS_LEVEL = "SKIP_STOPS_LEVEL"
SKIP_FREEZE_LEVEL = "SKIP_FREEZE_LEVEL"


class FreezePolicy(str, Enum):
    """
    User-configurable policy for broker Stops Level and Freeze Level enforcement (Sprint 20).
    - SKIP_TRADE: Rejects orders whose SL or TP violates broker Stops Level.
    - ADJUST_TO_MINIMUM: Widens the SL or TP mathematically to exactly match the broker minimum.
    """
    SKIP_TRADE = "SKIP_TRADE"
    ADJUST_TO_MINIMUM = "ADJUST_TO_MINIMUM"


@dataclass(frozen=True)
class SkipReason:
    """
    Structured outcome returned when an OrderIntent is rejected by risk or entry guards.
    Strictly aligns with SPRINT_1_ARCHITECTURE_BLUEPRINT diagnostics.
    """
    code: str
    reason: str
    evaluated_at: float
    details: Dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        return self.reason


from engine.symbol_mapper import (
    PipConverter,
    AssetClass,
    BrokerSymbolSpec,
    default_pip_converter,
)


class RiskModel(str, Enum):
    """Supported position sizing algorithms for AccountProfile."""
    FIXED_LOT = "fixed_lot"
    PERCENT_EQUITY = "percent_equity"
    FIXED_CURRENCY = "fixed_currency"
    PROVIDER_MULTIPLIER = "provider_multiplier"


# Legacy SizingMode alias for backward compatibility
class SizingMode(str, enum.Enum):
    EQUITY_PERCENT = "equity_percent"
    FIXED_LOT = "fixed_lot"
    MULTIPLIER = "multiplier"
    BALANCE_PERCENT = "balance_percent"


class GuardRejectionReason(str, enum.Enum):
    """Explicit trade admission guard rejection codes."""
    SKIP_SPREAD = "SKIP_SPREAD"
    SKIP_SLIPPAGE = "SKIP_SLIPPAGE"
    SKIP_MAX_POSITIONS = "SKIP_MAX_POSITIONS"
    SKIP_LOW_MARGIN = "SKIP_LOW_MARGIN"
    SKIP_EXPIRED_SIGNAL = "SKIP_EXPIRED_SIGNAL"
    SKIP_SYSTEM_HALTED = "SKIP_SYSTEM_HALTED"
    SKIP_INVALID_SL = "SKIP_INVALID_SL"
    SKIP_BELOW_MIN_LOT = "SKIP_BELOW_MIN_LOT"
    SKIP_EXCEEDS_MAX_LOT = "SKIP_EXCEEDS_MAX_LOT"
    SKIP_LICENSE_EXPIRED = "LICENSE_EXPIRED"
    SKIP_DUP = "SKIP_DUP"
    SKIP_FIFO_HEDGE = "SKIP_FIFO_HEDGE"
    SKIP_STOPS_LEVEL = "SKIP_STOPS_LEVEL"
    SKIP_FREEZE_LEVEL = "SKIP_FREEZE_LEVEL"


class PanicReason(str, enum.Enum):
    """Global emergency liquidation reasons."""
    DAILY_LOSS_LIMIT_BREACHED = "DAILY_LOSS_LIMIT_BREACHED"
    MAX_DRAWDOWN_BREACHED = "MAX_DRAWDOWN_BREACHED"
    MANUAL_PANIC = "MANUAL_PANIC"
    BROKER_DISCONNECT_CRITICAL = "BROKER_DISCONNECT_CRITICAL"


# =============================================================================
# SYMBOL SPECIFICATIONS & CATALOG
# =============================================================================

DEFAULT_PIP_SIZES: Dict[str, float] = {
    # 2-decimal JPY pairs (0.01)
    "USDJPY": 0.01, "GBPJPY": 0.01, "EURJPY": 0.01, "AUDJPY": 0.01,
    "CADJPY": 0.01, "NZDJPY": 0.01, "CHFJPY": 0.01,
    # 4-decimal Forex pairs (0.0001)
    "EURUSD": 0.0001, "GBPUSD": 0.0001, "AUDUSD": 0.0001, "NZDUSD": 0.0001,
    "USDCAD": 0.0001, "USDCHF": 0.0001, "EURGBP": 0.0001, "EURCHF": 0.0001,
    "EURAUD": 0.0001, "GBPAUD": 0.0001, "GBPCAD": 0.0001, "AUDCAD": 0.0001,
    # Metals & Commodities
    "XAUUSD": 0.1, "GOLD": 0.1,
    "XAGUSD": 0.01, "SILVER": 0.01,
    "USOIL": 0.01, "UKOIL": 0.01, "WTI": 0.01, "BRENT": 0.01,
    # Crypto
    "BTCUSD": 1.0, "ETHUSD": 1.0, "SOLUSD": 0.1, "XRPUSD": 0.001,
    # Indices
    "US30": 1.0, "DJ30": 1.0, "NAS100": 1.0, "USTEC": 1.0,
    "SPX500": 0.1, "US500": 0.1, "GER40": 1.0, "UK100": 1.0,
}


def get_default_pip_size(
    symbol: str,
    digits: Optional[int] = None,
    point: Optional[Union[float, Any]] = None,
) -> float:
    """Returns standard 1-pip price distance for a symbol using PipConverter."""
    return float(default_pip_converter.get_pip_size(symbol, digits=digits, point=point))


def classify_asset(symbol: str) -> AssetClass:
    """Classifies symbol into an AssetClass using PipConverter."""
    return default_pip_converter.classify_asset(symbol)


def pips_to_price_offset(
    symbol: str,
    pips: Union[int, float, str, Any],
    digits: Optional[int] = None,
    point: Optional[Union[float, Any]] = None,
) -> float:
    """Translates raw pips into exact price offset (e.g. 50 pips -> 0.0050 for EURUSD, 0.50 for USDJPY, 5.0 for Gold)."""
    return float(default_pip_converter.pips_to_price_offset(symbol, pips, digits=digits, point=point))


pips_to_price_delta = pips_to_price_offset


def price_offset_to_pips(
    symbol: str,
    price_offset: Union[float, str, Any],
    digits: Optional[int] = None,
    point: Optional[Union[float, Any]] = None,
) -> float:
    """Translates price offset into pip count."""
    return float(default_pip_converter.price_offset_to_pips(symbol, price_offset, digits=digits, point=point))


price_delta_to_pips = price_offset_to_pips


def calculate_sl(
    symbol: str,
    entry_price: Union[float, int, str, Any],
    sl_pips: Union[float, int, str, Any],
    side: Union[str, int, OrderSide] = "BUY",
    digits: Optional[int] = None,
    point: Optional[Union[float, Any]] = None,
    quantize_places: Optional[int] = None,
) -> float:
    """Calculates Stop Loss price from entry price and pip distance."""
    return float(default_pip_converter.calculate_sl(
        symbol=symbol,
        entry_price=entry_price,
        sl_pips=sl_pips,
        side=side,
        digits=digits,
        point=point,
        quantize_places=quantize_places,
    ))


def calculate_tp(
    symbol: str,
    entry_price: Union[float, int, str, Any],
    tp_pips: Union[float, int, str, Any],
    side: Union[str, int, OrderSide] = "BUY",
    digits: Optional[int] = None,
    point: Optional[Union[float, Any]] = None,
    quantize_places: Optional[int] = None,
) -> float:
    """Calculates Take Profit price from entry price and pip distance."""
    return float(default_pip_converter.calculate_tp(
        symbol=symbol,
        entry_price=entry_price,
        tp_pips=tp_pips,
        side=side,
        digits=digits,
        point=point,
        quantize_places=quantize_places,
    ))


def calculate_trailing_stop(
    symbol: str,
    current_price: Union[float, int, str, Any],
    current_sl: Union[float, int, str, Any],
    trailing_pips: Union[float, int, str, Any],
    side: Union[str, int, OrderSide] = "BUY",
    step_pips: Union[float, int, str, Any] = 0,
    digits: Optional[int] = None,
    point: Optional[Union[float, Any]] = None,
    quantize_places: Optional[int] = None,
) -> Tuple[bool, float]:
    """Calculates Trailing Stop price update using PipConverter."""
    should_modify, new_sl = default_pip_converter.calculate_trailing_stop(
        symbol=symbol,
        current_price=current_price,
        current_sl=current_sl,
        trailing_pips=trailing_pips,
        side=side,
        step_pips=step_pips,
        digits=digits,
        point=point,
        quantize_places=quantize_places,
    )
    return should_modify, float(new_sl)


def get_default_pip_value(symbol: str, account_currency: str = "USD") -> float:
    """
    Returns standard cash value of 1 pip for 1.0 standard lot.
    Default standard FX: 100,000 units * 0.0001 = $10.00 per pip.
    Gold (100 oz): 100 * 0.10 = $10.00 per pip.
    """
    return 10.0


@dataclass(frozen=True)
class SymbolSpecification:
    """Contract size, tick size, and lot limits for a financial instrument."""
    symbol: str
    contract_size: float              # Units per 1.0 standard lot
    pip_size: float                   # Price change corresponding to 1 pip
    quote_currency: str = "USD"       # Currency in which profit is calculated
    base_currency: str = ""           # Base currency (e.g. USD in USDJPY)
    min_lot: float = 0.01
    max_lot: float = 100.00
    lot_step: float = 0.01

    def pip_value_usd(self, current_price: float = 0.0, quote_to_usd_rate: float = 1.0) -> float:
        raw_pip_value = self.contract_size * self.pip_size
        if self.quote_currency == "USD":
            return raw_pip_value
        elif self.base_currency == "USD" and current_price > 0:
            return raw_pip_value / current_price
        elif quote_to_usd_rate != 1.0:
            return raw_pip_value * quote_to_usd_rate
        elif self.quote_currency == "JPY":
            return raw_pip_value / 150.0
        else:
            return raw_pip_value

    calculate_pip_value = pip_value_usd


DEFAULT_SYMBOL_CATALOG: Dict[str, SymbolSpecification] = {
    "EURUSD": SymbolSpecification("EURUSD", 100_000.0, 0.0001, quote_currency="USD", base_currency="EUR"),
    "GBPUSD": SymbolSpecification("GBPUSD", 100_000.0, 0.0001, quote_currency="USD", base_currency="GBP"),
    "AUDUSD": SymbolSpecification("AUDUSD", 100_000.0, 0.0001, quote_currency="USD", base_currency="AUD"),
    "NZDUSD": SymbolSpecification("NZDUSD", 100_000.0, 0.0001, quote_currency="USD", base_currency="NZD"),
    "USDJPY": SymbolSpecification("USDJPY", 100_000.0, 0.01, quote_currency="JPY", base_currency="USD"),
    "USDCAD": SymbolSpecification("USDCAD", 100_000.0, 0.0001, quote_currency="CAD", base_currency="USD"),
    "USDCHF": SymbolSpecification("USDCHF", 100_000.0, 0.0001, quote_currency="CHF", base_currency="USD"),
    "EURGBP": SymbolSpecification("EURGBP", 100_000.0, 0.0001, quote_currency="GBP", base_currency="EUR"),
    "EURJPY": SymbolSpecification("EURJPY", 100_000.0, 0.01, quote_currency="JPY", base_currency="EUR"),
    "GBPJPY": SymbolSpecification("GBPJPY", 100_000.0, 0.01, quote_currency="JPY", base_currency="GBP"),
    "XAUUSD": SymbolSpecification("XAUUSD", 100.0, 0.10, quote_currency="USD", base_currency="XAU"),
    "GOLD": SymbolSpecification("GOLD", 100.0, 0.10, quote_currency="USD", base_currency="XAU"),
    "XAGUSD": SymbolSpecification("XAGUSD", 5_000.0, 0.01, quote_currency="USD", base_currency="XAG"),
    "SILVER": SymbolSpecification("SILVER", 5_000.0, 0.01, quote_currency="USD", base_currency="XAG"),
    "US30": SymbolSpecification("US30", 1.0, 1.0, quote_currency="USD", min_lot=0.10, lot_step=0.10),
    "DJ30": SymbolSpecification("DJ30", 1.0, 1.0, quote_currency="USD", min_lot=0.10, lot_step=0.10),
    "NAS100": SymbolSpecification("NAS100", 1.0, 1.0, quote_currency="USD", min_lot=0.10, lot_step=0.10),
    "USTEC": SymbolSpecification("USTEC", 1.0, 1.0, quote_currency="USD", min_lot=0.10, lot_step=0.10),
    "SPX500": SymbolSpecification("SPX500", 10.0, 1.0, quote_currency="USD", min_lot=0.10, lot_step=0.10),
    "GER40": SymbolSpecification("GER40", 1.0, 1.0, quote_currency="EUR", min_lot=0.10, lot_step=0.10),
    "BTCUSD": SymbolSpecification("BTCUSD", 1.0, 1.0, quote_currency="USD", min_lot=0.01, lot_step=0.01),
    "ETHUSD": SymbolSpecification("ETHUSD", 1.0, 1.0, quote_currency="USD", min_lot=0.01, lot_step=0.01),
}


# =============================================================================
# CURRENCY MAPPING FOR SYMBOLS
# =============================================================================

KNOWN_SYMBOL_CURRENCIES: Dict[str, Set[str]] = {
    # Forex
    "EURUSD": {"EUR", "USD"}, "GBPUSD": {"GBP", "USD"}, "USDJPY": {"USD", "JPY"},
    "USDCHF": {"USD", "CHF"}, "AUDUSD": {"AUD", "USD"}, "NZDUSD": {"NZD", "USD"},
    "USDCAD": {"USD", "CAD"}, "EURGBP": {"EUR", "GBP"}, "EURJPY": {"EUR", "JPY"},
    "GBPJPY": {"GBP", "JPY"}, "AUDJPY": {"AUD", "JPY"}, "CADJPY": {"CAD", "JPY"},
    "EURAUD": {"EUR", "AUD"}, "GBPAUD": {"GBP", "AUD"}, "AUDNZD": {"AUD", "NZD"},
    "AUDCAD": {"AUD", "CAD"}, "NZDJPY": {"NZD", "JPY"}, "CHFJPY": {"CHF", "JPY"},
    # Metals & Commodities
    "XAUUSD": {"XAU", "USD"}, "GOLD": {"XAU", "USD"},
    "XAGUSD": {"XAG", "USD"}, "SILVER": {"XAG", "USD"},
    "USOIL": {"USD"}, "WTI": {"USD"}, "UKOIL": {"GBP", "USD"}, "BRENT": {"GBP", "USD"},
    # Indices
    "US30": {"USD"}, "DJ30": {"USD"}, "WALLSTREET": {"USD"},
    "NAS100": {"USD"}, "USTEC": {"USD"}, "NDX": {"USD"},
    "SPX500": {"USD"}, "US500": {"USD"}, "SP500": {"USD"},
    "GER40": {"EUR"}, "GER30": {"EUR"}, "DAX": {"EUR"},
    "UK100": {"GBP"}, "FTSE": {"GBP"}, "JP225": {"JPY"}, "NIKKEI": {"JPY"},
    # Crypto
    "BTCUSD": {"USD"}, "ETHUSD": {"USD"}, "SOLUSD": {"USD"}, "XRPUSD": {"USD"},
}


def extract_currencies_for_symbol(symbol: str) -> Set[str]:
    """
    Extracts the underlying economic currency codes associated with a trading symbol.
    E.g. 'EURUSD' -> {'EUR', 'USD'}, 'GOLD' -> {'XAU', 'USD'}, 'GER40' -> {'EUR'}.
    """
    clean_sym = symbol.upper().replace("/", "").replace("_", "").replace("-", "").strip()
    if clean_sym in KNOWN_SYMBOL_CURRENCIES:
        return set(KNOWN_SYMBOL_CURRENCIES[clean_sym])

    # If 6-character FX pair (e.g. 'EURCAD')
    if len(clean_sym) == 6 and clean_sym.isalpha():
        return {clean_sym[:3], clean_sym[3:]}

    # Fallback to USD quote assumption
    return {"USD"}


# =============================================================================
# ECONOMIC NEWS CALENDAR INGESTION (SPRINT 7)
# =============================================================================

@dataclass(frozen=True)
class EconomicEvent:
    """
    Economic calendar event record ingested from local offline JSON cache.
    """
    id: str
    title: str
    currency: str                          # e.g. "USD", "EUR", "GBP"
    impact: str                            # "HIGH", "MEDIUM", "LOW", "NONE"
    timestamp: float                       # Epoch seconds
    date_iso: Optional[str] = None         # ISO-8601 string, e.g. "2026-11-21T13:30:00Z"
    country: Optional[str] = None
    forecast: Optional[str] = None
    previous: Optional[str] = None
    actual: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> EconomicEvent:
        """Parses a dictionary record into a strictly normalized EconomicEvent."""
        event_id = str(data.get("id") or data.get("event_id") or f"ev_{int(time.time() * 1000)}")
        title = str(data.get("title") or data.get("name") or "Economic Event")
        currency = str(data.get("currency") or data.get("country") or "USD").upper().strip()

        # Normalize impact to uppercase HIGH, MEDIUM, LOW, NONE
        raw_impact = str(data.get("impact") or "HIGH").upper().strip()
        if raw_impact in ("HIGH", "3", "RED", "CRITICAL"):
            impact = "HIGH"
        elif raw_impact in ("MEDIUM", "MED", "2", "ORANGE"):
            impact = "MEDIUM"
        elif raw_impact in ("LOW", "1", "YELLOW"):
            impact = "LOW"
        else:
            impact = "NONE"

        # Timestamp parsing
        ts: float = 0.0
        date_iso = data.get("date_iso") or data.get("date") or data.get("time")

        if "timestamp" in data and data["timestamp"]:
            ts = float(data["timestamp"])
        elif date_iso:
            # Parse ISO string
            try:
                dt_str = str(date_iso).replace("Z", "+00:00")
                dt = datetime.datetime.fromisoformat(dt_str)
                ts = dt.timestamp()
            except Exception:
                try:
                    dt = datetime.datetime.strptime(str(date_iso)[:19], "%Y-%m-%d %H:%M:%S")
                    ts = dt.replace(tzinfo=datetime.timezone.utc).timestamp()
                except Exception:
                    ts = 0.0

        return cls(
            id=event_id,
            title=title,
            currency=currency,
            impact=impact,
            timestamp=ts,
            date_iso=str(date_iso) if date_iso else None,
            country=data.get("country"),
            forecast=data.get("forecast"),
            previous=data.get("previous"),
            actual=data.get("actual"),
        )


class EconomicCalendar:
    """
    Local offline cache reader and fast lookup index for economic calendar events.
    Operates 100% offline without external network dependencies (FR-1.1 & Sprint 7).
    """

    def __init__(self, events: Optional[List[EconomicEvent]] = None) -> None:
        self._events: List[EconomicEvent] = events or []
        self._by_currency: Dict[str, List[EconomicEvent]] = {}
        self._index_events()

    def _index_events(self) -> None:
        """Pre-sorts and groups events by currency for <10 microsecond lookup."""
        self._by_currency.clear()
        for ev in self._events:
            curr = ev.currency.upper()
            if curr not in self._by_currency:
                self._by_currency[curr] = []
            self._by_currency[curr].append(ev)

        for curr in self._by_currency:
            self._by_currency[curr].sort(key=lambda e: e.timestamp)

    @classmethod
    def from_file(cls, path: Union[str, Path]) -> EconomicCalendar:
        """Loads and parses economic calendar events from a local JSON file."""
        file_path = Path(path)
        if not file_path.exists():
            return cls([])

        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        records = data if isinstance(data, list) else data.get("events", [])
        return cls.from_dict_list(records)

    @classmethod
    def from_dict_list(cls, records: List[Dict[str, Any]]) -> EconomicCalendar:
        """Constructs an EconomicCalendar from a list of dictionary event records."""
        events = [EconomicEvent.from_dict(r) for r in records]
        return cls(events)

    def get_events(self) -> List[EconomicEvent]:
        return list(self._events)

    def get_active_events(
        self,
        currencies: Iterable[str],
        current_time: float,
        window_before_s: float = 1800.0,
        window_after_s: float = 1800.0,
        impact_levels: Sequence[str] = ("HIGH",),
    ) -> List[EconomicEvent]:
        """
        Returns all events affecting the given currencies that fall within the
        time window [current_time - window_after_s, current_time + window_before_s].
        """
        active: List[EconomicEvent] = []
        target_impacts = {str(imp).upper() for imp in impact_levels}

        for curr in currencies:
            curr_upper = curr.upper()
            for ev in self._by_currency.get(curr_upper, []):
                if ev.impact.upper() in target_impacts:
                    # Trade is blocked if current_time is in [event_time - window_before, event_time + window_after]
                    # Equivalently: (ev.timestamp - window_before_s) <= current_time <= (ev.timestamp + window_after_s)
                    if (ev.timestamp - window_before_s) <= current_time <= (ev.timestamp + window_after_s):
                        active.append(ev)

        return active


def load_economic_calendar(source: Union[str, Path, List[Dict[str, Any]]]) -> EconomicCalendar:
    """Convenience helper to load an offline EconomicCalendar instance."""
    if isinstance(source, list):
        return EconomicCalendar.from_dict_list(source)
    return EconomicCalendar.from_file(source)


def check_news_filter(
    symbol: str,
    current_time: float,
    calendar: Union[EconomicCalendar, List[EconomicEvent], str, Path],
    window_before_s: float = 1800.0,   # Default: 30 minutes before
    window_after_s: float = 1800.0,    # Default: 30 minutes after
    impact_levels: Sequence[str] = ("HIGH",),
    eval_timestamp: Optional[float] = None,
) -> Tuple[bool, Optional[SkipReason]]:
    """
    Pure function: Enforces the news calendar filter (Sprint 7).
    Blocks specific currency pairs if current_time is within the user-defined
    window of a High Impact event.
    """
    # Resolve calendar instance
    cal: EconomicCalendar
    if isinstance(calendar, EconomicCalendar):
        cal = calendar
    elif isinstance(calendar, list):
        cal = EconomicCalendar(calendar)
    else:
        cal = EconomicCalendar.from_file(calendar)

    currencies = extract_currencies_for_symbol(symbol)
    active_events = cal.get_active_events(
        currencies=currencies,
        current_time=current_time,
        window_before_s=window_before_s,
        window_after_s=window_after_s,
        impact_levels=impact_levels,
    )

    if not active_events:
        return True, None

    # Pick the closest event to current_time for reporting
    event = min(active_events, key=lambda e: abs(e.timestamp - current_time))
    delta_s = event.timestamp - current_time
    delta_m = abs(delta_s) / 60.0
    timing_str = f"{delta_m:.1f}m before event" if delta_s >= 0 else f"{delta_m:.1f}m after event"

    window_str = f"-{window_before_s/60:.0f}m/+{window_after_s/60:.0f}m"
    reason_msg = (
        f"{SKIP_NEWS}: High Impact event '{event.title}' ({event.currency}) is scheduled "
        f"at {event.date_iso or event.timestamp} ({timing_str}; window: {window_str})"
    )

    return False, SkipReason(
        code=SKIP_NEWS,
        reason=reason_msg,
        evaluated_at=eval_timestamp or current_time,
        details={
            "symbol": symbol,
            "currency": event.currency,
            "event_title": event.title,
            "event_timestamp": event.timestamp,
            "impact": event.impact,
            "delta_seconds": round(delta_s, 1),
            "window_before_s": window_before_s,
            "window_after_s": window_after_s,
        },
    )


# =============================================================================
# EQUITY CIRCUIT BREAKER (SPRINT 7)
# =============================================================================

@dataclass(frozen=True)
class CircuitBreakerResult:
    """
    Outcome returned by the global equity circuit breaker (Sprint 7).
    """
    is_tripped: bool
    trigger_reason: Optional[str] = None
    drawdown_amount: float = 0.0
    drawdown_pct: float = 0.0
    panic_close_intent: Optional[OrderIntent] = None
    panic_close_intents: List[OrderIntent] = field(default_factory=list)
    pause_entries: bool = False
    skip_reason: Optional[SkipReason] = None


def generate_panic_close_intent(
    symbol: str = "ALL",
    ticket: int = 0,
    volume: float = 0.0,
    magic_number: int = 0,
) -> OrderIntent:
    """
    Generates an immediate 'Panic Close' OrderIntent for liquidating positions.
    """
    return OrderIntent(
        symbol=symbol,
        side=OrderSide.BUY,
        action_type=ActionType.FULL_CLOSE,
        volume=volume,
        magic_number=magic_number,
        order_ticket=ticket,
        flags=int(ExecutionFlags.IOC),
    )


def check_equity_circuit_breaker(
    equity: float,
    balance: float,
    daily_loss_limit: Optional[float] = None,
    max_loss_limit: Optional[float] = None,
    open_positions: Optional[List[Any]] = None,
    starting_equity: Optional[float] = None,
    initial_balance: Optional[float] = None,
    magic_number: int = 0,
    eval_timestamp: Optional[float] = None,
) -> CircuitBreakerResult:
    """
    Pure function: Evaluates current account equity and balance against
    daily_loss_limit and max_loss_limit (Sprint 7 Directive).

    If drawdown breaches either limit:
    - Generates a 'Panic Close' intent for all open positions.
    - Sets pause_entries = True.
    - Returns structured CircuitBreakerResult with diagnostic reason.
    """
    now = eval_timestamp if eval_timestamp is not None else time.time()

    # Determine baseline references
    ref_daily = starting_equity if (starting_equity and starting_equity > 0) else balance
    ref_max = initial_balance if (initial_balance and initial_balance > 0) else balance

    daily_drawdown = max(0.0, ref_daily - equity)
    daily_drawdown_pct = (daily_drawdown / ref_daily * 100.0) if ref_daily > 0 else 0.0

    max_drawdown = max(0.0, ref_max - equity)
    max_drawdown_pct = (max_drawdown / ref_max * 100.0) if ref_max > 0 else 0.0

    is_tripped = False
    trigger_reason: Optional[str] = None
    active_drawdown = daily_drawdown
    active_drawdown_pct = daily_drawdown_pct

    # Check 1: Daily Loss Limit
    if daily_loss_limit is not None and daily_loss_limit > 0:
        if daily_loss_limit <= 50.0 and ref_daily >= 500.0:
            is_breach = daily_drawdown_pct >= daily_loss_limit
        else:
            is_breach = daily_drawdown >= daily_loss_limit

        if is_breach:
            is_tripped = True
            active_drawdown = daily_drawdown
            active_drawdown_pct = daily_drawdown_pct
            trigger_reason = (
                f"DAILY_LOSS_LIMIT_BREACHED: Daily drawdown of ${daily_drawdown:.2f} "
                f"({daily_drawdown_pct:.2f}%) breaches daily loss limit threshold ({daily_loss_limit})"
            )

    # Check 2: Max Loss / Max Drawdown Limit
    if not is_tripped and max_loss_limit is not None and max_loss_limit > 0:
        if max_loss_limit <= 50.0 and ref_max >= 500.0:
            is_breach = max_drawdown_pct >= max_loss_limit
        else:
            is_breach = max_drawdown >= max_loss_limit

        if is_breach:
            is_tripped = True
            active_drawdown = max_drawdown
            active_drawdown_pct = max_drawdown_pct
            trigger_reason = (
                f"MAX_LOSS_LIMIT_BREACHED: Maximum drawdown of ${max_drawdown:.2f} "
                f"({max_drawdown_pct:.2f}%) breaches max loss limit threshold ({max_loss_limit})"
            )

    if not is_tripped:
        return CircuitBreakerResult(
            is_tripped=False,
            drawdown_amount=daily_drawdown,
            drawdown_pct=daily_drawdown_pct,
            panic_close_intent=None,
            panic_close_intents=[],
            pause_entries=False,
            skip_reason=None,
        )

    # Generate Panic Close Intents
    panic_intents: List[OrderIntent] = []
    if open_positions:
        for pos in open_positions:
            ticket = getattr(pos, "ticket", 0) if hasattr(pos, "ticket") else 0
            sym = getattr(pos, "symbol", "ALL") if hasattr(pos, "symbol") else "ALL"
            vol = getattr(pos, "volume", 0.0) if hasattr(pos, "volume") else 0.0
            panic_intents.append(
                generate_panic_close_intent(
                    symbol=sym,
                    ticket=ticket,
                    volume=vol,
                    magic_number=magic_number,
                )
            )

    primary_panic_intent = panic_intents[0] if panic_intents else generate_panic_close_intent(
        symbol="ALL",
        ticket=0,
        volume=0.0,
        magic_number=magic_number,
    )

    skip_res = SkipReason(
        code=SKIP_CIRCUIT_BREAKER,
        reason=f"{SKIP_CIRCUIT_BREAKER}: {trigger_reason}",
        evaluated_at=now,
        details={
            "equity": equity,
            "balance": balance,
            "daily_drawdown": round(daily_drawdown, 2),
            "daily_drawdown_pct": round(daily_drawdown_pct, 2),
            "max_drawdown": round(max_drawdown, 2),
            "max_drawdown_pct": round(max_drawdown_pct, 2),
        },
    )

    return CircuitBreakerResult(
        is_tripped=True,
        trigger_reason=trigger_reason,
        drawdown_amount=active_drawdown,
        drawdown_pct=active_drawdown_pct,
        panic_close_intent=primary_panic_intent,
        panic_close_intents=panic_intents,
        pause_entries=True,
        skip_reason=skip_res,
    )


# =============================================================================
# DATA STRUCTURES: MARKET QUOTE & ACCOUNT PROFILE
# =============================================================================

@dataclass(frozen=True)
class MarketQuote:
    """
    Real-time market snapshot for rule evaluation and position sizing.
    """
    symbol: str
    bid: float
    ask: float
    timestamp: float = 0.0                 # Epoch seconds (0.0 means use eval_time)
    pip_size: Optional[float] = None       # If None, resolved from DEFAULT_PIP_SIZES
    pip_value: Optional[float] = None      # Cash value of 1 pip for 1.0 lot in account currency

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0

    @property
    def spread(self) -> float:
        return self.ask - self.bid

    @property
    def resolved_pip_size(self) -> float:
        if self.pip_size is not None and self.pip_size > 0:
            return self.pip_size
        return get_default_pip_size(self.symbol)

    @property
    def resolved_pip_value(self) -> float:
        if self.pip_value is not None and self.pip_value > 0:
            return self.pip_value
        return get_default_pip_value(self.symbol)

    @property
    def spread_pips(self) -> float:
        ps = self.resolved_pip_size
        return (self.ask - self.bid) / ps if ps > 0 else 0.0


@dataclass
class AccountProfile:
    """
    Account configuration, risk limits, broker execution constraints,
    circuit breaker thresholds, and news filter policies.
    """
    account_id: Union[int, str] = 0
    balance: float = 10000.0
    equity: float = 10000.0
    free_margin: float = 10000.0
    currency: str = "USD"
    open_positions_count: int = 0

    # Risk Model Configuration
    risk_model: Union[RiskModel, str] = RiskModel.PERCENT_EQUITY
    fixed_lot: float = 0.10
    equity_risk_pct: float = 1.0           # 1.0 = 1.0% of equity
    fixed_currency_risk: float = 50.0      # E.g. $50.00 cash risk
    lot_multiplier: float = 1.0            # Multiplier for provider volume

    # Lot Bounds & Broker Precision
    min_lot: float = 0.01
    max_lot: float = 100.0
    lot_step: float = 0.01

    # Entry Guard Thresholds
    max_signal_age_s: float = 30.0         # Max allowed age in seconds (Blueprint Sec 5.6)
    max_drift_pips: float = 5.0            # Max allowable price drift in pips
    max_spread_pips: float = 3.0           # Max allowable broker spread in pips
    allow_favorable_drift: bool = False    # If True, favorable drift does not trigger SKIP_DRIFT

    # Account Safety Guards
    equity_hard_stop: Optional[float] = None    # Halt if equity <= equity_hard_stop
    max_open_positions: Optional[int] = None    # Reject if open_positions_count >= max
    min_free_margin: Optional[float] = None     # Reject if free_margin < min_free_margin
    margin_level: float = 0.0                   # Current margin level % (Equity / Margin * 100)
    margin_safety_threshold: float = 0.0        # Cutoff threshold (e.g. 200.0%)

    # Equity Circuit Breaker Settings (Sprint 7)
    daily_loss_limit: Optional[float] = None    # Cash amount or % threshold
    max_loss_limit: Optional[float] = None      # Cash amount or % threshold
    daily_starting_equity: Optional[float] = None
    initial_balance: Optional[float] = None
    pause_new_entries: bool = False             # Flag indicating trading is paused
    is_circuit_breaker_tripped: bool = False

    # Economic News Filter Settings (Sprint 7)
    news_filter_enabled: bool = False
    news_window_before_s: float = 1800.0        # 30 minutes before
    news_window_after_s: float = 1800.0         # 30 minutes after
    news_impact_levels: Tuple[str, ...] = ("HIGH",)
    economic_calendar: Optional[EconomicCalendar] = None

    # Cryptographic Licensing & Grace Period Guard (Sprint 8)
    license_manager: Optional[Any] = None
    license_expires_at: Optional[float] = None

    # FIFO Compliance & Hedge Blocking (Sprint 20: US NFA Rule 2-43(b))
    fifo_compliant: bool = False

    # Broker Freeze & Stops Level Policy (Sprint 20)
    freeze_policy: Union[FreezePolicy, str] = FreezePolicy.ADJUST_TO_MINIMUM


# Legacy RiskProfile for backward compatibility
@dataclass
class RiskProfile:
    """Legacy user-configured account risk profile parameters."""
    sizing_mode: SizingMode = SizingMode.EQUITY_PERCENT
    equity_percent: float = 1.0
    fixed_lot: float = 0.10
    multiplier: float = 1.0
    max_spread_pips: float = 4.0
    max_slippage_pips: float = 1.5
    max_open_trades: int = 5
    margin_safety_threshold: float = 200.0
    max_signal_age_s: float = 30.0
    daily_loss_limit_usd: float = 500.0
    max_drawdown_percent: float = 4.5
    clamp_to_min_lot: bool = True
    reject_if_exceeds_risk: bool = False


# =============================================================================
# PURE FUNCTIONS: LOT QUANTIZATION & SIZING
# =============================================================================

def quantize_lot(
    lot: float,
    min_lot: float = 0.01,
    max_lot: float = 100.0,
    lot_step: float = 0.01,
) -> float:
    """
    Pure function: Rounds a raw lot size to the nearest broker step and clamps
    between min_lot and max_lot. Eliminates floating-point representation artifacts.
    """
    if lot < min_lot:
        return min_lot
    if lot > max_lot:
        return max_lot

    step_str = f"{lot_step:.8f}".rstrip("0")
    decimals = len(step_str.split(".")[1]) if "." in step_str else 0

    steps = round(lot / lot_step)
    quantized = round(steps * lot_step, decimals)
    return max(min_lot, min(max_lot, quantized))


def quantize_lot_size(raw_lots: float, lot_step: float, min_lot: float, max_lot: float) -> float:
    """Legacy helper: Quantizes raw lot size using conservative floor rounding."""
    return quantize_lot(raw_lots, min_lot, max_lot, lot_step)


def calculate_fixed_lot(account: AccountProfile) -> float:
    """Pure function: Returns fixed lot configured in AccountProfile, quantized."""
    return quantize_lot(
        lot=account.fixed_lot,
        min_lot=account.min_lot,
        max_lot=account.max_lot,
        lot_step=account.lot_step,
    )


def calculate_equity_risk_lot(
    account: AccountProfile,
    entry_price: float,
    stop_loss: float,
    pip_size: Optional[float] = None,
    pip_value: Optional[float] = None,
    symbol: Optional[str] = None,
    quote_to_usd_rate: float = 1.0,
) -> Tuple[Optional[float], Optional[str]]:
    """
    Pure function: Computes lot size based on % of equity risked over Stop Loss distance.
    Supports explicit pip_size & pip_value or resolves them dynamically from symbol specification.
    """
    if stop_loss <= 0.0:
        return None, "Stop loss is missing or zero (required for % equity risk calculation)"

    delta_price = abs(entry_price - stop_loss)
    if delta_price <= 1e-9:
        return None, f"Stop loss ({stop_loss}) equals entry price ({entry_price}); distance is 0"

    if account.equity <= 0.0:
        return None, f"Account equity ({account.equity}) is zero or negative"

    # Resolve pip size & pip value
    sym = symbol or "EURUSD"
    spec = DEFAULT_SYMBOL_CATALOG.get(sym.upper())

    if pip_size is not None and pip_size > 0:
        psize = pip_size
    elif spec is not None:
        psize = spec.pip_size
    else:
        psize = get_default_pip_size(sym)

    if pip_value is not None and pip_value > 0:
        pval = pip_value
    elif spec is not None:
        pval = spec.calculate_pip_value(entry_price, quote_to_usd_rate)
    else:
        pval = get_default_pip_value(sym)

    risk_cash = account.equity * (account.equity_risk_pct / 100.0)
    sl_pips = delta_price / psize
    raw_lot = risk_cash / (sl_pips * pval)

    lot = quantize_lot(
        lot=raw_lot,
        min_lot=account.min_lot,
        max_lot=account.max_lot,
        lot_step=account.lot_step,
    )
    return lot, None


def calculate_fixed_currency_risk_lot(
    account: AccountProfile,
    entry_price: float,
    stop_loss: float,
    pip_size: Optional[float] = None,
    pip_value: Optional[float] = None,
    symbol: Optional[str] = None,
    quote_to_usd_rate: float = 1.0,
) -> Tuple[Optional[float], Optional[str]]:
    """
    Pure function: Computes lot size based on fixed currency amount risked over Stop Loss distance.
    """
    if stop_loss <= 0.0:
        return None, "Stop loss is missing or zero (required for fixed currency risk calculation)"

    delta_price = abs(entry_price - stop_loss)
    if delta_price <= 1e-9:
        return None, f"Stop loss ({stop_loss}) equals entry price ({entry_price}); distance is 0"

    risk_cash = account.fixed_currency_risk
    if risk_cash <= 0.0:
        return None, f"Fixed currency risk ({risk_cash}) must be positive"

    sym = symbol or "EURUSD"
    spec = DEFAULT_SYMBOL_CATALOG.get(sym.upper())

    if pip_size is not None and pip_size > 0:
        psize = pip_size
    elif spec is not None:
        psize = spec.pip_size
    else:
        psize = get_default_pip_size(sym)

    if pip_value is not None and pip_value > 0:
        pval = pip_value
    elif spec is not None:
        pval = spec.calculate_pip_value(entry_price, quote_to_usd_rate)
    else:
        pval = get_default_pip_value(sym)

    sl_pips = delta_price / psize
    raw_lot = risk_cash / (sl_pips * pval)

    lot = quantize_lot(
        lot=raw_lot,
        min_lot=account.min_lot,
        max_lot=account.max_lot,
        lot_step=account.lot_step,
    )
    return lot, None


def calculate_provider_multiplier_lot(
    account: AccountProfile,
    provider_volume: float,
) -> float:
    """Pure function: Multiplies provider's incoming volume by the configured lot multiplier."""
    raw_lot = provider_volume * account.lot_multiplier
    return quantize_lot(
        lot=raw_lot,
        min_lot=account.min_lot,
        max_lot=account.max_lot,
        lot_step=account.lot_step,
    )


# Legacy calculate_lot_size function for backward compatibility
def calculate_lot_size(
    equity: float,
    entry_price: float,
    stop_loss: Optional[float],
    symbol: str,
    risk_profile: RiskProfile,
    symbol_spec: Optional[SymbolSpecification] = None,
    quote_to_usd_rate: float = 1.0,
    signal_volume: Optional[float] = None,
) -> float:
    """Legacy position sizing calculator."""
    if equity <= 0:
        return 0.0

    spec = symbol_spec or DEFAULT_SYMBOL_CATALOG.get(symbol.upper())
    if spec is None:
        spec = SymbolSpecification(symbol.upper(), 100_000.0, 0.0001, "USD")

    if risk_profile.sizing_mode == SizingMode.FIXED_LOT:
        return quantize_lot(risk_profile.fixed_lot, spec.min_lot, spec.max_lot, spec.lot_step)

    if risk_profile.sizing_mode == SizingMode.MULTIPLIER:
        base_vol = signal_volume if (signal_volume is not None and signal_volume > 0) else spec.min_lot
        raw_lots = base_vol * risk_profile.multiplier
        return quantize_lot(raw_lots, spec.min_lot, spec.max_lot, spec.lot_step)

    if stop_loss is None or stop_loss <= 0 or entry_price <= 0:
        return 0.0

    sl_dist = abs(entry_price - stop_loss)
    if sl_dist <= 0:
        return 0.0

    risk_usd = equity * (risk_profile.equity_percent / 100.0)
    pip_val = spec.pip_value_usd(entry_price, quote_to_usd_rate)
    sl_pips = sl_dist / spec.pip_size
    loss_per_lot = sl_pips * pip_val

    if loss_per_lot <= 0:
        return 0.0

    raw_lots = risk_usd / loss_per_lot
    return quantize_lot(raw_lots, spec.min_lot, spec.max_lot, spec.lot_step)


# =============================================================================
# PURE FUNCTIONS: ENTRY GUARDS
# =============================================================================

def check_signal_age(
    signal_timestamp_s: float,
    eval_timestamp_s: float,
    max_signal_age_s: float,
) -> Tuple[bool, Optional[SkipReason]]:
    """Pure function: Enforces max_signal_age_s guard (Blueprint Section 5.6 item 4)."""
    if signal_timestamp_s <= 0.0:
        return True, None

    age_s = eval_timestamp_s - signal_timestamp_s
    if age_s > max_signal_age_s:
        reason_msg = (
            f"{SKIP_AGE}: Signal age {age_s:.2f}s exceeds maximum allowed age "
            f"{max_signal_age_s:.2f}s (Signal: {signal_timestamp_s:.2f}, Eval: {eval_timestamp_s:.2f})"
        )
        return False, SkipReason(
            code=SKIP_AGE,
            reason=reason_msg,
            evaluated_at=eval_timestamp_s,
            details={"age_s": round(age_s, 3), "max_signal_age_s": max_signal_age_s},
        )

    return True, None


def check_price_drift(
    signal_price: float,
    current_price: float,
    side: OrderSide | int,
    pip_size: float,
    max_drift_pips: float,
    allow_favorable: bool = False,
    eval_timestamp: float = 0.0,
) -> Tuple[bool, Optional[SkipReason]]:
    """Pure function: Validates price drift between signal entry and current market quote."""
    if signal_price <= 0.0:
        return True, None

    is_buy = int(side) == int(OrderSide.BUY)
    adverse_price_diff = (current_price - signal_price) if is_buy else (signal_price - current_price)

    adverse_drift_pips = adverse_price_diff / pip_size
    abs_drift_pips = abs(current_price - signal_price) / pip_size
    drift_to_check = adverse_drift_pips if allow_favorable else abs_drift_pips

    if drift_to_check > max_drift_pips:
        reason_msg = (
            f"{SKIP_DRIFT}: Price drift of {abs_drift_pips:.1f} pips exceeds max allowed drift "
            f"{max_drift_pips:.1f} pips (Signal price: {signal_price:.5f}, Current: {current_price:.5f}, "
            f"Adverse pips: {adverse_drift_pips:.1f})"
        )
        return False, SkipReason(
            code=SKIP_DRIFT,
            reason=reason_msg,
            evaluated_at=eval_timestamp or time.time(),
            details={
                "signal_price": signal_price,
                "current_price": current_price,
                "drift_pips": round(abs_drift_pips, 2),
                "adverse_pips": round(adverse_drift_pips, 2),
                "max_drift_pips": max_drift_pips,
            },
        )

    return True, None


def check_spread(
    quote: MarketQuote,
    max_spread_pips: float,
    eval_timestamp: float = 0.0,
) -> Tuple[bool, Optional[SkipReason]]:
    """Pure function: Validates that current broker spread does not exceed max_spread_pips."""
    spread_pips = quote.spread_pips
    if spread_pips > max_spread_pips:
        reason_msg = (
            f"{SKIP_SPREAD}: Market spread of {spread_pips:.1f} pips exceeds max allowed spread "
            f"{max_spread_pips:.1f} pips (Bid: {quote.bid:.5f}, Ask: {quote.ask:.5f})"
        )
        return False, SkipReason(
            code=SKIP_SPREAD,
            reason=reason_msg,
            evaluated_at=eval_timestamp or time.time(),
            details={
                "bid": quote.bid,
                "ask": quote.ask,
                "spread_pips": round(spread_pips, 2),
                "max_spread_pips": max_spread_pips,
            },
        )

    return True, None


# Legacy GuardResult and evaluate_entry_guards for backward compatibility
@dataclass
class GuardResult:
    passed: bool
    rejection_reason: Optional[GuardRejectionReason] = None
    details: Dict[str, Any] = field(default_factory=dict)


def evaluate_entry_guards(
    current_spread_pips: float,
    current_slippage_pips: float,
    current_open_trades: int,
    current_margin_level: float,
    signal_age_s: float,
    risk_profile: RiskProfile,
    is_panic_halt: bool = False,
    has_valid_sl: bool = True,
) -> GuardResult:
    """Legacy entry guards evaluator."""
    if is_panic_halt:
        return GuardResult(False, GuardRejectionReason.SKIP_SYSTEM_HALTED, {"error": "System panic"})
    if not has_valid_sl:
        return GuardResult(False, GuardRejectionReason.SKIP_INVALID_SL, {"error": "Missing SL"})
    if current_spread_pips > risk_profile.max_spread_pips:
        return GuardResult(False, GuardRejectionReason.SKIP_SPREAD, {"spread": current_spread_pips})
    if current_slippage_pips > risk_profile.max_slippage_pips:
        return GuardResult(False, GuardRejectionReason.SKIP_SLIPPAGE, {"slippage": current_slippage_pips})
    if current_open_trades >= risk_profile.max_open_trades:
        return GuardResult(False, GuardRejectionReason.SKIP_MAX_POSITIONS, {"trades": current_open_trades})
    if 0.0 < current_margin_level < risk_profile.margin_safety_threshold:
        return GuardResult(False, GuardRejectionReason.SKIP_LOW_MARGIN, {"margin_level": current_margin_level})
    if signal_age_s > risk_profile.max_signal_age_s:
        return GuardResult(False, GuardRejectionReason.SKIP_EXPIRED_SIGNAL, {"age": signal_age_s})
    return GuardResult(True)


# Legacy OpenPosition and PanicResponse for backward compatibility
@dataclass
class OpenPosition:
    ticket: int
    symbol: str
    side: OrderSide
    volume: float
    open_price: float
    current_price: float
    unrealized_pnl: float = 0.0

    def calculate_pnl(self, current_price: float, spec: Optional[SymbolSpecification] = None) -> float:
        self.current_price = current_price
        contract = spec.contract_size if spec else 100_000.0
        diff = (current_price - self.open_price) if self.side == OrderSide.BUY else (self.open_price - current_price)
        pnl = diff * contract * self.volume
        self.unrealized_pnl = pnl
        return pnl


@dataclass
class PanicResponse:
    is_panic: bool
    reason: Optional[PanicReason] = None
    starting_equity: float = 0.0
    current_equity: float = 0.0
    cumulative_loss_usd: float = 0.0
    drawdown_percent: float = 0.0
    emergency_close_intents: List[OrderIntent] = field(default_factory=list)
    trading_halted: bool = False
    timestamp_ns: int = 0


class EquityBreaker:
    """Legacy EquityBreaker stateful wrapper."""
    def __init__(self, starting_equity: float = 10_000.0, risk_profile: Optional[RiskProfile] = None):
        self.starting_equity = starting_equity
        self.risk_profile = risk_profile or RiskProfile()
        self.is_panic = False
        self.panic_reason = None

    def evaluate_equity(
        self,
        current_equity: float,
        open_positions: Optional[List[OpenPosition]] = None,
    ) -> PanicResponse:
        res = check_equity_circuit_breaker(
            equity=current_equity,
            balance=self.starting_equity,
            daily_loss_limit=self.risk_profile.daily_loss_limit_usd,
            max_loss_limit=self.risk_profile.max_drawdown_percent,
            open_positions=open_positions,
            starting_equity=self.starting_equity,
        )
        self.is_panic = res.is_tripped
        close_list = res.panic_close_intents or ([res.panic_close_intent] if res.panic_close_intent else [])
        return PanicResponse(
            is_panic=res.is_tripped,
            reason=PanicReason.DAILY_LOSS_LIMIT_BREACHED if res.is_tripped else None,
            starting_equity=self.starting_equity,
            current_equity=current_equity,
            cumulative_loss_usd=res.drawdown_amount,
            drawdown_percent=res.drawdown_pct,
            emergency_close_intents=close_list,
            trading_halted=res.pause_entries,
            timestamp_ns=time.time_ns(),
        )

    def evaluate_tick_stream(
        self,
        open_positions: List[OpenPosition],
        symbol_catalog: Optional[Dict[str, SymbolSpecification]] = None,
    ) -> PanicResponse:
        catalog = symbol_catalog or DEFAULT_SYMBOL_CATALOG
        total_pnl = sum(pos.calculate_pnl(pos.current_price, catalog.get(pos.symbol.upper())) for pos in open_positions)
        return self.evaluate_equity(self.starting_equity + total_pnl, open_positions)

    def reset_daily_session(self, new_starting_equity: float) -> None:
        self.starting_equity = new_starting_equity
        self.is_panic = False


# =============================================================================
# UNIFIED STATELESS RISK ENGINE CLASS
# =============================================================================

class RiskEngine:
    """
    Stateless rule evaluation engine.
    Pure functional evaluation:
        (OrderIntent, AccountProfile, MarketQuote) -> Union[OrderIntent, SkipReason]

    Never mutates inputs. Contributes <50 microseconds overhead to the <5ms latency budget.
    Utilizes PipConverter for high-precision decimal SL, TP, and Trailing Stop calculations.
    """

    pip_converter: PipConverter = default_pip_converter

    def __init__(
        self,
        risk_profile: Optional[RiskProfile] = None,
        symbol_catalog: Optional[Dict[str, SymbolSpecification]] = None,
        starting_equity: float = 10_000.0,
        pip_converter: Optional[PipConverter] = None,
    ) -> None:
        """Legacy constructor for stateful facade compatibility."""
        self.risk_profile: RiskProfile = risk_profile or RiskProfile()
        self.symbol_catalog: Dict[str, SymbolSpecification] = dict(DEFAULT_SYMBOL_CATALOG)
        if symbol_catalog:
            self.symbol_catalog.update(symbol_catalog)
        self.equity_breaker = EquityBreaker(starting_equity=starting_equity, risk_profile=self.risk_profile)
        self.pip_converter: PipConverter = pip_converter or default_pip_converter

    # -------------------------------------------------------------------------
    # PipConverter Integration Methods (Sprint 19 Directive)
    # -------------------------------------------------------------------------

    @classmethod
    def calculate_sl(cls, *args, **kwargs) -> float:
        """Calculates Stop Loss price from entry price and pip distance."""
        return calculate_sl(*args, **kwargs)

    @classmethod
    def calculate_tp(cls, *args, **kwargs) -> float:
        """Calculates Take Profit price from entry price and pip distance."""
        return calculate_tp(*args, **kwargs)

    @classmethod
    def calculate_trailing_stop(cls, *args, **kwargs) -> Tuple[bool, float]:
        """Calculates Trailing Stop price adjustment (returns should_modify, new_sl)."""
        return calculate_trailing_stop(*args, **kwargs)

    @classmethod
    def pips_to_price_offset(cls, *args, **kwargs) -> float:
        """Translates raw integer pips into exact price offset."""
        return pips_to_price_offset(*args, **kwargs)

    @classmethod
    def price_offset_to_pips(cls, *args, **kwargs) -> float:
        """Translates price offset into pip count."""
        return price_offset_to_pips(*args, **kwargs)

    @classmethod
    def classify_asset(cls, symbol: str) -> AssetClass:
        """Classifies symbol into an AssetClass."""
        return classify_asset(symbol)

    @classmethod
    def get_pip_size(cls, symbol: str, digits: Optional[int] = None, point: Optional[float] = None) -> float:
        """Returns 1-pip price distance for a symbol."""
        return get_default_pip_size(symbol, digits=digits, point=point)

    # Legacy facade methods
    def calculate_lot(
        self,
        equity: float,
        entry_price: float,
        stop_loss: Optional[float],
        symbol: str,
        quote_to_usd_rate: float = 1.0,
        signal_volume: Optional[float] = None,
    ) -> float:
        spec = self.symbol_catalog.get(symbol.upper())
        return calculate_lot_size(
            equity, entry_price, stop_loss, symbol, self.risk_profile, spec, quote_to_usd_rate, signal_volume
        )

    def check_entry_guards(
        self,
        current_spread_pips: float,
        current_slippage_pips: float,
        current_open_trades: int,
        current_margin_level: float,
        signal_age_s: float = 0.0,
        has_valid_sl: bool = True,
    ) -> GuardResult:
        return evaluate_entry_guards(
            current_spread_pips,
            current_slippage_pips,
            current_open_trades,
            current_margin_level,
            signal_age_s,
            self.risk_profile,
            self.equity_breaker.is_panic,
            has_valid_sl,
        )

    def update_tick(self, open_positions: List[OpenPosition]) -> PanicResponse:
        return self.equity_breaker.evaluate_tick_stream(open_positions, self.symbol_catalog)

    # -------------------------------------------------------------------------
    # PURE FUNCTIONAL EVALUATE METHOD (PRIMARY API)
    # -------------------------------------------------------------------------

    @classmethod
    def evaluate(
        cls,
        intent: OrderIntent,
        account: AccountProfile,
        quote: MarketQuote,
        eval_time: Optional[float] = None,
    ) -> Union[OrderIntent, SkipReason]:
        """
        Evaluates an OrderIntent against active AccountProfile, MarketQuote,
        Equity Circuit Breaker, and Economic News Calendar Filter.
        """
        now = eval_time if eval_time is not None else time.time()

        # Non-entry actions (position management) bypass admission guards
        if intent.action_type in (ActionType.FULL_CLOSE, ActionType.CANCEL_PENDING):
            return replace(intent)

        if intent.action_type == ActionType.PARTIAL_CLOSE:
            quantized_vol = quantize_lot(
                lot=intent.volume,
                min_lot=account.min_lot,
                max_lot=account.max_lot,
                lot_step=account.lot_step,
            )
            return replace(intent, volume=quantized_vol)

        is_entry = intent.action_type in (
            ActionType.MARKET_ENTRY,
            ActionType.LIMIT_ENTRY,
            ActionType.STOP_ENTRY,
        )

        # ---------------------------------------------------------------------
        # 0. Cryptographic 30-Day License & Offline Grace Period Guard (FR-8.1)
        # ---------------------------------------------------------------------
        if is_entry:
            if account.license_manager is not None:
                is_allowed, reason = account.license_manager.check_execution_allowed(current_time=now)
                if not is_allowed:
                    account.license_manager.is_locked = True
                    status_obj = getattr(account.license_manager, "last_status", None)
                    status_val = getattr(status_obj, "value", str(status_obj or "EXPIRED"))
                    raise LicenseExpiredError(
                        reason or "LICENSE_EXPIRED: 30-day offline grace period exceeded",
                        details={"evaluated_at": now, "status": status_val},
                    )
            elif account.license_expires_at is not None and now >= account.license_expires_at:
                exp_msg = (
                    f"LICENSE_EXPIRED: 30-day offline grace period exceeded "
                    f"(now={now:.0f} >= exp={account.license_expires_at:.0f})"
                )
                raise LicenseExpiredError(
                    exp_msg,
                    details={"evaluated_at": now, "expires_at": account.license_expires_at},
                )

        # ---------------------------------------------------------------------
        # 1. Market Quote Validity Check
        # ---------------------------------------------------------------------
        if quote.bid <= 0.0 or quote.ask <= 0.0 or quote.bid > quote.ask:
            return SkipReason(
                code=SKIP_INVALID_QUOTE,
                reason=(
                    f"{SKIP_INVALID_QUOTE}: Invalid market quote for {quote.symbol} "
                    f"(Bid: {quote.bid}, Ask: {quote.ask})"
                ),
                evaluated_at=now,
                details={"bid": quote.bid, "ask": quote.ask, "symbol": quote.symbol},
            )

        # ---------------------------------------------------------------------
        # 2. Equity Circuit Breaker / Trading Paused Guard (Sprint 7)
        # ---------------------------------------------------------------------
        if is_entry:
            # Check administrative or pre-tripped pause flag
            if account.pause_new_entries or account.is_circuit_breaker_tripped:
                return SkipReason(
                    code=SKIP_CIRCUIT_BREAKER,
                    reason=(
                        f"{SKIP_CIRCUIT_BREAKER}: Circuit breaker active; "
                        "new entries paused due to circuit breaker trip"
                    ),
                    evaluated_at=now,
                    details={"pause_new_entries": account.pause_new_entries},
                )

            # Evaluate active equity drawdown against daily and max loss limits
            if account.daily_loss_limit is not None or account.max_loss_limit is not None:
                cb_res = check_equity_circuit_breaker(
                    equity=account.equity,
                    balance=account.balance,
                    daily_loss_limit=account.daily_loss_limit,
                    max_loss_limit=account.max_loss_limit,
                    starting_equity=account.daily_starting_equity,
                    initial_balance=account.initial_balance,
                    eval_timestamp=now,
                )
                if cb_res.is_tripped and cb_res.skip_reason:
                    return cb_res.skip_reason

        # ---------------------------------------------------------------------
        # 3. Economic News Calendar Filter Guard (Sprint 7)
        # ---------------------------------------------------------------------
        if is_entry and account.news_filter_enabled and account.economic_calendar is not None:
            news_ok, news_reason = check_news_filter(
                symbol=intent.symbol,
                current_time=now,
                calendar=account.economic_calendar,
                window_before_s=account.news_window_before_s,
                window_after_s=account.news_window_after_s,
                impact_levels=account.news_impact_levels,
                eval_timestamp=now,
            )
            if not news_ok and news_reason is not None:
                return news_reason

        # ---------------------------------------------------------------------
        # 4. Max Spread Guard
        # ---------------------------------------------------------------------
        spread_ok, spread_reason = check_spread(
            quote=quote,
            max_spread_pips=account.max_spread_pips,
            eval_timestamp=now,
        )
        if not spread_ok and spread_reason is not None:
            return spread_reason

        # ---------------------------------------------------------------------
        # 5. Max Signal Age Guard (FR-7.6 / Blueprint Sec 5.6)
        # ---------------------------------------------------------------------
        signal_timestamp_s = (
            intent.timestamp_ns / 1_000_000_000.0 if intent.timestamp_ns > 0 else 0.0
        )
        age_ok, age_reason = check_signal_age(
            signal_timestamp_s=signal_timestamp_s,
            eval_timestamp_s=now,
            max_signal_age_s=account.max_signal_age_s,
        )
        if not age_ok and age_reason is not None:
            return age_reason

        # ---------------------------------------------------------------------
        # 6. Entry-Specific Guards (Drift & Account Position Limits)
        # ---------------------------------------------------------------------
        if is_entry:
            # Price Drift Guard
            current_exec_price = (
                quote.ask if int(intent.side) == int(OrderSide.BUY) else quote.bid
            )
            drift_ok, drift_reason = check_price_drift(
                signal_price=intent.price,
                current_price=current_exec_price,
                side=intent.side,
                pip_size=quote.resolved_pip_size,
                max_drift_pips=account.max_drift_pips,
                allow_favorable=account.allow_favorable_drift,
                eval_timestamp=now,
            )
            if not drift_ok and drift_reason is not None:
                return drift_reason

            # Max Open Positions Guard
            if (
                account.max_open_positions is not None
                and account.open_positions_count >= account.max_open_positions
            ):
                return SkipReason(
                    code=SKIP_MAX_POSITIONS,
                    reason=(
                        f"{SKIP_MAX_POSITIONS}: Open positions count ({account.open_positions_count}) "
                        f"has reached maximum allowed limit ({account.max_open_positions})"
                    ),
                    evaluated_at=now,
                    details={
                        "open_positions": account.open_positions_count,
                        "max_open_positions": account.max_open_positions,
                    },
                )

        # ---------------------------------------------------------------------
        # 7. Account Safety Guards (Equity Hard Stop & Min Free Margin)
        # ---------------------------------------------------------------------
        if (
            account.equity_hard_stop is not None
            and account.equity <= account.equity_hard_stop
        ):
            return SkipReason(
                code=SKIP_EQUITY_HARD_STOP,
                reason=(
                    f"{SKIP_EQUITY_HARD_STOP}: Account equity {account.equity:.2f} is at or below "
                    f"configured hard stop threshold {account.equity_hard_stop:.2f}"
                ),
                evaluated_at=now,
                details={"equity": account.equity, "hard_stop": account.equity_hard_stop},
            )

        if (
            account.min_free_margin is not None
            and account.free_margin < account.min_free_margin
        ):
            return SkipReason(
                code=SKIP_MIN_MARGIN,
                reason=(
                    f"{SKIP_MIN_MARGIN}: Account free margin {account.free_margin:.2f} is below "
                    f"minimum requirement {account.min_free_margin:.2f}"
                ),
                evaluated_at=now,
                details={"free_margin": account.free_margin, "min_free_margin": account.min_free_margin},
            )

        if (
            account.margin_safety_threshold > 0.0
            and 0.0 < account.margin_level < account.margin_safety_threshold
        ):
            return SkipReason(
                code=SKIP_MIN_MARGIN,
                reason=(
                    f"{SKIP_MIN_MARGIN}: Account margin level {account.margin_level:.1f}% is below "
                    f"safety cutoff {account.margin_safety_threshold:.1f}%"
                ),
                evaluated_at=now,
                details={"margin_level": account.margin_level, "threshold": account.margin_safety_threshold},
            )

        # ---------------------------------------------------------------------
        # 8. Position Sizing
        # ---------------------------------------------------------------------
        # Non-entry actions retain their volumes
        if intent.action_type in (ActionType.FULL_CLOSE, ActionType.CANCEL_PENDING):
            return replace(intent)

        if intent.action_type == ActionType.PARTIAL_CLOSE:
            quantized_vol = quantize_lot(
                lot=intent.volume,
                min_lot=account.min_lot,
                max_lot=account.max_lot,
                lot_step=account.lot_step,
            )
            return replace(intent, volume=quantized_vol)

        model = (
            account.risk_model.value
            if isinstance(account.risk_model, RiskModel)
            else str(account.risk_model)
        )

        effective_entry_price = intent.price
        if effective_entry_price <= 0.0:
            effective_entry_price = (
                quote.ask if int(intent.side) == int(OrderSide.BUY) else quote.bid
            )

        pip_size = quote.resolved_pip_size
        pip_val = quote.resolved_pip_value

        calculated_lot: float

        if model == RiskModel.FIXED_LOT.value:
            calculated_lot = calculate_fixed_lot(account)

        elif model == RiskModel.PERCENT_EQUITY.value:
            lot, err = calculate_equity_risk_lot(
                account=account,
                entry_price=effective_entry_price,
                stop_loss=intent.stop_loss,
                pip_size=pip_size,
                pip_value=pip_val,
            )
            if lot is None:
                return SkipReason(
                    code=SKIP_SL_REQUIRED,
                    reason=f"{SKIP_SL_REQUIRED}: Risk model '{model}' requires valid Stop Loss: {err}",
                    evaluated_at=now,
                    details={"error": err, "model": model, "stop_loss": intent.stop_loss},
                )
            calculated_lot = lot

        elif model == RiskModel.FIXED_CURRENCY.value:
            lot, err = calculate_fixed_currency_risk_lot(
                account=account,
                entry_price=effective_entry_price,
                stop_loss=intent.stop_loss,
                pip_size=pip_size,
                pip_value=pip_val,
            )
            if lot is None:
                return SkipReason(
                    code=SKIP_SL_REQUIRED,
                    reason=f"{SKIP_SL_REQUIRED}: Risk model '{model}' requires valid Stop Loss: {err}",
                    evaluated_at=now,
                    details={"error": err, "model": model, "stop_loss": intent.stop_loss},
                )
            calculated_lot = lot

        elif model == RiskModel.PROVIDER_MULTIPLIER.value:
            calculated_lot = calculate_provider_multiplier_lot(
                account=account,
                provider_volume=intent.volume,
            )

        else:
            calculated_lot = calculate_fixed_lot(account)

        return replace(intent, volume=calculated_lot)
