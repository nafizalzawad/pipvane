"""
Pipvane v3 Core Signal Parser & Format Pack Classification Engine.

Evaluates normalized Telegram text, classifies messages (signal / update / chatter),
and extracts strict OrderIntent fields using dynamic, JSON-based format packs:
- Classification Engine (signal, update, chatter)
- Multi-style Format Pack Grammar Registry (standard_block, single_line, reply_update, emoji_compact, localized_arabic)
- Reliable field extraction: symbol, side, order_type, entry (single or range), SL, multiple TPs (up to 10), and risk modifiers
- Market Geometry Validation (enforcing SL/TP directional rules; throws SKIP_GEOMETRY on violation)
- Extraction completeness Confidence Scoring (0 to 100)
- Direct mapping to 128-byte OrderIntent for IPC dispatch
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import json
import logging
import math
import os
import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

from engine.normalizer import TextNormalizer, clean_text
from engine.ipc_protocol import (
    OrderIntent,
    OrderSide,
    ActionType,
    ExecutionFlags,
)

logger = logging.getLogger("pipvane.engine.signal_parser")

SKIP_GEOMETRY = "SKIP_GEOMETRY"


class GeometryValidationError(ValueError):
    """
    Raised when an extracted trade signal violates market order geometry.
    Never fails silently; provides exact SKIP_GEOMETRY reason code.
    """
    def __init__(self, message: str, reason_code: str = SKIP_GEOMETRY):
        super().__init__(f"{reason_code}: {message}")
        self.reason_code = reason_code
        self.message = message


# =============================================================================
# ENUMS & DATA STRUCTURES
# =============================================================================

class MessageType(str, Enum):
    """Categorical classification of raw incoming Telegram channel content."""
    SIGNAL = "signal"    # New actionable trade order directive
    UPDATE = "update"    # Position update / modification / risk management
    CHATTER = "chatter"  # Conversation, analysis, news commentary, marketing, disclaimers


# Standard symbol canonical normalization map
SYMBOL_ALIASES: Dict[str, str] = {
    "GOLD": "XAUUSD",
    "الذهب": "XAUUSD",
    "ذهب": "XAUUSD",
    "SILVER": "XAGUSD",
    "DJ30": "US30",
    "WS30": "US30",
    "DOW": "US30",
    "WALLSTREET": "US30",
    "USTEC": "NAS100",
    "NDX": "NAS100",
    "NASDAQ": "NAS100",
    "NAS": "NAS100",
    "US500": "SPX500",
    "SP500": "SPX500",
    "SPX": "SPX500",
    "GER30": "GER40",
    "DAX": "GER40",
    "DAX40": "GER40",
    "UK100": "UK100",
    "FTSE": "UK100",
    "WTI": "USOIL",
    "CRUDE": "USOIL",
    "BRENT": "UKOIL",
    "BTC": "BTCUSD",
    "ETH": "ETHUSD",
    "SOL": "SOLUSD",
}

KNOWN_SYMBOLS_SET = {
    "XAUUSD", "GOLD", "XAGUSD", "SILVER",
    "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "NZDUSD", "USDCAD",
    "GBPJPY", "EURJPY", "EURGBP", "AUDJPY", "CADJPY", "GBPAUD", "EURAUD",
    "US30", "DJ30", "WS30", "NAS100", "USTEC", "SPX500", "US500", "GER40", "GER30",
    "BTCUSD", "ETHUSD", "SOLUSD", "XRPUSD", "USOIL", "UKOIL", "WTI", "CRUDE", "BRENT"
}

ALL_KNOWN_SYMBOLS = set(KNOWN_SYMBOLS_SET) | set(SYMBOL_ALIASES.keys())

RESERVED_WORDS = {
    "MODIFY", "UPDATE", "CANCEL", "MARKET", "SIGNAL", "TARGET",
    "PROFIT", "SECURE", "REPORT", "CLOSED", "ENTRY", "LEVERAGE"
}


@dataclass
class FormatPack:
    """Represents a compiled, updateable regex grammar for a channel style."""
    pack_id: str
    name: str
    description: str
    version: str
    priority: int
    style: str
    classification_markers: Dict[str, List[str]]
    patterns: Dict[str, Any]

    # Pre-compiled regex caches
    _compiled_symbol: List[re.Pattern] = field(default_factory=list, repr=False)
    _compiled_side: List[re.Pattern] = field(default_factory=list, repr=False)
    _compiled_limit: Optional[re.Pattern] = field(default=None, repr=False)
    _compiled_stop: Optional[re.Pattern] = field(default=None, repr=False)
    _compiled_entry: List[re.Pattern] = field(default_factory=list, repr=False)
    _compiled_sl: List[re.Pattern] = field(default_factory=list, repr=False)
    _compiled_tp: List[re.Pattern] = field(default_factory=list, repr=False)
    _compiled_partial_close: Optional[re.Pattern] = field(default=None, repr=False)
    _compiled_breakeven: Optional[re.Pattern] = field(default=None, repr=False)
    _compiled_full_close: Optional[re.Pattern] = field(default=None, repr=False)
    _compiled_cancel_pending: Optional[re.Pattern] = field(default=None, repr=False)

    def __post_init__(self):
        self._compile_patterns()

    def _compile_patterns(self) -> None:
        """Pre-compiles all regex patterns for maximum execution speed (<100us)."""
        pat = self.patterns

        # Symbols
        self._compiled_symbol = [
            re.compile(p, re.IGNORECASE) for p in pat.get("symbol", [])
        ]
        # Sides
        self._compiled_side = [
            re.compile(p, re.IGNORECASE) for p in pat.get("side", [])
        ]
        # Order types
        ot = pat.get("order_type", {})
        if "limit" in ot:
            self._compiled_limit = re.compile(ot["limit"], re.IGNORECASE)
        if "stop" in ot:
            self._compiled_stop = re.compile(ot["stop"], re.IGNORECASE)

        # Entry
        self._compiled_entry = [
            re.compile(p, re.IGNORECASE) for p in pat.get("entry", [])
        ]
        # SL
        self._compiled_sl = [
            re.compile(p, re.IGNORECASE) for p in pat.get("stop_loss", [])
        ]
        # TP
        self._compiled_tp = [
            re.compile(p, re.IGNORECASE) for p in pat.get("take_profit", [])
        ]

        # Modifiers
        mods = pat.get("modifiers", {})
        if "partial_close" in mods:
            self._compiled_partial_close = re.compile(mods["partial_close"], re.IGNORECASE)
        if "breakeven" in mods:
            self._compiled_breakeven = re.compile(mods["breakeven"], re.IGNORECASE)
        if "full_close" in mods:
            self._compiled_full_close = re.compile(mods["full_close"], re.IGNORECASE)
        if "cancel_pending" in mods:
            self._compiled_cancel_pending = re.compile(mods["cancel_pending"], re.IGNORECASE)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> FormatPack:
        return cls(
            pack_id=data["pack_id"],
            name=data.get("name", data["pack_id"]),
            description=data.get("description", ""),
            version=data.get("version", "1.0.0"),
            priority=data.get("priority", 10),
            style=data.get("style", "custom"),
            classification_markers=data.get("classification_markers", {}),
            patterns=data.get("patterns", {}),
        )

    @classmethod
    def from_json(cls, json_str_or_path: Union[str, Path]) -> FormatPack:
        if os.path.isfile(str(json_str_or_path)):
            with open(json_str_or_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        else:
            data = json.loads(str(json_str_or_path))
        return cls.from_dict(data)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pack_id": self.pack_id,
            "name": self.name,
            "description": self.description,
            "version": self.version,
            "priority": self.priority,
            "style": self.style,
            "classification_markers": self.classification_markers,
            "patterns": self.patterns,
        }


@dataclass
class ParsedSignal:
    """
    Standardized trading signal or position update result.
    Fully compatible with Golden Set QA gates and OrderIntent generation.
    """
    action: str                        # "BUY", "SELL", "MODIFY", "CLOSE", "CANCEL"
    symbol: str                        # Canonical symbol, e.g. "XAUUSD"
    order_type: str                    # "MARKET", "LIMIT", "STOP", "UPDATE"
    entry_price: Optional[float] = None
    entry_range: Optional[Tuple[float, float]] = None
    stop_loss: Optional[float] = None
    take_profits: List[float] = field(default_factory=list)
    is_modification: bool = False
    partial_close_pct: Optional[float] = None
    move_to_be: bool = False
    full_close: bool = False
    cancel_pending: bool = False
    message_type: str = "signal"       # "signal", "update", "chatter"
    format_pack_id: Optional[str] = None
    confidence: float = 1.0            # Normalized confidence 0.0 to 1.0
    confidence_score: int = 100        # Extraction completeness score 0 to 100
    geometry_valid: bool = True        # True if SL and TPs follow market geometry
    skip_reason: Optional[str] = None  # None if valid; "SKIP_GEOMETRY" if invalid
    raw_text: str = ""
    cleaned_text: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Dictionary representation matching Golden Set QA gate fields."""
        return {
            "action": self.action,
            "symbol": self.symbol,
            "order_type": self.order_type,
            "entry_price": self.entry_price,
            "entry_range": list(self.entry_range) if self.entry_range else None,
            "stop_loss": self.stop_loss,
            "take_profits": self.take_profits,
            "is_modification": self.is_modification,
            "partial_close_pct": self.partial_close_pct,
            "move_to_be": self.move_to_be,
            "full_close": self.full_close,
            "cancel_pending": self.cancel_pending,
            "message_type": self.message_type,
            "format_pack_id": self.format_pack_id,
            "confidence": self.confidence,
            "confidence_score": self.confidence_score,
            "geometry_valid": self.geometry_valid,
            "skip_reason": self.skip_reason,
            "raw_text": self.raw_text,
            "cleaned_text": self.cleaned_text,
        }

    def to_order_intent(
        self,
        volume: float = 0.01,
        slippage_cap_pips: float = 2.0,
        magic_number: int = 123456,
        signal_id: int = 0,
        order_ticket: int = 0,
    ) -> Optional[OrderIntent]:
        """
        Translates parsed signal fields into the strict 128-byte OrderIntent struct
        for IPC Shared Memory dispatch to MetaTrader terminals.
        """
        if self.message_type == MessageType.CHATTER.value:
            return None

        # Determine side
        side = OrderSide.BUY if self.action == "BUY" else OrderSide.SELL

        # Determine action type
        if self.is_modification:
            if self.full_close:
                action_type = ActionType.FULL_CLOSE
            elif self.cancel_pending:
                action_type = ActionType.CANCEL_PENDING
            elif self.partial_close_pct is not None:
                action_type = ActionType.PARTIAL_CLOSE
                volume = max(0.01, round(volume * (self.partial_close_pct / 100.0), 2))
            elif self.move_to_be:
                action_type = ActionType.MODIFY_SL_TP
                price = self.entry_price or 0.0
            else:
                action_type = ActionType.MODIFY_SL_TP
        else:
            if self.order_type == "LIMIT":
                action_type = ActionType.LIMIT_ENTRY
            elif self.order_type == "STOP":
                action_type = ActionType.STOP_ENTRY
            else:
                action_type = ActionType.MARKET_ENTRY

        # Primary entry price & targets
        price = self.entry_price or 0.0
        sl = self.stop_loss or 0.0
        tp = self.take_profits[0] if self.take_profits else 0.0

        return OrderIntent(
            symbol=self.symbol,
            side=side,
            action_type=action_type,
            volume=volume,
            price=price,
            stop_loss=sl,
            take_profit=tp,
            slippage_cap_pips=slippage_cap_pips,
            magic_number=magic_number,
            order_ticket=order_ticket,
            signal_id=signal_id,
        )


# =============================================================================
# SIGNAL PARSER CLASS
# =============================================================================

class SignalParser:
    """
    Robust trading signal parser with updateable JSON format pack grammars.
    Accepts raw or cleaned strings from TextNormalizer, classifies intent,
    and reliably extracts structured OrderIntent parameters.
    """

    def __init__(
        self,
        format_packs_dir: Optional[Union[str, Path]] = None,
        max_workers: Optional[int] = None,
    ) -> None:
        self.format_packs: Dict[str, FormatPack] = {}
        self._init_format_packs(format_packs_dir)
        self._max_workers: int = max_workers or min(32, (os.cpu_count() or 1) + 4)
        self._executor: Optional[concurrent.futures.ThreadPoolExecutor] = None

    @property
    def executor(self) -> concurrent.futures.ThreadPoolExecutor:
        """
        Sprint 21: Dedicated ThreadPoolExecutor for offloading CPU-intensive
        regex parsing off the asyncio event loop during burst events.
        """
        if self._executor is None:
            self._executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=self._max_workers,
                thread_name_prefix="PipvaneParserPool",
            )
        return self._executor

    def _init_format_packs(self, packs_dir: Optional[Union[str, Path]]) -> None:
        """Loads format packs from directory or embedded defaults."""
        if packs_dir is None:
            packs_dir = Path(__file__).parent / "format_packs"

        packs_path = Path(packs_dir)
        if packs_path.exists() and packs_path.is_dir():
            for p in sorted(packs_path.glob("*.json")):
                try:
                    pack = FormatPack.from_json(p)
                    self.register_format_pack(pack)
                except Exception as e:
                    logger.warning("Failed to load format pack %s: %s", p, e)

        # If no packs were loaded from disk, ensure fallback baseline packs exist
        if not self.format_packs:
            self._register_fallback_packs()

    def register_format_pack(self, pack: FormatPack) -> None:
        """Registers or updates a format pack in the active engine registry."""
        self.format_packs[pack.pack_id] = pack
        logger.debug("Registered format pack: %s (priority=%d)", pack.pack_id, pack.priority)

    def load_format_pack(self, pack_source: Union[str, Dict[str, Any], Path]) -> FormatPack:
        """Loads and registers a format pack from JSON file, string, or dictionary."""
        if isinstance(pack_source, dict):
            pack = FormatPack.from_dict(pack_source)
        else:
            pack = FormatPack.from_json(pack_source)
        self.register_format_pack(pack)
        return pack

    def get_format_pack(self, pack_id: str) -> Optional[FormatPack]:
        """Retrieves a registered format pack by ID."""
        return self.format_packs.get(pack_id)

    def list_format_packs(self) -> List[FormatPack]:
        """Returns all registered format packs sorted by priority (highest first)."""
        return sorted(self.format_packs.values(), key=lambda p: p.priority, reverse=True)

    # -------------------------------------------------------------------------
    # CLASSIFICATION ENGINE
    # -------------------------------------------------------------------------

    def classify(self, text: str) -> MessageType:
        """
        Classifies incoming message into 'signal', 'update', or 'chatter'.
        Evaluates presence of actionable trade directives, position modifiers,
        symbols, and conversational/marketing chatter indicators.
        """
        cleaned = clean_text(text)

        # 1. Check for explicit position update markers
        is_update = bool(re.search(
            r"\b(MODIFY|UPDATE|تعديل|SL\s+(?:TO\s+)?BE|MOVE\s+SL|BREAKEVEN|RISK\s+FREE|"
            r"CLOSE\s*(?:[0-9]+%|ALL|NOW|HALF|FULL)|FULL\s+CLOSE|EXIT\s+(?:ALL|NOW)|SECURE|حجز|تأمين|TP\d?\s+HIT|"
            r"CANCEL\s+(?:ORDER|PENDING)|DELETE\s+ORDER|CANCEL\b|إلغاء)\b",
            cleaned,
            re.IGNORECASE,
        ))
        if is_update:
            return MessageType.UPDATE

        # 2. Check for trade action directive (BUY, SELL, LIMIT, STOP, etc.)
        has_action = bool(re.search(
            r"\b(BUY|SELL|LONG|SHORT|LIMIT|STOP|شراء|بيع|صفقة)\b|\[DIR:(BUY|SELL)\]",
            cleaned,
            re.IGNORECASE,
        ))

        # 3. Check for recognizable symbol
        has_symbol = bool(self._extract_symbol(cleaned) != "UNKNOWN")

        # 4. Check for pricing parameters (SL, TP, Entry, numbers)
        has_price_levels = bool(re.search(
            r"(?:SL|TP|ENTRY|TARGET|دخول|وقف|هدف|@|at)[:\s]*[0-9]+|\b[0-9]{3,5}\.?[0-9]*\b",
            cleaned,
            re.IGNORECASE,
        ))

        if has_symbol and has_action and has_price_levels:
            return MessageType.SIGNAL

        if has_action and has_price_levels:
            return MessageType.SIGNAL

        # Check explicit chatter / announcement indicators
        is_chatter = bool(re.search(
            r"(?:DISCLAIMER|RISK\s+WARNING|JOIN\s+VIP|VIP\s+CHANNEL|PROMO|SUBSCRIBE|"
            r"CONGRATULATIONS|FEEDBACK|PROFIT\s+REPORT|GOOD\s+MORNING|WEEKEND|HTTPS?://|T\.ME/)",
            cleaned,
            re.IGNORECASE,
        ))

        if is_chatter or not (has_action or has_symbol):
            return MessageType.CHATTER

        return MessageType.CHATTER

    # -------------------------------------------------------------------------
    # GEOMETRY VALIDATION & CONFIDENCE SCORING
    # -------------------------------------------------------------------------

    def validate_geometry(
        self,
        signal: ParsedSignal,
        raise_exception: bool = False,
    ) -> Tuple[bool, Optional[str]]:
        """
        Validates extracted numerical values against market order geometry:
        1. BUY: Stop Loss must be strictly below Entry. Take profits must be strictly above Entry.
           Take profit ladders must be strictly ascending (TP1 < TP2 < ...).
        2. SELL: Stop Loss must be strictly above Entry. Take profits must be strictly below Entry.
           Take profit ladders must be strictly descending (TP1 > TP2 > ...).
        3. If Entry is omitted but both SL and TP are provided:
           BUY requires TP > SL; SELL requires TP < SL.
        
        If validation fails:
        - Sets signal.geometry_valid = False
        - Sets signal.skip_reason = SKIP_GEOMETRY
        - If raise_exception=True, raises GeometryValidationError with reason code SKIP_GEOMETRY.
        """
        # Updates, modifications, and chatter are not subject to standard entry geometry
        if signal.is_modification or signal.message_type != MessageType.SIGNAL.value:
            signal.geometry_valid = True
            signal.skip_reason = None
            return True, None

        action = signal.action
        entry = signal.entry_price
        sl = signal.stop_loss
        tps = signal.take_profits

        reason: Optional[str] = None

        if action == "BUY":
            # 1. Stop loss must be strictly below entry
            if entry is not None and sl is not None:
                if sl >= entry:
                    reason = f"BUY Stop Loss ({sl}) must be strictly below Entry price ({entry})"

            # 2. Take profits must be strictly above entry
            if not reason and entry is not None and tps:
                for idx, tp in enumerate(tps):
                    if tp <= entry:
                        reason = f"BUY Take Profit {idx + 1} ({tp}) must be strictly above Entry price ({entry})"
                        break

            # 3. Take profits must be strictly above stop loss
            if not reason and sl is not None and tps:
                for idx, tp in enumerate(tps):
                    if tp <= sl:
                        reason = f"BUY Take Profit {idx + 1} ({tp}) must be strictly above Stop Loss ({sl})"
                        break

            # 4. Take profit ladder must be strictly ascending
            if not reason and len(tps) > 1:
                for idx in range(1, len(tps)):
                    if tps[idx] <= tps[idx - 1]:
                        reason = (
                            f"BUY Take Profit ladder must be ascending: "
                            f"TP{idx + 1} ({tps[idx]}) <= TP{idx} ({tps[idx - 1]})"
                        )
                        break

        elif action == "SELL":
            # 1. Stop loss must be strictly above entry
            if entry is not None and sl is not None:
                if sl <= entry:
                    reason = f"SELL Stop Loss ({sl}) must be strictly above Entry price ({entry})"

            # 2. Take profits must be strictly below entry
            if not reason and entry is not None and tps:
                for idx, tp in enumerate(tps):
                    if tp >= entry:
                        reason = f"SELL Take Profit {idx + 1} ({tp}) must be strictly below Entry price ({entry})"
                        break

            # 3. Take profits must be strictly below stop loss
            if not reason and sl is not None and tps:
                for idx, tp in enumerate(tps):
                    if tp >= sl:
                        reason = f"SELL Take Profit {idx + 1} ({tp}) must be strictly below Stop Loss ({sl})"
                        break

            # 4. Take profit ladder must be strictly descending
            if not reason and len(tps) > 1:
                for idx in range(1, len(tps)):
                    if tps[idx] >= tps[idx - 1]:
                        reason = (
                            f"SELL Take Profit ladder must be descending: "
                            f"TP{idx + 1} ({tps[idx]}) >= TP{idx} ({tps[idx - 1]})"
                        )
                        break

        if reason:
            signal.geometry_valid = False
            signal.skip_reason = SKIP_GEOMETRY
            full_msg = f"{SKIP_GEOMETRY}: {reason}"
            if raise_exception:
                raise GeometryValidationError(reason, reason_code=SKIP_GEOMETRY)
            return False, full_msg

        signal.geometry_valid = True
        signal.skip_reason = None
        return True, None

    def calculate_confidence(self, signal: ParsedSignal) -> int:
        """
        Calculates an extraction completeness confidence score (0 to 100):
        - Missing Stop Loss drops score by 30
        - Missing Take Profit drops score by 10
        - Missing Entry Price drops score by 15 (or 25 for pending orders)
        - Unknown Symbol drops score by 50
        - Failed Geometry drops score by 50
        - Chatter drops score to 0..10
        """
        if signal.message_type == MessageType.CHATTER.value:
            signal.confidence_score = 10 if signal.symbol != "UNKNOWN" else 0
            signal.confidence = round(signal.confidence_score / 100.0, 2)
            return signal.confidence_score

        if signal.is_modification:
            score = 100
            if signal.symbol == "UNKNOWN":
                score -= 40
            # Check for at least one actionable modification instruction
            has_mod_param = bool(
                signal.partial_close_pct is not None
                or signal.move_to_be
                or signal.full_close
                or signal.cancel_pending
                or signal.stop_loss is not None
            )
            if not has_mod_param:
                score -= 30
            score = max(0, min(100, score))
            signal.confidence_score = score
            signal.confidence = round(score / 100.0, 2)
            return score

        # Standard Signal Scoring
        score = 100

        # Symbol check
        if signal.symbol == "UNKNOWN":
            score -= 50

        # Action check
        if signal.action not in ("BUY", "SELL"):
            score -= 40

        # Stop loss check
        if signal.stop_loss is None and not signal.move_to_be:
            score -= 30

        # Take profit check
        if not signal.take_profits:
            score -= 10

        # Entry price check
        if signal.entry_price is None and signal.entry_range is None:
            if signal.order_type in ("LIMIT", "STOP"):
                score -= 25
            else:
                score -= 15

        # Geometry penalty
        if not signal.geometry_valid:
            score -= 50

        score = max(0, min(100, score))
        signal.confidence_score = score
        signal.confidence = round(score / 100.0, 2)
        return score

    # -------------------------------------------------------------------------
    # EXTRACTION ENGINE
    # -------------------------------------------------------------------------

    def parse(
        self,
        raw_text: str,
        pack_id: Optional[str] = None,
        enforce_geometry: bool = False,
    ) -> ParsedSignal:
        """
        Full parsing pipeline:
        1. Normalizes raw multi-lingual text using TextNormalizer.
        2. Classifies message type (signal, update, chatter).
        3. Selects optimal format pack.
        4. Extracts symbol, side, order_type, entry (single or range), SL, TPs, and modifiers.
        5. Validates price geometry (throws GeometryValidationError if enforce_geometry=True).
        6. Calculates completeness confidence score (0 to 100).
        """
        cleaned = clean_text(raw_text)
        msg_type = self.classify(cleaned)

        # Select format pack
        pack: Optional[FormatPack] = None
        if pack_id and pack_id in self.format_packs:
            pack = self.format_packs[pack_id]
        else:
            pack = self._select_format_pack(cleaned, msg_type)

        # If classified as pure chatter without any trading directives, return early
        if msg_type == MessageType.CHATTER:
            symbol = self._extract_symbol(cleaned)
            parsed_chatter = ParsedSignal(
                action="UNKNOWN",
                symbol=symbol,
                order_type="UNKNOWN",
                message_type=MessageType.CHATTER.value,
                confidence=0.0,
                confidence_score=0,
                geometry_valid=True,
                raw_text=raw_text,
                cleaned_text=cleaned,
                format_pack_id=pack.pack_id if pack else None,
            )
            self.calculate_confidence(parsed_chatter)
            return parsed_chatter

        # Extract Fields
        symbol = self._extract_symbol(cleaned, pack)
        action, is_modification = self._extract_action(cleaned, pack, msg_type)
        order_type = self._extract_order_type(cleaned, pack, is_modification)
        entry_price, entry_range = self._extract_entry(cleaned, pack, is_modification, symbol)
        stop_loss, sl_to_be = self._extract_stop_loss(cleaned, pack)
        take_profits = self._extract_take_profits(cleaned, pack)
        modifiers = self._extract_modifiers(cleaned, pack)

        # Consolidate breakeven flags
        move_to_be = modifiers.get("breakeven", False) or sl_to_be

        parsed = ParsedSignal(
            action=action,
            symbol=symbol,
            order_type=order_type,
            entry_price=entry_price,
            entry_range=entry_range,
            stop_loss=stop_loss,
            take_profits=take_profits,
            is_modification=is_modification,
            partial_close_pct=modifiers.get("partial_close"),
            move_to_be=move_to_be,
            full_close=modifiers.get("full_close", False),
            cancel_pending=modifiers.get("cancel_pending", False),
            message_type=msg_type.value,
            format_pack_id=pack.pack_id if pack else None,
            confidence=1.0,
            confidence_score=100,
            geometry_valid=True,
            skip_reason=None,
            raw_text=raw_text,
            cleaned_text=cleaned,
        )

        # Validate order geometry
        self.validate_geometry(parsed, raise_exception=enforce_geometry)

        # Calculate completeness confidence score
        self.calculate_confidence(parsed)

        return parsed

    # -------------------------------------------------------------------------
    # ASYNC THREAD POOL PIPELINE (Sprint 21 Burst Queue Optimization)
    # -------------------------------------------------------------------------

    async def parse_async(
        self,
        raw_text: str,
        pack_id: Optional[str] = None,
        enforce_geometry: bool = False,
    ) -> ParsedSignal:
        """
        Sprint 21 Directive: Asynchronously parses raw text in the worker thread pool.
        Guarantees that parsing a heavy text block will not block the asyncio event loop
        or impede concurrent IPC dispatch operations.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self.executor,
            self.parse,
            raw_text,
            pack_id,
            enforce_geometry,
        )

    async def parse_batch_async(
        self,
        raw_texts: Sequence[str],
        pack_id: Optional[str] = None,
        enforce_geometry: bool = False,
    ) -> List[ParsedSignal]:
        """
        Sprint 21 Directive: Concurrently parses a batch of raw texts across the
        worker thread pool, returning all results asynchronously.
        """
        tasks = [
            self.parse_async(t, pack_id=pack_id, enforce_geometry=enforce_geometry)
            for t in raw_texts
        ]
        return await asyncio.gather(*tasks)

    async def classify_async(self, text: str) -> MessageType:
        """
        Sprint 21 Directive: Asynchronously classifies incoming text intent
        in the worker thread pool.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.executor, self.classify, text)

    def shutdown(self, wait: bool = False) -> None:
        """Shuts down the thread pool worker executor."""
        if self._executor is not None:
            self._executor.shutdown(wait=wait)
            self._executor = None

    def close(self) -> None:
        """Closes resources and shuts down background worker pool."""
        self.shutdown(wait=False)

    def __enter__(self) -> SignalParser:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    # -------------------------------------------------------------------------
    # INTERNAL FIELD EXTRACTORS
    # -------------------------------------------------------------------------

    def _select_format_pack(self, cleaned: str, msg_type: MessageType) -> FormatPack:
        """Finds best matching format pack based on classification markers and priority."""
        packs = self.list_format_packs()
        for p in packs:
            markers = p.classification_markers
            if msg_type == MessageType.UPDATE and "update" in markers:
                for pat in markers["update"]:
                    if re.search(pat, cleaned, re.IGNORECASE):
                        return p
            elif msg_type == MessageType.SIGNAL and "signal" in markers:
                for pat in markers["signal"]:
                    if re.search(pat, cleaned, re.IGNORECASE):
                        return p

        # Default fallback to standard_block or first registered pack
        return self.format_packs.get("standard_block", packs[0])

    def _extract_symbol(self, text: str, pack: Optional[FormatPack] = None) -> str:
        """Extracts and normalizes currency/asset symbol."""
        # 1. Direct search across all known symbols and aliases (sorted by length descending)
        for sym in sorted(ALL_KNOWN_SYMBOLS, key=len, reverse=True):
            if re.search(rf"\b{re.escape(sym)}\b", text, re.IGNORECASE):
                upper_sym = sym.upper()
                return SYMBOL_ALIASES.get(upper_sym, upper_sym)

        # 2. Try pack patterns if available, skipping reserved words
        if pack and pack._compiled_symbol:
            for p in pack._compiled_symbol:
                for m in p.finditer(text):
                    candidate = m.group(1).replace("/", "").upper()
                    if candidate not in RESERVED_WORDS:
                        return SYMBOL_ALIASES.get(candidate, candidate)

        # 3. Currency pair pattern: 3 letters + optional slash + 3 letters
        m = re.search(r"\b([A-Z]{3})/([A-Z]{3})\b", text, re.IGNORECASE)
        if m:
            pair = f"{m.group(1)}{m.group(2)}".upper()
            if pair not in RESERVED_WORDS:
                return SYMBOL_ALIASES.get(pair, pair)

        return "UNKNOWN"

    def _extract_action(
        self,
        text: str,
        pack: Optional[FormatPack],
        msg_type: MessageType,
    ) -> Tuple[str, bool]:
        """Returns (action: 'BUY'|'SELL'|'MODIFY'|'CLOSE'|'CANCEL', is_modification: bool)."""
        if re.search(r"\b(CANCEL\s+ORDER|CANCEL\s+PENDING|DELETE\s+ORDER|CANCEL|إلغاء)\b", text, re.IGNORECASE):
            return "CANCEL", True

        if re.search(r"\b(CLOSE|EXIT|إغلاق)\b", text, re.IGNORECASE) and not re.search(r"\bMODIFY\b", text, re.IGNORECASE):
            return "CLOSE", True

        if msg_type == MessageType.UPDATE or re.search(r"\b(MODIFY|UPDATE|تعديل)\b", text, re.IGNORECASE):
            return "MODIFY", True

        # Check side patterns
        if re.search(r"\b(SELL|SHORT|بيع)\b|\[DIR:SELL\]", text, re.IGNORECASE):
            return "SELL", False

        if re.search(r"\b(BUY|LONG|شراء)\b|\[DIR:BUY\]", text, re.IGNORECASE):
            return "BUY", False

        return "BUY", False

    def _extract_order_type(
        self,
        text: str,
        pack: Optional[FormatPack],
        is_modification: bool,
    ) -> str:
        """Determines 'MARKET', 'LIMIT', 'STOP', or 'UPDATE'."""
        if is_modification:
            return "UPDATE"

        # Check LIMIT
        if re.search(r"\b(LIMIT|BUY\s+LIMIT|SELL\s+LIMIT)\b", text, re.IGNORECASE):
            return "LIMIT"

        # Check STOP (excluding STOP LOSS / SL)
        if re.search(r"\b(BUY\s+STOP|SELL\s+STOP)\b", text, re.IGNORECASE):
            return "STOP"

        if re.search(r"\bSTOP\b", text, re.IGNORECASE) and not re.search(
            r"\b(STOP[\s_-]?LOSS|SL|وقف)\b", text, re.IGNORECASE
        ):
            return "STOP"

        return "MARKET"

    def _extract_entry(
        self,
        text: str,
        pack: Optional[FormatPack],
        is_modification: bool,
        symbol: str,
    ) -> Tuple[Optional[float], Optional[Tuple[float, float]]]:
        """Extracts entry price and/or price range."""
        if is_modification:
            return None, None

        # 1. Check for price range pattern: e.g. "2650.00 - 2652.00"
        range_match = re.search(
            r"(?:@|at|entry|zone|دخول|سعر\s+الدخول)?[:\s]*([0-9]+\.?[0-9]*)\s*-\s*([0-9]+\.?[0-9]*)",
            text,
            re.IGNORECASE,
        )
        if range_match:
            try:
                p1 = float(range_match.group(1))
                p2 = float(range_match.group(2))
                low, high = min(p1, p2), max(p1, p2)
                return low, (low, high)
            except ValueError:
                pass

        # 2. Check for labeled entry: "@ 2650.50", "Entry: 2650.50"
        entry_match = re.search(
            r"(?:@|at|entry[:\s]*|price[:\s]*|دخول[:\s]*|سعر\s+الدخول[:\s]*)\s*([0-9]+\.?[0-9]*)",
            text,
            re.IGNORECASE,
        )
        if entry_match:
            try:
                return float(entry_match.group(1)), None
            except ValueError:
                pass

        # 3. Pack specific entry patterns
        if pack and pack._compiled_entry:
            for p in pack._compiled_entry:
                m = p.search(text)
                if m:
                    try:
                        return float(m.group(1)), None
                    except (ValueError, IndexError):
                        pass

        # 4. Standalone price adjacent to action/symbol (e.g. "BUY 2650", "BUY XAUUSD 2650")
        words = text.split()
        for i, w in enumerate(words):
            if w.upper().strip(":,") in [symbol, "BUY", "SELL", "LIMIT", "STOP", "[DIR:BUY]", "[DIR:SELL]"]:
                for j in range(i + 1, min(i + 4, len(words))):
                    curr_word = words[j].upper().strip(":,")
                    if curr_word in ["SL", "TP", "TARGET", "STOP", "LOSS", "STOPLOSS", "TAKEPROFIT", "CLOSE", "MODIFY"]:
                        break
                    if curr_word.startswith("SL") or curr_word.startswith("TP") or curr_word.startswith("TARGET"):
                        break
                    clean_val = re.sub(r"[^\d.]", "", words[j])
                    if clean_val and self._is_valid_float(clean_val):
                        prev_word = words[j - 1].upper().strip(":,") if j > 0 else ""
                        if prev_word not in ["SL", "TP", "TARGET", "STOP", "LOSS", "STOPLOSS", "TAKEPROFIT"]:
                            return float(clean_val), None

        return None, None

    def _extract_stop_loss(
        self,
        text: str,
        pack: Optional[FormatPack],
    ) -> Tuple[Optional[float], bool]:
        """Returns (stop_loss: Optional[float], is_breakeven: bool)."""
        # Check Breakeven indicators
        if re.search(
            r"\b(SL\s+TO\s+(?:BE|ENTRY|BREAKEVEN)|MOVE\s+SL\s+TO\s+(?:BE|ENTRY|BREAKEVEN)|"
            r"SL\s+BE|BREAKEVEN|SL\s+TO\s+ENTRY|SECURE\s+AT\s+ENTRY)\b",
            text,
            re.IGNORECASE,
        ):
            return None, True

        # Check explicit SL pattern: e.g. "SL: 2640.00", "SL TO 195.20"
        sl_match = re.search(
            r"(?:SL|STOP[\s_-]?LOSS|وقف\s*الخسارة|الوقف)[:\s]*([TOto\s]*)([0-9]+\.?[0-9]*)",
            text,
            re.IGNORECASE,
        )
        if sl_match:
            val_str = sl_match.group(2)
            if val_str and self._is_valid_float(val_str):
                return float(val_str), False

        return None, False

    def _extract_take_profits(
        self,
        text: str,
        pack: Optional[FormatPack],
    ) -> List[float]:
        """Extracts take profit ladder (supports up to 10 unique TPs in sequential order)."""
        take_profits: List[float] = []

        # Find all TP labeled levels: "TP1: 2670", "TP: 2670", "Target 2: 2680", "هدف أول: 2670"
        tp_matches = re.finditer(
            r"(?:TP\d+|TAKE[\s_-]?PROFIT\d+|TARGET\d+|TP|TAKE[\s_-]?PROFIT|TARGET|هدف\s*(?:أول|ثان|ثالث|رابع|خامس|سادس|سابع|ثامن|تاسع|عاشر|\d+)?|أهداف)[:\s]*([0-9]+\.?[0-9]*)",
            text,
            re.IGNORECASE,
        )
        for m in tp_matches:
            val_str = m.group(1)
            if val_str and self._is_valid_float(val_str):
                val = float(val_str)
                if val not in take_profits:
                    take_profits.append(val)
                    if len(take_profits) >= 10:
                        break

        return take_profits

    def _extract_modifiers(
        self,
        text: str,
        pack: Optional[FormatPack],
    ) -> Dict[str, Any]:
        """Extracts risk and position modifiers."""
        res: Dict[str, Any] = {
            "partial_close": None,
            "breakeven": False,
            "full_close": False,
            "cancel_pending": False,
        }

        # Partial close % (e.g., "CLOSE 50%", "Close half", "SECURE 33%")
        close_pct_match = re.search(
            r"(?:CLOSE|SECURE|BOOK|LOCK|TAKE|إغلاق|حجز)\s*([0-9]+(?:\.[0-9]+)?)\s*%",
            text,
            re.IGNORECASE,
        )
        if close_pct_match:
            try:
                res["partial_close"] = float(close_pct_match.group(1))
            except ValueError:
                pass
        elif re.search(r"\b(CLOSE\s+HALF|SECURE\s+HALF|HALF\s+CLOSE|نصف\s+العقد)\b", text, re.IGNORECASE):
            res["partial_close"] = 50.0

        # Breakeven / Move SL to Entry
        if re.search(
            r"\b(MOVE\s+SL\s+TO\s+(?:BE|ENTRY|BREAKEVEN)|SL\s+TO\s+(?:BE|ENTRY|BREAKEVEN)|"
            r"SL\s+BE|BREAKEVEN|RISK\s+FREE|SECURE\s+AT\s+ENTRY|نقل\s+الوقف|تأمين)\b",
            text,
            re.IGNORECASE,
        ):
            res["breakeven"] = True

        # Full close
        if re.search(
            r"\b(CLOSE\s+ALL|CLOSE\s+NOW|FULL\s+CLOSE|EXIT\s+NOW|EXIT\s+ALL|إغلاق\s+كلي|خروج\s+الآن)\b",
            text,
            re.IGNORECASE,
        ):
            res["full_close"] = True

        # Cancel pending
        if re.search(
            r"\b(CANCEL\s+ORDER|CANCEL\s+PENDING|DELETE\s+PENDING|CANCEL|إلغاء\s+الأمر)\b",
            text,
            re.IGNORECASE,
        ):
            res["cancel_pending"] = True

        return res

    @staticmethod
    def _is_valid_float(val: str) -> bool:
        try:
            float(val)
            return True
        except ValueError:
            return False

    def _register_fallback_packs(self) -> None:
        """Embedded fallback baseline format packs in case JSON files are not accessible."""
        fallback = FormatPack(
            pack_id="standard_block",
            name="Standard Block Style",
            description="Fallback standard block style",
            version="1.0.0",
            priority=10,
            style="standard_block",
            classification_markers={
                "signal": [r"\b(BUY|SELL)\b", r"\b(SL|TP)\b"],
                "update": [r"\b(MODIFY|UPDATE|CLOSE)\b"],
            },
            patterns={
                "symbol": [r"\b([A-Z]{3}/?[A-Z]{3}|GOLD|XAUUSD|US30|NAS100)\b"],
                "side": [r"\b(BUY|LONG)\b", r"\b(SELL|SHORT)\b"],
            },
        )
        self.register_format_pack(fallback)
