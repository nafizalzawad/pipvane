"""
Pipvane v3 Deterministic Text Normalizer (ADR-002).

Executes a 7-stage pure functional pre-processing pipeline standardizing:
1. Stage 1: NFKC Unicode Compatibility Decomposition
2. Stage 2: Invisible & Zero-Width Formatting Artifact Stripping (ZWSP, ZWNJ, BOM, etc.)
3. Stage 3: Numeric Harmonization (Arabic-Indic, Persian, Devanagari, Bengali -> ASCII 0-9)
4. Stage 4: Homoglyph Normalization (Cyrillic & Greek lookalikes -> Latin ASCII)
5. Stage 5: Directional & Domain Trading Emoji Translation / Tokenization (🟢 -> "BUY", 🔴 -> "SELL", 🎯 -> "TP", 🛑 -> "SL")
6. Stage 6: Decimal Separator & Range Punctuation Disambiguation (1,0850 -> 1.0850)
7. Stage 7: Whitespace Normalization & Semantic Linebreak Preservation

Pure, stateless, zero-database execution designed for sub-5ms latency budgets.
"""

from __future__ import annotations

import re
import time
import unicodedata
from dataclasses import dataclass
from typing import Dict, Optional, Tuple, Set


# =============================================================================
# STAGE 2: INVISIBLE & ZERO-WIDTH CODEPOINTS (ADR-002 Section 4.2)
# =============================================================================

_INVISIBLE_CODEPOINTS: Set[int] = {
    0x200B,  # ZERO WIDTH SPACE (ZWSP)
    0x200C,  # ZERO WIDTH NON-JOINER (ZWNJ)
    0x200D,  # ZERO WIDTH JOINER (ZWJ)
    0x200E,  # LEFT-TO-RIGHT MARK (LRM)
    0x200F,  # RIGHT-TO-LEFT MARK (RLM)
    0x202A,  # LEFT-TO-RIGHT EMBEDDING (LRE)
    0x202B,  # RIGHT-TO-LEFT EMBEDDING (RLE)
    0x202C,  # POP DIRECTIONAL FORMATTING (PDF)
    0x202D,  # LEFT-TO-RIGHT OVERRIDE (LRO)
    0x202E,  # RIGHT-TO-LEFT OVERRIDE (RLO)
    0x2060,  # WORD JOINER (WJ)
    0x2066,  # LEFT-TO-RIGHT ISOLATE (LRI)
    0x2067,  # RIGHT-TO-LEFT ISOLATE (RLI)
    0x2068,  # FIRST STRONG ISOLATE (FSI)
    0x2069,  # POP DIRECTIONAL ISOLATE (PDI)
    0xFEFF,  # ZERO WIDTH NO-BREAK SPACE / BOM
    0x00AD,  # SOFT HYPHEN (SHY)
}

# Variation Selectors: U+FE00 through U+FE0F
for _cp in range(0xFE00, 0xFE10):
    _INVISIBLE_CODEPOINTS.add(_cp)

_INVISIBLE_TRANS_TABLE: Dict[int, None] = {cp: None for cp in _INVISIBLE_CODEPOINTS}


# =============================================================================
# STAGE 3: NUMERIC HARMONIZATION TABLES (ADR-002 Section 4.3)
# =============================================================================

# Standard Arabic-Indic: ٠-٩ (U+0660 - U+0669)
# Persian / Eastern Arabic-Indic: ۰-۹ (U+06F0 - U+06F9)
# Devanagari: ०-९ (U+0966 - U+096F)
# Bengali: ০-৯ (U+09E6 - U+09EF)
_DIGIT_MAPPINGS: Dict[int, str] = {}
for i in range(10):
    _DIGIT_MAPPINGS[0x0660 + i] = str(i)  # Arabic-Indic
    _DIGIT_MAPPINGS[0x06F0 + i] = str(i)  # Persian
    _DIGIT_MAPPINGS[0x0966 + i] = str(i)  # Devanagari
    _DIGIT_MAPPINGS[0x09E6 + i] = str(i)  # Bengali

_DIGIT_TRANS_TABLE: Dict[int, str] = dict(_DIGIT_MAPPINGS)


# =============================================================================
# STAGE 4: HOMOGLYPH NORMALIZATION TABLES (ADR-002 Section 4.4)
# =============================================================================

_HOMOGLYPH_MAPPINGS: Dict[str, str] = {
    # Cyrillic Lowercase
    "\u0430": "a",  # Cyrillic Small Letter A
    "\u0432": "v",  # Cyrillic Small Letter Ve
    "\u0435": "e",  # Cyrillic Small Letter Ie
    "\u0455": "s",  # Cyrillic Small Letter Dze
    "\u0456": "i",  # Cyrillic Byelorussian-Ukrainian I
    "\u0458": "j",  # Cyrillic Small Letter Je
    "\u043a": "k",  # Cyrillic Small Letter Ka
    "\u043c": "m",  # Cyrillic Small Letter Em
    "\u043d": "n",  # Cyrillic Small Letter En
    "\u043e": "o",  # Cyrillic Small Letter O
    "\u0440": "p",  # Cyrillic Small Letter Er
    "\u0441": "s",  # Cyrillic Small Letter Es
    "\u0442": "t",  # Cyrillic Small Letter Te
    "\u0443": "y",  # Cyrillic Small Letter U
    "\u0445": "x",  # Cyrillic Small Letter Ha
    # Cyrillic Uppercase
    "\u0410": "A",  # Cyrillic Capital Letter A
    "\u0412": "B",  # Cyrillic Capital Letter Ve (В -> Latin B)
    "\u0415": "E",  # Cyrillic Capital Letter Ie
    "\u0405": "S",  # Cyrillic Capital Letter Dze
    "\u0406": "I",  # Cyrillic Capital Letter I
    "\u0408": "J",  # Cyrillic Capital Letter Je
    "\u041a": "K",  # Cyrillic Capital Letter Ka
    "\u041c": "M",  # Cyrillic Capital Letter Em
    "\u041d": "H",  # Cyrillic Capital Letter En (Н -> Latin H)
    "\u041e": "O",  # Cyrillic Capital Letter O
    "\u0420": "P",  # Cyrillic Capital Letter Er (Р -> Latin P)
    "\u0421": "C",  # Cyrillic Capital Letter Es (С -> Latin C)
    "\u0422": "T",  # Cyrillic Capital Letter Te
    "\u0423": "Y",  # Cyrillic Capital Letter U
    "\u0425": "X",  # Cyrillic Capital Letter Ha
    # Greek Capital Homoglyphs
    "\u0391": "A",  # Alpha
    "\u0392": "B",  # Beta
    "\u0395": "E",  # Epsilon
    "\u0396": "Z",  # Zeta
    "\u0397": "H",  # Eta
    "\u0399": "I",  # Iota
    "\u039a": "K",  # Kappa
    "\u039c": "M",  # Mu
    "\u039d": "N",  # Nu
    "\u039f": "O",  # Omicron
    "\u03a1": "P",  # Rho
    "\u03a4": "T",  # Tau
    "\u03a5": "Y",  # Upsilon
    "\u03a7": "X",  # Chi
}

_HOMOGLYPH_TRANS_TABLE: Dict[int, str] = {ord(k): v for k, v in _HOMOGLYPH_MAPPINGS.items()}

# Combined fast single-pass translation table for invisibles + digits + homoglyphs
_COMBINED_CHAR_TRANS_TABLE: Dict[int, Optional[str]] = {
    **_INVISIBLE_TRANS_TABLE,
    **_DIGIT_TRANS_TABLE,
    **_HOMOGLYPH_TRANS_TABLE,
}


# =============================================================================
# STAGE 5: EMOJI TRANSLATION & TOKENIZATION (ADR-002 Section 4.5)
# =============================================================================

# Text intent mapping (e.g., 🟢/📈 -> "BUY", 🔴/📉 -> "SELL", 🎯 -> "TP", 🛑 -> "SL")
_EMOJI_INTENTS: Dict[str, str] = {
    # BUY Direction
    "\U0001f7e2": " BUY ",    # 🟢 Green Circle
    "\U0001f7e9": " BUY ",    # 🟩 Green Square
    "\U0001f4c8": " BUY ",    # 📈 Chart Increasing
    "\U0001f680": " BUY ",    # 🚀 Rocket
    "\U0001f402": " BUY ",    # 🐂 Ox / Bull
    "\U0001f53c": " BUY ",    # 🔼 Up Triangle
    "\u2b06\ufe0f": " BUY ",  # ⬆️ Up Arrow (with variation selector)
    "\u2b06": " BUY ",        # ⬆ Up Arrow
    # SELL Direction
    "\U0001f534": " SELL ",   # 🔴 Red Circle
    "\U0001f7e5": " SELL ",   # 🟥 Red Square
    "\U0001f4c9": " SELL ",   # 📉 Chart Decreasing
    "\U0001fa78": " SELL ",   # 🩸 Drop of Blood
    "\U0001f43b": " SELL ",   # 🐻 Bear
    "\U0001f53d": " SELL ",   # 🔽 Down Triangle
    "\u2b07\ufe0f": " SELL ",  # ⬇️ Down Arrow (with variation selector)
    "\u2b07": " SELL ",       # ⬇ Down Arrow
    # Take Profit
    "\U0001f3af": " TP ",     # 🎯 Direct Hit / Bullseye
    "\U0001f4b0": " TP ",     # 💰 Money Bag
    "\U0001f4b5": " TP ",     # 💵 Dollar Banknote
    "\u2705": " TP ",         # ✅ Check Mark Button
    # Stop Loss
    "\U0001f6d1": " SL ",     # 🛑 Stop Sign
    "\u26d4": " SL ",         # ⛔ No Entry
    "\u274c": " SL ",         # ❌ Cross Mark
    # Entry
    "\U0001f4cd": " ENTRY ",  # 📍 Round Pushpin
    "\U0001f3f7\ufe0f": " ENTRY ",  # 🏷️ Label (with variation selector)
    "\U0001f3f7": " ENTRY ",        # 🏷 Label
}

# Delimited canonical token mapping (ADR-002 tokens: [DIR:BUY], [TOKEN:TP], etc.)
_EMOJI_TOKENS: Dict[str, str] = {
    # BUY Direction
    "\U0001f7e2": " [DIR:BUY] ",
    "\U0001f7e9": " [DIR:BUY] ",
    "\U0001f4c8": " [DIR:BUY] ",
    "\U0001f680": " [DIR:BUY] ",
    "\U0001f402": " [DIR:BUY] ",
    "\U0001f53c": " [DIR:BUY] ",
    "\u2b06\ufe0f": " [DIR:BUY] ",
    "\u2b06": " [DIR:BUY] ",
    # SELL Direction
    "\U0001f534": " [DIR:SELL] ",
    "\U0001f7e5": " [DIR:SELL] ",
    "\U0001f4c9": " [DIR:SELL] ",
    "\U0001fa78": " [DIR:SELL] ",
    "\U0001f43b": " [DIR:SELL] ",
    "\U0001f53d": " [DIR:SELL] ",
    "\u2b07\ufe0f": " [DIR:SELL] ",
    "\u2b07": " [DIR:SELL] ",
    # Take Profit
    "\U0001f3af": " [TOKEN:TP] ",
    "\U0001f4b0": " [TOKEN:TP] ",
    "\U0001f4b5": " [TOKEN:TP] ",
    "\u2705": " [TOKEN:TP] ",
    # Stop Loss
    "\U0001f6d1": " [TOKEN:SL] ",
    "\u26d4": " [TOKEN:SL] ",
    "\u274c": " [TOKEN:SL] ",
    # Entry
    "\U0001f4cd": " [TOKEN:ENTRY] ",
    "\U0001f3f7\ufe0f": " [TOKEN:ENTRY] ",
    "\U0001f3f7": " [TOKEN:ENTRY] ",
}


# =============================================================================
# STAGE 6: DECIMAL SEPARATORS & PUNCTUATION REGEXES (ADR-002 Section 4.6)
# =============================================================================

# All Unicode dashes (en-dash, em-dash, minus sign, horizontal bar, figure dash)
_DASH_REGEX = re.compile(r"[\u2010\u2012\u2013\u2014\u2015\u2212]")

# Decimal comma between digits (e.g., "1,0850" -> "1.0850", "2650,5" -> "2650.5")
_DECIMAL_COMMA_REGEX = re.compile(r"(?<=\d),(?=\d)")

# Price range hyphens between numbers (e.g., "1.0850-1.0870" -> "1.0850 - 1.0870")
_PRICE_RANGE_DASH_REGEX = re.compile(r"(?<=\d)\s*-\s*(?=\d)")


# =============================================================================
# DATA STRUCTURES
# =============================================================================

@dataclass(frozen=True)
class NormalizationOptions:
    """Configurable pipeline stages for text normalization."""
    strip_invisibles: bool = True
    harmonize_digits: bool = True
    normalize_homoglyphs: bool = True
    tokenize_emojis: bool = True
    standardize_decimals: bool = True
    preserve_linebreaks: bool = True
    emoji_style: str = "intent"  # "intent" ("BUY"/"SELL") or "token" ("[DIR:BUY]")


@dataclass(frozen=True)
class NormalizationResult:
    """Outcome and telemetry of text normalization."""
    normalized_text: str
    has_buy_directive: bool
    has_sell_directive: bool
    digits_replaced_count: int
    homoglyphs_replaced_count: int
    emojis_replaced_count: int
    latency_microseconds: float


# =============================================================================
# PURE FUNCTIONS (STATELESS, ZERO SIDE-EFFECTS, ZERO DB CALLS)
# =============================================================================

def strip_zero_width_and_invisibles(text: str) -> str:
    """
    Pure function: Strips zero-width joiners (\\u200D), zero-width spaces (\\u200B),
    non-joiners (\\u200C), BOM (\\uFEFF), direction marks, and variation selectors.
    """
    return text.translate(_INVISIBLE_TRANS_TABLE)


def normalize_digits(text: str) -> str:
    """
    Pure function: Converts Arabic-Indic (٠١٢٣٤٥٦٧٨٩), Persian (۰۱۲۳۴۵۶۷۸۹),
    Devanagari (०१२३४५६७८९), and Bengali (০১২৩৪৫৬৭৮৯) digits to ASCII 0-9.
    Fast execution via C-level translation table.
    """
    return text.translate(_DIGIT_TRANS_TABLE)


def normalize_homoglyphs(text: str) -> str:
    """
    Pure function: Maps Cyrillic (а, о, е, с, р, у, х, А, В, Е, К, М, Н, О, Р, С, Т, Х, У)
    and Greek (Α, Β, Ε, Ζ, Η, Ι, Κ, Μ, Ν, Ο, Ρ, Τ, Υ, Χ) lookalike homoglyphs
    to their exact Latin ASCII equivalents.
    """
    return text.translate(_HOMOGLYPH_TRANS_TABLE)


def translate_emojis(text: str, style: str = "intent") -> Tuple[str, int]:
    """
    Pure function: Maps directional and trading emojis to text intents or canonical tokens.
    - "intent": 🟢/📈 -> " BUY ", 🔴/📉 -> " SELL ", 🎯 -> " TP ", 🛑 -> " SL "
    - "token": 🟢/📈 -> " [DIR:BUY] ", 🔴/📉 -> " [DIR:SELL] ", 🎯 -> " [TOKEN:TP] ", 🛑 -> " [TOKEN:SL] "
    Returns (translated_text, replaced_count).
    """
    mapping = _EMOJI_TOKENS if style == "token" else _EMOJI_INTENTS
    replaced_count = 0
    res = text
    for glyph, replacement in mapping.items():
        if glyph in res:
            count = res.count(glyph)
            replaced_count += count
            res = res.replace(glyph, replacement)
    return res, replaced_count


def standardize_decimals_and_punctuation(text: str) -> str:
    """
    Pure function: Disambiguates decimal commas between digits, standardizes Unicode dashes,
    and formats price range hyphens with consistent whitespace.
    """
    # 1. Decimal comma between digits ("1,0850" -> "1.0850")
    res = _DECIMAL_COMMA_REGEX.sub(".", text)
    # 2. Standardize Unicode dashes to ASCII hyphen
    res = _DASH_REGEX.sub("-", res)
    # 3. Standardize price range hyphens between numbers ("1.0850-1.0870" -> "1.0850 - 1.0870")
    res = _PRICE_RANGE_DASH_REGEX.sub(" - ", res)
    return res


def normalize_whitespace(text: str, preserve_linebreaks: bool = True) -> str:
    """
    Pure function: Collapses consecutive horizontal whitespace, trims trailing spaces,
    and preserves semantic linebreaks while collapsing excessive blank lines (>2 to 2).
    """
    if preserve_linebreaks:
        lines = text.splitlines()
        cleaned_lines = []
        for line in lines:
            cleaned = re.sub(r"[^\S\r\n]+", " ", line).strip()
            cleaned_lines.append(cleaned)
        joined = "\n".join(cleaned_lines)
        return re.sub(r"\n{3,}", "\n\n", joined).strip()
    return re.sub(r"\s+", " ", text).strip()


def clean_text(raw_text: str, emoji_style: str = "intent") -> str:
    """
    Primary pure entry-point: Takes a raw multi-lingual string in and returns
    a fully cleaned, normalized Latin alphanumeric string out.
    Zero side-effects, zero database calls.
    """
    # Stage 1: NFKC Unicode decomposition
    t = unicodedata.normalize("NFKC", raw_text)

    # Stage 5 (Early): Translate emojis before stripping or character transforms
    t, _ = translate_emojis(t, style=emoji_style)

    # Stages 2, 3, 4: Fast single-pass C translation (Invisibles + Digits + Homoglyphs)
    t = t.translate(_COMBINED_CHAR_TRANS_TABLE)

    # Stage 6: Decimal separators & punctuation disambiguation
    t = standardize_decimals_and_punctuation(t)

    # Stage 7: Whitespace collapsing & linebreak preservation
    return normalize_whitespace(t, preserve_linebreaks=True)


# Alias for clean_text
normalize_text = clean_text


# =============================================================================
# CLASS & ENGINE INTERFACE (ADR-002)
# =============================================================================

class TextNormalizer:
    """
    Pure functional deterministic text normalizer implementing ADR-002.
    Transforms raw multi-lingual Telegram messages into standardized Latin alphanumeric text.
    Zero side-effects, zero DB calls, purely stateless.
    """

    def __call__(self, raw_text: str, emoji_style: str = "intent") -> str:
        """Enables direct instance invocation: normalizer(raw_text)."""
        return clean_text(raw_text, emoji_style=emoji_style)

    @staticmethod
    def clean(raw_text: str, emoji_style: str = "intent") -> str:
        """Pure function: takes raw string in, returns cleaned string out."""
        return clean_text(raw_text, emoji_style=emoji_style)

    @staticmethod
    def normalize_string(raw_text: str, emoji_style: str = "intent") -> str:
        """Alias for clean()."""
        return clean_text(raw_text, emoji_style=emoji_style)

    @staticmethod
    def strip_zero_width(text: str) -> str:
        """Pure function: strips invisible formatting artifacts and zero-width codepoints."""
        return strip_zero_width_and_invisibles(text)

    @staticmethod
    def normalize_digits(text: str) -> str:
        """Pure function: converts non-ASCII digits (Arabic-Indic, Persian, Devanagari) to ASCII 0-9."""
        return normalize_digits(text)

    @staticmethod
    def normalize_homoglyphs(text: str) -> str:
        """Pure function: maps Cyrillic and Greek lookalikes to Latin ASCII."""
        return normalize_homoglyphs(text)

    @staticmethod
    def translate_emojis(text: str, style: str = "intent") -> Tuple[str, int]:
        """Pure function: maps directional and trading emojis to intents or tokens."""
        return translate_emojis(text, style=style)

    @classmethod
    def normalize(
        cls,
        raw_text: str,
        options: Optional[NormalizationOptions] = None,
    ) -> NormalizationResult:
        """
        Executes the 7-stage normalization pipeline deterministically,
        returning transformed text along with directive detection and performance telemetry.
        """
        if options is None:
            options = NormalizationOptions()

        t0_ns = time.perf_counter_ns()
        text = raw_text

        # Stage 1: NFKC Compatibility Decomposition
        text = unicodedata.normalize("NFKC", text)

        # Stage 5 (Early): Emoji translation / tokenization
        emojis_count = 0
        if options.tokenize_emojis:
            text, emojis_count = translate_emojis(text, style=options.emoji_style)

        # Stage 2: Invisible & Zero-Width Formatting Artifact Stripping
        if options.strip_invisibles:
            text = strip_zero_width_and_invisibles(text)

        # Stage 3: Numeric Harmonization
        digits_count = 0
        if options.harmonize_digits:
            # Count replaced non-ASCII digits for telemetry
            digits_count = sum(1 for ch in text if ord(ch) in _DIGIT_MAPPINGS)
            text = normalize_digits(text)

        # Stage 4: Homoglyph Normalization
        homoglyphs_count = 0
        if options.normalize_homoglyphs:
            # Count replaced homoglyphs for telemetry
            homoglyphs_count = sum(1 for ch in text if ch in _HOMOGLYPH_MAPPINGS)
            text = normalize_homoglyphs(text)

        # Stage 6: Decimal Separators & Punctuation Disambiguation
        if options.standardize_decimals:
            text = standardize_decimals_and_punctuation(text)

        # Stage 7: Whitespace Normalization & Semantic Linebreak Preservation
        text = normalize_whitespace(text, preserve_linebreaks=options.preserve_linebreaks)

        # Telemetry and directive detection
        t1_ns = time.perf_counter_ns()
        latency_us = (t1_ns - t0_ns) / 1000.0

        has_buy = bool(re.search(r"\bBUY\b|\[DIR:BUY\]", text, re.IGNORECASE))
        has_sell = bool(re.search(r"\bSELL\b|\[DIR:SELL\]", text, re.IGNORECASE))

        return NormalizationResult(
            normalized_text=text,
            has_buy_directive=has_buy,
            has_sell_directive=has_sell,
            digits_replaced_count=digits_count,
            homoglyphs_replaced_count=homoglyphs_count,
            emojis_replaced_count=emojis_count,
            latency_microseconds=latency_us,
        )


def normalize_raw_text(
    raw_text: str,
    options: Optional[NormalizationOptions] = None,
) -> NormalizationResult:
    """Convenience functional wrapper for TextNormalizer.normalize()."""
    return TextNormalizer.normalize(raw_text, options)
