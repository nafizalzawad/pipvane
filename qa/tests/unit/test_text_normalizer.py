"""
Unit tests validating Pipvane v3 TextNormalizer (Sprint 2 ADR-002).

Tests:
1. Digit Normalization: Arabic-Indic, Persian, Devanagari, and Bengali digits to ASCII 0-9.
2. Homoglyph & Zero-Width Stripping: ZWJ, ZWSP, ZWNJ, BOM, and Cyrillic/Greek lookalikes to Latin.
3. Emoji Translation: Directional emojis (🟢/📈 -> BUY, 🔴/📉 -> SELL, 🎯 -> TP, 🛑 -> SL).
4. Decimal & Punctuation Disambiguation: European commas, unicode dashes, range hyphens.
5. Pure Stateless Contract: Zero DB calls, deterministic, fast (<1ms).
"""

import time
import pytest

from engine.normalizer import (
    TextNormalizer,
    clean_text,
    normalize_text,
    normalize_raw_text,
    normalize_digits,
    normalize_homoglyphs,
    strip_zero_width_and_invisibles,
    translate_emojis,
    standardize_decimals_and_punctuation,
    normalize_whitespace,
    NormalizationOptions,
    NormalizationResult,
)


# =============================================================================
# 1. DIGIT NORMALIZATION TESTS
# =============================================================================

@pytest.mark.unit
def test_digit_normalization_arabic_indic():
    """Verify standard Arabic-Indic numerals (٠١٢٣٤٥٦٧٨٩) map to ASCII 0123456789."""
    arabic_indic = "٠١٢٣٤٥٦٧٨٩"
    expected = "0123456789"
    assert normalize_digits(arabic_indic) == expected

    # Realistic signal line
    raw = "Entry: ٢٦٥٠.٥٠ | SL: ٢٦٤٠.٠٠ | TP: ٢٦٧٠.٠٠"
    cleaned = normalize_digits(raw)
    assert cleaned == "Entry: 2650.50 | SL: 2640.00 | TP: 2670.00"


@pytest.mark.unit
def test_digit_normalization_persian():
    """Verify Persian / Eastern Arabic-Indic numerals (۰۱۲۳۴۵۶۷۸۹) map to ASCII 0123456789."""
    persian = "۰۱۲۳۴۵۶۷۸۹"
    expected = "0123456789"
    assert normalize_digits(persian) == expected

    raw = "EURUSD قیمت ورود ۱.۰۸۵۰ حد ضرر ۱.۰۸۰۰"
    cleaned = normalize_digits(raw)
    assert "1.0850" in cleaned
    assert "1.0800" in cleaned


@pytest.mark.unit
def test_digit_normalization_devanagari():
    """Verify Devanagari numerals (०१२३४५६७८९) map to ASCII 0123456789."""
    devanagari = "०१२३४५६७८९"
    expected = "0123456789"
    assert normalize_digits(devanagari) == expected

    raw = "XAUUSD प्रवेश: २६५०.५० लक्ष्य: २६७०.००"
    cleaned = normalize_digits(raw)
    assert "2650.50" in cleaned
    assert "2670.00" in cleaned


@pytest.mark.unit
def test_digit_normalization_bengali():
    """Verify Bengali numerals (০১২৩৪৫৬৭৮৯) map to ASCII 0123456789."""
    bengali = "০১২৩৪৫৬৭৮৯"
    expected = "0123456789"
    assert normalize_digits(bengali) == expected

    raw = "GBPUSD ১.৩০৫০"
    cleaned = normalize_digits(raw)
    assert "1.3050" in cleaned


# =============================================================================
# 2. ZERO-WIDTH & HOMOGLYPH STRIPPING TESTS
# =============================================================================

@pytest.mark.unit
def test_zero_width_and_invisible_stripping():
    """Verify zero-width joiners, spaces, direction marks, and BOM are stripped."""
    # Construct strings with invisible characters embedded
    # \u200D: ZWJ, \u200B: ZWSP, \u200C: ZWNJ, \uFEFF: BOM, \u200E: LRM, \u200F: RLM
    raw_buy = "B\u200dU\u200bY"
    assert strip_zero_width_and_invisibles(raw_buy) == "BUY"

    raw_sell = "\ufeffS\u200ce\u200dl\u200fl"
    assert strip_zero_width_and_invisibles(raw_sell) == "Sell"

    raw_price = "2\u200b6\u200b5\u200b0"
    assert strip_zero_width_and_invisibles(raw_price) == "2650"


@pytest.mark.unit
def test_homoglyph_normalization_cyrillic():
    """Verify Cyrillic lookalikes map to their exact Latin ASCII equivalents."""
    # Cyrillic uppercase in symbol names
    raw_xau = "X\u0410UUSD"  # Cyrillic А (U+0410)
    assert normalize_homoglyphs(raw_xau) == "XAUUSD"

    raw_eur = "\u0415URUSD"  # Cyrillic Е (U+0415)
    assert normalize_homoglyphs(raw_eur) == "EURUSD"

    raw_gbp = "GB\u0420USD"  # Cyrillic Р (U+0420)
    assert normalize_homoglyphs(raw_gbp) == "GBPUSD"

    raw_buy = "\u0412UY"  # Cyrillic В (U+0412) -> B
    assert normalize_homoglyphs(raw_buy) == "BUY"

    # Cyrillic lowercase in action words
    raw_sell = "\u0441\u0435ll"  # Cyrillic с (U+0441), е (U+0435)
    assert normalize_homoglyphs(raw_sell) == "sell"

    # Full set of Cyrillic homoglyphs
    cyrillic_sample = "а в е ѕ і ј к м н о р с т у х А В Е Ѕ І Ј К М Н О Р С Т У Х"
    latin_expected = "a v e s i j k m n o p s t y x A B E S I J K M H O P C T Y X"
    assert normalize_homoglyphs(cyrillic_sample) == latin_expected


@pytest.mark.unit
def test_homoglyph_normalization_greek():
    """Verify Greek capital homoglyphs map to Latin ASCII equivalents."""
    greek_sample = "Α Β Ε Ζ Η Ι Κ Μ Ν Ο Ρ Τ Υ Χ"
    latin_expected = "A B E Z H I K M N O P T Y X"
    assert normalize_homoglyphs(greek_sample) == latin_expected

    raw_eur = "\u0395URUSD"  # Greek Epsilon (U+0395)
    assert normalize_homoglyphs(raw_eur) == "EURUSD"


# =============================================================================
# 3. EMOJI TRANSLATION TESTS
# =============================================================================

@pytest.mark.unit
def test_emoji_translation_directional_intents():
    """Verify directional emojis map to text intents (BUY, SELL, TP, SL, ENTRY)."""
    # BUY emojis
    assert "BUY" in translate_emojis("🟢 GOLD 2650", style="intent")[0]
    assert "BUY" in translate_emojis("📈 EURUSD", style="intent")[0]
    assert "BUY" in translate_emojis("🚀 US30", style="intent")[0]
    assert "BUY" in translate_emojis("🐂 XAUUSD", style="intent")[0]

    # SELL emojis
    assert "SELL" in translate_emojis("🔴 GOLD 2650", style="intent")[0]
    assert "SELL" in translate_emojis("📉 GBPUSD", style="intent")[0]
    assert "SELL" in translate_emojis("🐻 BTCUSD", style="intent")[0]

    # TP and SL emojis
    assert "TP" in translate_emojis("🎯 Target: 2670", style="intent")[0]
    assert "SL" in translate_emojis("🛑 Stop: 2640", style="intent")[0]
    assert "ENTRY" in translate_emojis("📍 2650.00", style="intent")[0]


@pytest.mark.unit
def test_emoji_translation_token_mode():
    """Verify token mode injects ADR-002 delimited tokens ([DIR:BUY], [TOKEN:TP], etc.)."""
    raw = "🟢 GOLD 🎯 2670 🛑 2640"
    tokenized, count = translate_emojis(raw, style="token")
    assert count == 3
    assert "[DIR:BUY]" in tokenized
    assert "[TOKEN:TP]" in tokenized
    assert "[TOKEN:SL]" in tokenized


# =============================================================================
# 4. DECIMAL & PUNCTUATION DISAMBIGUATION TESTS
# =============================================================================

@pytest.mark.unit
def test_decimal_comma_and_punctuation():
    """Verify European decimal commas and unicode dashes are standardized."""
    # Decimal commas between digits
    assert standardize_decimals_and_punctuation("1,0850") == "1.0850"
    assert standardize_decimals_and_punctuation("2650,50") == "2650.50"

    # List comma should be preserved
    list_text = "TP1, TP2, TP3"
    assert standardize_decimals_and_punctuation(list_text) == "TP1, TP2, TP3"

    # Unicode dashes: en-dash, em-dash, minus
    dashes = "2650–2660"  # en-dash \u2013
    assert "-" in standardize_decimals_and_punctuation(dashes)

    # Price range hyphens: standardized with surrounding spaces
    price_range = "1.0850-1.0870"
    standardized = standardize_decimals_and_punctuation(price_range)
    assert standardized == "1.0850 - 1.0870"


# =============================================================================
# 5. PURE STATELESS FUNCTION & END-TO-END CONTRACT TESTS
# =============================================================================

@pytest.mark.unit
def test_clean_text_pure_function_contract():
    """Verify clean_text is a pure, stateless function returning a cleaned string."""
    raw = "🟢 XАUUSD (Cyrillic А)\nEntry: ٢٦٥٠,٥٠ - ٢٦٥٢,٠٠\n🛑 SL: ٢٦٤٠,٠٠\n🎯 TP: ٢٦٧٠,٠٠"
    cleaned = clean_text(raw)

    assert isinstance(cleaned, str)
    assert "BUY" in cleaned
    assert "XAUUSD" in cleaned  # Cyrillic А normalized to Latin A
    assert "2650.50" in cleaned  # Digits harmonized and comma converted to dot
    assert "2640.00" in cleaned
    assert "2670.00" in cleaned
    assert "SL" in cleaned
    assert "TP" in cleaned

    # Determinism: second invocation produces identical output
    cleaned_again = clean_text(raw)
    assert cleaned == cleaned_again


@pytest.mark.unit
def test_text_normalizer_class_interface():
    """Verify TextNormalizer class methods, static methods, and callable instance."""
    raw = "🔴 EURUSD SЕLL (Cyrillic Е) @ ۱.۰۸۵۰"

    # Static clean method
    res1 = TextNormalizer.clean(raw)
    assert "SELL" in res1
    assert "1.0850" in res1

    # Static normalize_string
    res2 = TextNormalizer.normalize_string(raw)
    assert res1 == res2

    # Callable instance
    normalizer = TextNormalizer()
    res3 = normalizer(raw)
    assert res1 == res3

    # Functional normalize_text wrapper
    res4 = normalize_text(raw)
    assert res1 == res4


@pytest.mark.unit
def test_text_normalizer_normalize_result_object():
    """Verify TextNormalizer.normalize() returns rich NormalizationResult telemetry."""
    raw = "🟢 XАUUSD BUY @ ٢٦٥٠.٥٠ \u200b SL: ٢٦٤٠ 🎯 TP: ٢٦٧٠"
    result = TextNormalizer.normalize(raw)

    assert isinstance(result, NormalizationResult)
    assert isinstance(result.normalized_text, str)
    assert result.has_buy_directive is True
    assert result.has_sell_directive is False
    assert result.digits_replaced_count > 0
    assert result.homoglyphs_replaced_count > 0
    assert result.emojis_replaced_count > 0
    assert result.latency_microseconds > 0.0

    # Latency budget: single-call execution should complete in well under 1000 microseconds (1ms)
    assert result.latency_microseconds < 1000.0


@pytest.mark.unit
def test_idempotency_contract():
    """Verify that normalizing already clean text is completely idempotent."""
    clean_sample = "BUY XAUUSD 2650.50 SL 2640.00 TP 2670.00"
    once = clean_text(clean_sample)
    twice = clean_text(once)
    assert once == twice
    assert once == clean_sample
