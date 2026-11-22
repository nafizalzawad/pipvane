"""
Normalization Test Suite (Pipvane v3 Sprint 2 / ADR-002).

Validates:
1. At least 20 messy multi-lingual string inputs containing a mix of Persian digits,
   zero-width spaces, Cyrillic & Greek homoglyphs, and emojis.
2. Output matches clean, standardized Latin-character strings.
3. 100% branch and statement coverage on engine.normalizer and engine.text_normalizer.
"""

import pytest

from engine.normalizer import (
    TextNormalizer,
    NormalizationOptions,
    NormalizationResult,
    clean_text,
    normalize_text,
    normalize_raw_text,
    strip_zero_width_and_invisibles,
    normalize_digits,
    normalize_homoglyphs,
    translate_emojis,
    standardize_decimals_and_punctuation,
    normalize_whitespace,
)
import engine.text_normalizer as alias_normalizer


# =============================================================================
# 25 MESSY REAL-WORLD TELEGRAM SIGNAL INPUTS & EXPECTED STANDARDIZED LATIN
# =============================================================================

MESSY_SIGNAL_TEST_CASES = [
    # 1. Gold Buy with Persian digits, Cyrillic Ve, Ha, A, and ZWSP / ZWNJ
    (
        "🟢\u200B\u0412UY \u0425\u0410UUSD\u200C 📍 ۲۶۵۰,۵۰ 🛑 ۲۶۴۰,۰۰ 🎯 ۲۶۸۰,۰۰",
        "BUY BUY XAUUSD ENTRY 2650.50 SL 2640.00 TP 2680.00",
    ),
    # 2. EURUSD Sell with Arabic-Indic digits, Greek Epsilon, Cyrillic Dze & Ie, LRM
    (
        "🔴\u200E \u0405\u0415LL \u0395URUSD 📍 ١,٠٨٥٠ 🛑 ١,٠٩٠٠ 🎯 ١,٠٧٥٠",
        "SELL SELL EURUSD ENTRY 1.0850 SL 1.0900 TP 1.0750",
    ),
    # 3. GBPUSD Bull rocket with Persian digits, Cyrillic Ve, and ZWJ
    (
        "🚀\u200D BUY G\u0412PUSD 📍 ۱,۲۹۵۰ 🛑 ۱,۲۹۰۰ 💰 ۱,۳۰۵۰",
        "BUY BUY GBPUSD ENTRY 1.2950 SL 1.2900 TP 1.3050",
    ),
    # 4. USDJPY Bear emoji, BOM byte order mark, and Devanagari digits
    (
        "\uFEFF🐻 SELL USDJPY 📍 १५२,५० 🛑 १५३,२० 🎯 १५۱,۰۰",
        "SELL SELL USDJPY ENTRY 152.50 SL 153.20 TP 151.00",
    ),
    # 5. BTCUSD Rocket with variation selector, Persian digits, and Cyrillic Ve
    (
        "⬆️\uFE0F \u0412UY \u0412TCUSD 📍 ۹۲۵۰۰,۵۰ 🛑 ۹۱۰۰۰,۰۰ 🎯 ۹۵۰۰۰,۰۰",
        "BUY BUY BTCUSD ENTRY 92500.50 SL 91000.00 TP 95000.00",
    ),
    # 6. ETHUSD Blood drop emoji, Cyrillic homoglyphs, and Persian digits
    (
        "🩸 \u0405\u0415LL \u0415THUSD 📍 ۳۱۵۰,۰۰ 🛑 ۳۲۲۰,۰۰ 🎯 ۳۰۵۰,۰۰",
        "SELL SELL ETHUSD ENTRY 3150.00 SL 3220.00 TP 3050.00",
    ),
    # 7. Silver multi-TP ladder with Cyrillic Ha & A, Persian digits, and newlines
    (
        "🟢 BUY \u0425\u0410GUSD 📍 ۳۱,۲۵ 🛑 ۳۰,۸۰\n🎯 TP1: ۳۱,۶۰\n🎯 TP2: ۳۲,۰۰\n🎯 TP3: ۳۲,۵۰",
        "BUY BUY XAGUSD ENTRY 31.25 SL 30.80\nTP TP1: 31.60\nTP TP2: 32.00\nTP TP3: 32.50",
    ),
    # 8. AUDUSD Price range with en-dash, Cyrillic A, Persian digits, and ZWSP
    (
        "📈\u200B BUY \u0410UDUSD 📍 ۰,۶۵۲۰\u2013۰,۶۵۴۰ 🛑 ۰,۶۴۸۰ 🎯 ۰,۶۶۰۰",
        "BUY BUY AUDUSD ENTRY 0.6520 - 0.6540 SL 0.6480 TP 0.6600",
    ),
    # 9. NZDUSD Down triangle emoji with Persian digits
    (
        "🔽 SELL NZDUSD 📍 ۰,۵۹۱۰ 🛑 ۰,۵۹۵۰ 🎯 ۰,۵۸۵۰",
        "SELL SELL NZDUSD ENTRY 0.5910 SL 0.5950 TP 0.5850",
    ),
    # 10. USDCAD Square buy emoji, Cyrillic Ve & A, Persian digits, Dollar emoji
    (
        "🟩 \u0412UY USDC\u0410D 📍 ۱,۳۹۵۰ 🛑 ۱,۳۹۰۰ 💵 ۱,۴۰۵۰",
        "BUY BUY USDCAD ENTRY 1.3950 SL 1.3900 TP 1.4050",
    ),
    # 11. USDCHF No entry sign for SL, Cyrillic Es, Persian digits
    (
        "🔴 SELL USD\u0421HF 📍 ۰,۸۸۴۰ ⛔ ۰,۸۸۹۰ 🎯 ۰,۸۷۸۰",
        "SELL SELL USDCHF ENTRY 0.8840 SL 0.8890 TP 0.8780",
    ),
    # 12. EURGBP Cross mark for SL, Checkmark for TP, Greek Epsilon & Cyrillic Ve
    (
        "🟢 BUY \u0395URG\u0412P 📍 ۰,۸۳۲۰ ❌ ۰,۸۲۹۰ ✅ ۰,۸۳۸۰",
        "BUY BUY EURGBP ENTRY 0.8320 SL 0.8290 TP 0.8380",
    ),
    # 13. EURJPY Chart decreasing emoji, Greek Epsilon, Persian digits
    (
        "📉 SELL \u0395URJPY 📍 ۱۶۲,۴۰ 🛑 ۱۶۳,۰۰ 🎯 ۱۶۱,۵۰",
        "SELL SELL EURJPY ENTRY 162.40 SL 163.00 TP 161.50",
    ),
    # 14. GBPJPY Bull Ox emoji, Cyrillic Ve, Persian digits
    (
        "🐂 BUY G\u0412PJPY 📍 ۱۹۶,۵۰ 🛑 ۱۹۵,۸۰ 🎯 ۱۹۷,۸۰",
        "BUY BUY GBPJPY ENTRY 196.50 SL 195.80 TP 197.80",
    ),
    # 15. AUDJPY Up triangle emoji, Cyrillic A, Persian digits
    (
        "🔼 BUY \u0410UDJPY 📍 ۹۹,۲۰ 🛑 ۹۸,۷۰ 🎯 ۱۰۰,۰۰",
        "BUY BUY AUDJPY ENTRY 99.20 SL 98.70 TP 100.00",
    ),
    # 16. CADJPY Down arrow emoji, Cyrillic A, Persian digits
    (
        "⬇️ SELL C\u0410DJPY 📍 ۱۰۸,۵۰ 🛑 ۱۰۹,۱۰ 🎯 ۱۰۷,۵۰",
        "SELL SELL CADJPY ENTRY 108.50 SL 109.10 TP 107.50",
    ),
    # 17. CHFJPY Money bag emoji, Cyrillic Es, Persian digits
    (
        "🟢 BUY \u0421HFJPY 📍 ۱۷۲,۳۰ 🛑 ۱۷۱,۷۰ 💰 ۱۷۳,۵۰",
        "BUY BUY CHFJPY ENTRY 172.30 SL 171.70 TP 173.50",
    ),
    # 18. EURAUD Label emoji with variation selector, Greek Epsilon & Cyrillic A
    (
        "🔴 SELL \u0395UR\u0410UD 🏷️ ۱,۶۴۵۰ 🛑 ۱,۶۵۱۰ 🎯 ۱,۶۳۵۰",
        "SELL SELL EURAUD ENTRY 1.6450 SL 1.6510 TP 1.6350",
    ),
    # 19. GBPAUD Soft hyphen \u00AD, Cyrillic Ve & A, Persian digits
    (
        "🟢\u00AD BUY G\u0412P\u0410UD 📍 ۱,۹۸۲۰ 🛑 ۱,۹۷۵۰ 🎯 ۱,۹۹۵۰",
        "BUY BUY GBPAUD ENTRY 1.9820 SL 1.9750 TP 1.9950",
    ),
    # 20. Heavy VIP multi-line signal with multiple ZWSP, Persian digits, and Cyrillic
    (
        "🎯\u200B VIP SIGNAL \u200B🎯\n🟢 \u0412UY \u0425\u0410UUSD 📍 ۲۶۵۵,۲۰\n🛑 SL: ۲۶۴۵,۰۰\n💰 TP: ۲۶۷۵,۰۰",
        "TP VIP SIGNAL TP\nBUY BUY XAUUSD ENTRY 2655.20\nSL SL: 2645.00\nTP TP: 2675.00",
    ),
    # 21. Lowercase Cyrillic homoglyphs (\u0445->x, \u0430->a, \u0443->y, \u0441->s, \u0435->e)
    (
        "🟢 buy \u0445\u0430\u0443usd \u0441\u0435ll \u0435urusd ۲۶۵۰,۵۰",
        "BUY buy xayusd sell eurusd 2650.50",
    ),
    # 22. Excessive spacing and multiple blank lines (>2)
    (
        "🟢    BUY    XAUUSD    \n\n\n\n📍    ۲۶۵۰,۵۰    \n\n\n🛑    ۲۶۴۰,۰۰    ",
        "BUY BUY XAUUSD\n\nENTRY 2650.50\n\nSL 2640.00",
    ),
    # 23. Embedded zero-width marks inside digits
    (
        "BUY XAUUSD ۲\u200C۶\u200D۵\u200E۰,۵۰",
        "BUY XAUUSD 2650.50",
    ),
    # 24. Bengali digits and Greek Omicron & Upsilon
    (
        "🔴 SELL \u039f\u03a5RUSD ১২৩৪.৫৬ 🛑 ১২৪০.০০",
        "SELL SELL OYRUSD 1234.56 SL 1240.00",
    ),
    # 25. Up arrow emoji without variation selector (\u2b06), Cyrillic Ve & A
    (
        "\u2b06 BUY G\u0412P\u0410UD ۲,۰۱۰۰ 🛑 ۲,۰۰۵۰",
        "BUY BUY GBPAUD 2.0100 SL 2.0050",
    ),
]


@pytest.mark.unit
@pytest.mark.parametrize("messy_input, expected_clean", MESSY_SIGNAL_TEST_CASES)
def test_normalization_twenty_plus_messy_strings(messy_input: str, expected_clean: str):
    """
    Validates at least 20 messy strings containing Persian digits, zero-width spaces,
    Cyrillic & Greek homoglyphs, and emojis match clean standardized Latin strings.
    """
    cleaned = clean_text(messy_input)
    assert cleaned == expected_clean, (
        f"Mismatch!\nInput:    {repr(messy_input)}\n"
        f"Expected: {repr(expected_clean)}\n"
        f"Actual:   {repr(cleaned)}"
    )


@pytest.mark.unit
def test_text_normalizer_class_interface():
    """Verify class methods and instance call invocation on TextNormalizer."""
    normalizer = TextNormalizer()
    raw = "🟢\u200B BUY XAUUSD ۲۶۵۰,۵۰"
    expected = "BUY BUY XAUUSD 2650.50"

    assert normalizer(raw) == expected
    assert TextNormalizer.clean(raw) == expected
    assert TextNormalizer.normalize_string(raw) == expected
    assert normalize_text(raw) == expected


@pytest.mark.unit
def test_pure_functions_individual_stages():
    """Verify standalone pure transformation functions for individual pipeline stages."""
    # 1. Zero-width stripping
    with_invisibles = "BUY\u200B\u200C\u200D\uFEFFXAUUSD"
    assert strip_zero_width_and_invisibles(with_invisibles) == "BUYXAUUSD"
    assert TextNormalizer.strip_zero_width(with_invisibles) == "BUYXAUUSD"

    # 2. Digits normalization
    persian_digits = "۰۱۲۳۴۵۶۷۸۹"
    assert normalize_digits(persian_digits) == "0123456789"
    assert TextNormalizer.normalize_digits(persian_digits) == "0123456789"

    # 3. Homoglyph normalization
    cyrillic_chars = "\u0410\u0412\u0415\u041a\u041c\u041d\u041e\u0420\u0421\u0422\u0425"
    assert normalize_homoglyphs(cyrillic_chars) == "ABEKMHOPCTX"
    assert TextNormalizer.normalize_homoglyphs(cyrillic_chars) == "ABEKMHOPCTX"

    # 4. Emoji translation (intent vs token)
    emoji_str = "🟢 BUY 🔴 SELL 🎯 🛑 📍"
    intent_text, count1 = translate_emojis(emoji_str, style="intent")
    assert "BUY" in intent_text and "SELL" in intent_text
    assert count1 == 5

    token_text, count2 = translate_emojis(emoji_str, style="token")
    assert "[DIR:BUY]" in token_text and "[DIR:SELL]" in token_text
    assert "[TOKEN:TP]" in token_text and "[TOKEN:SL]" in token_text
    assert count2 == 5

    t_text, t_count = TextNormalizer.translate_emojis(emoji_str, style="intent")
    assert t_count == 5

    # 5. Decimal and punctuation standardization
    dec_text = "1,0850 - 1,0870 and 2650,5\u20132660,0"
    dec_clean = standardize_decimals_and_punctuation(dec_text)
    assert "1.0850 - 1.0870" in dec_clean
    assert "2650.5 - 2660.0" in dec_clean

    # 6. Whitespace and linebreak handling
    ws_preserve = "Line 1   \n\n\n\nLine 2   "
    assert normalize_whitespace(ws_preserve, preserve_linebreaks=True) == "Line 1\n\nLine 2"
    assert normalize_whitespace(ws_preserve, preserve_linebreaks=False) == "Line 1 Line 2"


@pytest.mark.unit
def test_pipeline_options_toggles_and_branch_coverage():
    """Verify each stage can be selectively disabled via NormalizationOptions."""
    raw = "🟢\u200B BUY \u0410UDUSD ۲۶۵۰,۵۰"

    # Default options (options=None)
    res_default = TextNormalizer.normalize(raw)
    assert isinstance(res_default, NormalizationResult)
    assert res_default.has_buy_directive is True
    assert res_default.has_sell_directive is False
    assert res_default.digits_replaced_count > 0
    assert res_default.homoglyphs_replaced_count > 0
    assert res_default.emojis_replaced_count > 0
    assert res_default.latency_microseconds >= 0.0

    # strip_invisibles=False
    opt_no_strip = NormalizationOptions(strip_invisibles=False)
    res_no_strip = TextNormalizer.normalize(raw, options=opt_no_strip)
    assert "\u200B" in res_no_strip.normalized_text

    # tokenize_emojis=False
    opt_no_emoji = NormalizationOptions(tokenize_emojis=False)
    res_no_emoji = TextNormalizer.normalize(raw, options=opt_no_emoji)
    assert "🟢" in res_no_emoji.normalized_text
    assert res_no_emoji.emojis_replaced_count == 0

    # harmonize_digits=False
    opt_no_digits = NormalizationOptions(harmonize_digits=False)
    res_no_digits = TextNormalizer.normalize(raw, options=opt_no_digits)
    assert "۲۶۵۰" in res_no_digits.normalized_text
    assert res_no_digits.digits_replaced_count == 0

    # normalize_homoglyphs=False
    opt_no_homoglyphs = NormalizationOptions(normalize_homoglyphs=False)
    res_no_homoglyphs = TextNormalizer.normalize(raw, options=opt_no_homoglyphs)
    assert "\u0410" in res_no_homoglyphs.normalized_text
    assert res_no_homoglyphs.homoglyphs_replaced_count == 0

    # standardize_decimals=False
    raw_dec = "1,0850"
    opt_no_dec = NormalizationOptions(standardize_decimals=False)
    res_no_dec = TextNormalizer.normalize(raw_dec, options=opt_no_dec)
    assert "1,0850" in res_no_dec.normalized_text

    # preserve_linebreaks=False
    multiline = "Line 1\nLine 2"
    opt_no_lb = NormalizationOptions(preserve_linebreaks=False)
    res_no_lb = TextNormalizer.normalize(multiline, options=opt_no_lb)
    assert res_no_lb.normalized_text == "Line 1 Line 2"

    # Sell directive detection
    raw_sell = "🔴 SELL EURUSD 1.0850"
    res_sell = TextNormalizer.normalize(raw_sell)
    assert res_sell.has_sell_directive is True
    assert res_sell.has_buy_directive is False

    # Neutral text with no trade directive
    neutral = "Good morning traders! Market overview for today."
    res_neutral = TextNormalizer.normalize(neutral)
    assert res_neutral.has_buy_directive is False
    assert res_neutral.has_sell_directive is False

    # Token style emoji output
    opt_token = NormalizationOptions(emoji_style="token")
    res_token = TextNormalizer.normalize("🟢 BUY 🛑 SL", options=opt_token)
    assert "[DIR:BUY]" in res_token.normalized_text
    assert "[TOKEN:SL]" in res_token.normalized_text

    # Functional wrapper normalize_raw_text
    res_func = normalize_raw_text(raw)
    assert res_func.normalized_text == res_default.normalized_text


@pytest.mark.unit
def test_alias_module_exports():
    """Verify backwards-compatible module exports in engine.text_normalizer."""
    assert alias_normalizer.TextNormalizer is TextNormalizer
    assert alias_normalizer.clean_text is clean_text
    assert alias_normalizer.normalize_text is normalize_text
    assert alias_normalizer.normalize_raw_text is normalize_raw_text
    assert alias_normalizer.normalize_digits is normalize_digits
    assert alias_normalizer.normalize_homoglyphs is normalize_homoglyphs
    assert alias_normalizer.strip_zero_width_and_invisibles is strip_zero_width_and_invisibles
    assert alias_normalizer.translate_emojis is translate_emojis
    assert alias_normalizer.standardize_decimals_and_punctuation is standardize_decimals_and_punctuation
    assert alias_normalizer.normalize_whitespace is normalize_whitespace
    assert alias_normalizer.NormalizationOptions is NormalizationOptions
    assert alias_normalizer.NormalizationResult is NormalizationResult

    cleaned = alias_normalizer.clean_text("🟢 BUY XAUUSD ۲۶۵۰.۵۰")
    assert cleaned == "BUY BUY XAUUSD 2650.50"
