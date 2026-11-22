"""
Pipvane v3 Text Normalizer Alias Module.
Provides backwards-compatible exports matching engine/normalizer.py.
"""

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

__all__ = [
    "TextNormalizer",
    "clean_text",
    "normalize_text",
    "normalize_raw_text",
    "normalize_digits",
    "normalize_homoglyphs",
    "strip_zero_width_and_invisibles",
    "translate_emojis",
    "standardize_decimals_and_punctuation",
    "normalize_whitespace",
    "NormalizationOptions",
    "NormalizationResult",
]
