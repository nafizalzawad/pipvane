"""
Pipvane v3 Licensing Re-export Stub.
Aliases all licensing definitions directly to engine.license_manager.
"""

from engine.license_manager import (
    LicenseManager,
    LicenseEntitlements,
    LicenseValidationResult,
    LicenseValidationStatus,
    LicenseError,
    LicenseValidationError,
    LicenseExpiredError,
    LicenseAccountLimitExceededError,
    VAULT_LICENSE_KEY,
    VAULT_LAST_REFRESH_KEY,
    SEVEN_DAYS_SECONDS,
    THIRTY_DAYS_SECONDS,
    create_signed_jwt,
    decode_jwt_unverified,
    verify_jwt_signature,
)

__all__ = [
    "LicenseManager",
    "LicenseEntitlements",
    "LicenseValidationResult",
    "LicenseValidationStatus",
    "LicenseError",
    "LicenseValidationError",
    "LicenseExpiredError",
    "LicenseAccountLimitExceededError",
    "VAULT_LICENSE_KEY",
    "VAULT_LAST_REFRESH_KEY",
    "SEVEN_DAYS_SECONDS",
    "THIRTY_DAYS_SECONDS",
    "create_signed_jwt",
    "decode_jwt_unverified",
    "verify_jwt_signature",
]
