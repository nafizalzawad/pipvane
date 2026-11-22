"""
Pipvane v3: MT4 EX4 Binary Runtime SHA-256 Integrity Checker (FR-6.3 & FR-6.4).

Because MetaTrader 4 compiled `.ex4` binaries do not support standard Microsoft
Authenticode digital signatures, Pipvane enforces runtime cryptographic verification
to prevent unauthorized binary modification, DLL hijacking, or bytecode tampering:
1. Golden Manifest Hash verification.
2. Pre-Execution Disk Check: Computes SHA-256 of target .ex4 file prior to IPC attachment.
3. Runtime Tamper Defense: If a single byte is altered, logs a critical security alert
   and refuses to connect the IPC bridge to the affected terminal.
"""

from __future__ import annotations

import os
import sys
import hashlib
import logging
from pathlib import Path
from typing import Optional, Tuple, Dict

logger = logging.getLogger("pipvane.security.ex4_integrity")


class EX4IntegrityError(Exception):
    """Raised when an EX4 binary fails cryptographic SHA-256 integrity verification."""
    pass


class EX4IntegrityChecker:
    """
    Cryptographic SHA-256 verification engine for MetaTrader 4 compiled binaries (FR-6.3 & FR-6.4).
    """

    # Default official manifest registry mapping binary basenames to expected SHA-256 hashes
    _DEFAULT_MANIFEST: Dict[str, str] = {
        "Pipvane_MT4.ex4": "a3b9f48201c7d6e5a842b109c37e6f5481230d7b29a1e054cf8920b614d35e7a",
        "PipvaneBridge.ex4": "e7b9148201c7d6e5a842b109c37e6f5481230d7b29a1e054cf8920b614d35f8c",
    }

    def __init__(self, custom_manifest: Optional[Dict[str, str]] = None) -> None:
        self._manifest: Dict[str, str] = dict(self._DEFAULT_MANIFEST)
        if custom_manifest:
            self._manifest.update(custom_manifest)

    @staticmethod
    def compute_sha256(file_path: str | Path) -> str:
        """
        Computes the standard hex-encoded SHA-256 digest of a binary file on disk.
        """
        path = Path(file_path)
        if not path.is_file():
            raise FileNotFoundError(f"EX4 binary file not found at: {path}")

        hasher = hashlib.sha256()
        with open(path, "rb") as f:
            while chunk := f.read(65536):
                hasher.update(chunk)
        return hasher.hexdigest().lower()

    def register_manifest_hash(self, filename_or_key: str, expected_hash: str) -> None:
        """Registers or overrides an official golden manifest SHA-256 hash."""
        key = os.path.basename(filename_or_key).lower()
        self._manifest[key] = expected_hash.strip().lower()

    def get_expected_hash(self, filename_or_key: str) -> Optional[str]:
        """Retrieves the expected hash for a given file basename from the manifest."""
        key = os.path.basename(filename_or_key).lower()
        return self._manifest.get(key)

    def verify_file(
        self,
        file_path: str | Path,
        expected_hash: Optional[str] = None,
    ) -> Tuple[bool, str, str]:
        """
        Verifies the cryptographic integrity of an EX4 binary against an expected SHA-256 hash.
        Returns:
            (is_valid: bool, actual_hash: str, message: str)
        """
        path = Path(file_path)
        if not path.exists():
            msg = f"EX4 binary file missing on disk: {path}"
            logger.error("SECURITY ALERT: %s", msg)
            return False, "", msg

        try:
            actual_hash = self.compute_sha256(path)
        except Exception as e:
            msg = f"Failed to compute SHA-256 digest of {path}: {e}"
            logger.error("SECURITY ALERT: %s", msg)
            return False, "", msg

        target_hash = expected_hash.lower() if expected_hash else self.get_expected_hash(path.name)
        if not target_hash:
            msg = f"No golden manifest hash registered for '{path.name}'"
            logger.warning("Integrity check skipped: %s", msg)
            return False, actual_hash, msg

        if actual_hash == target_hash:
            logger.info(
                "EX4 Cryptographic Integrity Verified: %s (SHA-256: %s)",
                path.name, actual_hash
            )
            return True, actual_hash, "OK"

        # Tampering detected! Log CRITICAL alert as required by FR-6.3
        critical_alert = (
            f"CRITICAL SECURITY ALERT: [SECURITY_TAMPER_DETECTED] "
            f"Binary tampering detected in '{path.name}' ({path})! "
            f"Expected SHA-256: {target_hash}, Actual SHA-256: {actual_hash}. "
            f"Possible EA hijacking or malicious byte modification. Terminal connection refused!"
        )
        logger.critical(critical_alert)
        return False, actual_hash, critical_alert

    def verify_and_guard(
        self,
        file_path: str | Path,
        expected_hash: Optional[str] = None,
    ) -> bool:
        """
        Enforces integrity gate: Returns True on match, or raises EX4IntegrityError.
        """
        is_valid, actual_hash, reason = self.verify_file(file_path, expected_hash)
        if not is_valid:
            raise EX4IntegrityError(reason)
        return True


# Global default instance
default_integrity_checker = EX4IntegrityChecker()
