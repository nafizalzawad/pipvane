"""
Device Profile Generator & Persistence Manager for Telegram TDLib.

Ensures device parameters (Device Model, OS Version, App Version, System Language)
are randomized realistically on first installation, but strictly persisted and reused
across sessions and restarts to prevent Telegram anomaly detection and account flags.
"""

from __future__ import annotations

import random
import logging
from typing import Optional

from engine.vault import VaultStorage, DeviceProfile

logger = logging.getLogger("pipvane.engine.device_profile")

# Realistic hardware and OS distributions for Windows desktop clients
_DEVICE_MODELS = [
    "Desktop PC (x64-based PC)",
    "ThinkPad T14 Gen 3",
    "Dell XPS Desktop 8950",
    "HP EliteDesk 800 G8",
    "ASUS ExpertCenter D700",
    "Lenovo IdeaCentre 5",
    "Dell OptiPlex 7090",
    "Surface Laptop Studio",
]

_SYSTEM_VERSIONS = [
    "Windows 11 Pro 23H2 (Build 22631)",
    "Windows 11 Home 22H2 (Build 22621)",
    "Windows 10 Pro 22H2 (Build 19045)",
    "Windows 11 Enterprise 23H2 (Build 22631)",
]

_APP_VERSIONS = [
    "Pipvane Desktop 3.0.2",
    "Pipvane Desktop 3.0.3",
    "Pipvane Desktop 3.0.4",
]

_LANGUAGE_CODES = [
    "en-US",
    "en-GB",
    "en",
]


class DeviceProfileManager:
    """Manages generation and persistence of Telegram client device parameters."""

    @classmethod
    def generate_random_profile(cls) -> DeviceProfile:
        """Generates a realistic randomized device profile."""
        return DeviceProfile(
            device_model=random.choice(_DEVICE_MODELS),
            system_version=random.choice(_SYSTEM_VERSIONS),
            app_version=random.choice(_APP_VERSIONS),
            system_language_code=random.choice(_LANGUAGE_CODES),
        )

    @classmethod
    def get_or_create_profile(
        cls,
        vault: VaultStorage,
        profile_id: str = "default",
        custom_override: Optional[DeviceProfile] = None,
    ) -> DeviceProfile:
        """
        Retrieves existing device profile from Vault. If not found, generates a new
        randomized profile, saves it in the local vault, and returns it.
        Guarantees stable parameters across restarts.
        """
        existing = vault.get_device_profile(profile_id)
        if existing is not None:
            logger.info("Loaded persisted device profile: model=%s, os=%s", existing.device_model, existing.system_version)
            return existing

        profile = custom_override or cls.generate_random_profile()
        vault.save_device_profile(profile, profile_id=profile_id)
        logger.info("Generated and saved new device profile: model=%s, os=%s", profile.device_model, profile.system_version)
        return profile
