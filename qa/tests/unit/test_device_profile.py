"""
Unit tests validating stable device profile generation and persistence.
Ensures device parameters remain 100% consistent across restarts to avoid Telegram anomaly detection.
"""

import os
import tempfile
import pytest

from engine.vault import VaultStorage, DeviceProfile
from engine.device_profile import DeviceProfileManager


@pytest.mark.unit
def test_device_profile_generation():
    """Verify generated profile contains valid desktop parameters."""
    profile = DeviceProfileManager.generate_random_profile()
    assert isinstance(profile, DeviceProfile)
    assert len(profile.device_model) > 0
    assert len(profile.system_version) > 0
    assert len(profile.app_version) > 0
    assert len(profile.system_language_code) > 0
    assert "Windows" in profile.system_version
    assert "Pipvane" in profile.app_version


@pytest.mark.unit
def test_device_profile_persistence_and_stability():
    """
    Verify that device parameters are generated once and remain completely stable
    across multiple queries and application restarts.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "vault.db")
        vault = VaultStorage(db_path=db_path)

        # First run: generates and stores profile
        p1 = DeviceProfileManager.get_or_create_profile(vault, profile_id="test_user")
        assert p1 is not None

        # Second query on same vault: must return identical profile
        p2 = DeviceProfileManager.get_or_create_profile(vault, profile_id="test_user")
        assert p1 == p2
        assert p1.device_model == p2.device_model
        assert p1.system_version == p2.system_version
        assert p1.app_version == p2.app_version
        assert p1.system_language_code == p2.system_language_code

        # Simulate application restart: load new VaultStorage instance from the same disk DB
        vault_after_restart = VaultStorage(db_path=db_path)
        p3 = DeviceProfileManager.get_or_create_profile(vault_after_restart, profile_id="test_user")
        assert p1 == p3
        assert p3.device_model == p1.device_model
        assert p3.system_version == p1.system_version


@pytest.mark.unit
def test_device_profile_custom_override():
    """Verify custom profile overrides are respected and saved."""
    vault = VaultStorage(db_path=":memory:")
    custom = DeviceProfile(
        device_model="Custom Precision 5570",
        system_version="Windows 11 Pro 23H2 (Build 22631)",
        app_version="Pipvane Desktop 3.0.4",
        system_language_code="en-US",
    )
    p = DeviceProfileManager.get_or_create_profile(vault, profile_id="custom_user", custom_override=custom)
    assert p == custom

    # Reloading returns the custom profile
    p2 = DeviceProfileManager.get_or_create_profile(vault, profile_id="custom_user")
    assert p2 == custom
