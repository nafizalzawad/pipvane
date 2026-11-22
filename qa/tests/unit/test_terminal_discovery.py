"""
Unit tests for TerminalDiscovery (Sprint 13 Directive).

Validates:
1. Registry & Path Scanning: Identifies terminal.exe and terminal64.exe instances.
2. PE Binary Inspection: Distinguishes 32-bit MT4 vs 64-bit MT5 binaries.
3. Portable Mode Support: Detects portable installations via custom paths and folder layouts.
4. Dynamic IPC Mapping: Assigns unique Named Pipe and Shared Memory addresses.
5. Deduplication: Merges multiple discovery vectors for the same executable path.
6. JSON Serialization: Exports structured JSON adhering to dashboard specifications.
"""

import json
import struct
from pathlib import Path
from typing import List
from unittest.mock import MagicMock, patch

import pytest

from engine.terminal_discovery import (
    DiscoveredTerminal,
    TerminalDiscovery,
)


def create_mock_pe(path: Path, machine: int = 0x8664) -> None:
    """Helper creating a minimal valid DOS/PE header stub with specified machine type."""
    path.parent.mkdir(parents=True, exist_ok=True)
    # DOS Header: 'MZ' at 0x00, e_lfanew pointer at 0x3C pointing to offset 64
    dos_header = bytearray(b"MZ" + b"\x00" * 58 + struct.pack("<I", 64))
    # PE Header at offset 64: 'PE\0\0' + 2-byte Machine + 18 bytes padding
    pe_header = b"PE\x00\x00" + struct.pack("<H", machine) + b"\x00" * 18
    path.write_bytes(dos_header + pe_header)


# =============================================================================
# 1. PE Binary Inspection Tests (MT4 vs MT5)
# =============================================================================

def test_pe_inspection_mt4_and_mt5(tmp_path: Path):
    """Inspects PE headers to classify 32-bit MT4 (0x014C) vs 64-bit MT5 (0x8664)."""
    discovery = TerminalDiscovery()

    # 1. 32-bit MT4 binary
    mt4_exe = tmp_path / "mt4_root" / "terminal.exe"
    create_mock_pe(mt4_exe, machine=0x014C)
    is_64, ver = discovery._inspect_executable_binary(mt4_exe)
    assert is_64 is False
    assert ver == "MT4"

    # 2. 64-bit MT5 binary
    mt5_exe = tmp_path / "mt5_root" / "terminal.exe"
    create_mock_pe(mt5_exe, machine=0x8664)
    is_64, ver = discovery._inspect_executable_binary(mt5_exe)
    assert is_64 is True
    assert ver == "MT5"

    # 3. Dedicated terminal64.exe name
    mt5_64 = tmp_path / "mt5_x64" / "terminal64.exe"
    create_mock_pe(mt5_64, machine=0x8664)
    is_64, ver = discovery._inspect_executable_binary(mt5_64)
    assert is_64 is True
    assert ver == "MT5"


# =============================================================================
# 2. Portable Mode Support Tests
# =============================================================================

def test_scan_custom_paths_and_portable_mode(tmp_path: Path):
    """Verifies custom directory scan detects portable MetaTrader instances."""
    discovery = TerminalDiscovery()

    # Setup Portable MT4 folder layout
    portable_mt4_dir = tmp_path / "CustomPortableMT4"
    (portable_mt4_dir / "MQL4").mkdir(parents=True, exist_ok=True)
    mt4_exe = portable_mt4_dir / "terminal.exe"
    create_mock_pe(mt4_exe, machine=0x014C)

    # Setup Portable MT5 folder layout
    portable_mt5_dir = tmp_path / "CustomPortableMT5"
    (portable_mt5_dir / "MQL5").mkdir(parents=True, exist_ok=True)
    mt5_exe = portable_mt5_dir / "terminal64.exe"
    create_mock_pe(mt5_exe, machine=0x8664)

    # Scan custom paths passed by user/UI
    discovered = discovery.scan_custom_paths([
        str(portable_mt4_dir),
        str(mt5_exe), # Direct executable path
    ])

    assert len(discovered) == 2

    # Check MT4
    d_mt4 = next(t for t in discovered if t.version == "MT4")
    assert d_mt4.is_portable is True
    assert d_mt4.source == "custom_path"
    assert Path(d_mt4.executable_path).name == "terminal.exe"

    # Check MT5
    d_mt5 = next(t for t in discovered if t.version == "MT5")
    assert d_mt5.is_portable is True
    assert d_mt5.source == "custom_path"
    assert Path(d_mt5.executable_path).name == "terminal64.exe"


# =============================================================================
# 3. Dynamic IPC Mapping Tests
# =============================================================================

def test_dynamic_ipc_mapping_and_indexing(tmp_path: Path):
    """Dynamic IPC mapping assigns unique Named Pipes and Shared Memory names."""
    discovery = TerminalDiscovery(
        pipe_prefix=r"\\.\pipe\pipvane_",
        shm_prefix=r"Local\Pipvane_SHM_",
    )

    # Create 2 MT5 terminals and 1 MT4 terminal
    t1_exe = tmp_path / "Broker1_MT5" / "terminal64.exe"
    t2_exe = tmp_path / "Broker2_MT5" / "terminal64.exe"
    t3_exe = tmp_path / "Broker3_MT4" / "terminal.exe"

    create_mock_pe(t1_exe, machine=0x8664)
    create_mock_pe(t2_exe, machine=0x8664)
    create_mock_pe(t3_exe, machine=0x014C)

    terminals = discovery.scan_all(custom_paths=[
        str(tmp_path / "Broker1_MT5"),
        str(tmp_path / "Broker2_MT5"),
        str(tmp_path / "Broker3_MT4"),
    ])

    assert len(terminals) == 3

    mt4_terms = [t for t in terminals if t.version == "MT4"]
    mt5_terms = [t for t in terminals if t.version == "MT5"]

    assert len(mt4_terms) == 1
    assert len(mt5_terms) == 2

    # Check MT4 mapping
    assert mt4_terms[0].id == "mt4_1"
    assert mt4_terms[0].named_pipe_address == r"\\.\pipe\pipvane_mt4_1"
    assert mt4_terms[0].shm_map_name == r"Local\Pipvane_SHM_mt4_1"

    # Check MT5 mappings (deterministic indices mt5_1, mt5_2)
    assert mt5_terms[0].id == "mt5_1"
    assert mt5_terms[0].named_pipe_address == r"\\.\pipe\pipvane_mt5_1"
    assert mt5_terms[0].shm_map_name == r"Local\Pipvane_SHM_mt5_1"

    assert mt5_terms[1].id == "mt5_2"
    assert mt5_terms[1].named_pipe_address == r"\\.\pipe\pipvane_mt5_2"
    assert mt5_terms[1].shm_map_name == r"Local\Pipvane_SHM_mt5_2"


# =============================================================================
# 4. Deduplication Tests
# =============================================================================

def test_deduplication_of_terminals(tmp_path: Path):
    """The same terminal found via multiple vectors is merged without duplicates."""
    discovery = TerminalDiscovery()

    term_dir = tmp_path / "Pepperstone MT5"
    exe = term_dir / "terminal64.exe"
    create_mock_pe(exe, machine=0x8664)

    # Pass the exact same terminal multiple times via directory and direct file
    terminals = discovery.scan_all(custom_paths=[
        str(term_dir),
        str(exe),
        str(term_dir).lower(),
    ])

    # Must be deduplicated into exactly 1 record
    assert len(terminals) == 1
    assert terminals[0].id == "mt5_1"


# =============================================================================
# 5. JSON Serialization Tests
# =============================================================================

def test_json_export_structure(tmp_path: Path):
    """to_json returns structured, parseable JSON conforming to PRD schema."""
    discovery = TerminalDiscovery()

    t_exe = tmp_path / "ICMarkets MT5" / "terminal64.exe"
    create_mock_pe(t_exe, machine=0x8664)

    json_str = discovery.discover_and_export_json(custom_paths=[str(t_exe)])
    data = json.loads(json_str)

    assert isinstance(data, list)
    assert len(data) == 1

    item = data[0]
    required_keys = {
        "id",
        "name",
        "version",
        "executable_path",
        "install_directory",
        "is_64bit",
        "is_portable",
        "is_running",
        "named_pipe_address",
        "shm_map_name",
        "source",
        "build",
    }
    assert required_keys.issubset(item.keys())
    assert item["version"] == "MT5"
    assert item["is_64bit"] is True
    assert item["named_pipe_address"] == r"\\.\pipe\pipvane_mt5_1"
    assert item["shm_map_name"] == r"Local\Pipvane_SHM_mt5_1"


# =============================================================================
# 6. Standard Installation Path Scanning Tests (Mocked)
# =============================================================================

def test_scan_standard_paths_mocked(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Standard path scanner searches Program Files and Program Files (x86)."""
    fake_prog_files = tmp_path / "Program Files"
    fake_mt5 = fake_prog_files / "MetaTrader 5" / "terminal64.exe"
    create_mock_pe(fake_mt5, machine=0x8664)

    fake_prog_x86 = tmp_path / "Program Files (x86)"
    fake_mt4 = fake_prog_x86 / "MetaTrader 4" / "terminal.exe"
    create_mock_pe(fake_mt4, machine=0x014C)

    # Monkeypatch environment variables
    monkeypatch.setenv("ProgramFiles", str(fake_prog_files))
    monkeypatch.setenv("ProgramFiles(x86)", str(fake_prog_x86))
    monkeypatch.delenv("LOCALAPPDATA", raising=False)

    discovery = TerminalDiscovery()
    discovered = discovery.scan_standard_paths()

    assert len(discovered) == 2
    versions = {t.version for t in discovered}
    assert versions == {"MT4", "MT5"}


# =============================================================================
# 7. Roaming Origin Pointers Tests
# =============================================================================

def test_scan_roaming_origins(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Scans %APPDATA%\\MetaQuotes\\Terminal\\<hash>\\origin.txt to resolve installations."""
    appdata_dir = tmp_path / "AppData" / "Roaming"
    term_hash_dir = appdata_dir / "MetaQuotes" / "Terminal" / "50CA3DFB510CAD5A8BFB84D881842C69"
    term_hash_dir.mkdir(parents=True, exist_ok=True)

    install_dir = tmp_path / "InstalledMT5"
    exe = install_dir / "terminal64.exe"
    create_mock_pe(exe, machine=0x8664)

    # Write origin.txt pointing to the install folder (UTF-16 encoded)
    origin_txt = term_hash_dir / "origin.txt"
    origin_txt.write_text(str(install_dir), encoding="utf-16")

    monkeypatch.setenv("APPDATA", str(appdata_dir))

    discovery = TerminalDiscovery()
    discovered = discovery.scan_roaming_origins()

    assert len(discovered) == 1
    assert discovered[0].version == "MT5"
    assert discovered[0].source == "roaming_origin"
    assert discovered[0].data_directory == str(term_hash_dir)


# =============================================================================
# 8. Registry Scanning Tests (Mocked)
# =============================================================================

def test_scan_registry_mocked(tmp_path: Path):
    """Verifies registry parser identifies MetaTrader keys and creates terminal records."""
    discovery = TerminalDiscovery()

    install_dir = tmp_path / "FTMO MT4"
    exe = install_dir / "terminal.exe"
    create_mock_pe(exe, machine=0x014C)

    # Mock registry key handle
    mock_key = MagicMock()
    mock_values = {
        "DisplayName": ("FTMO MetaTrader 4", 1),
        "Publisher": ("FTMO", 1),
        "InstallLocation": (str(install_dir), 1),
        "DisplayIcon": (str(exe), 1),
        "UninstallString": (str(install_dir / "uninstall.exe"), 1),
    }

    with patch("engine.terminal_discovery.winreg") as mock_winreg:
        mock_winreg.QueryValueEx.side_effect = lambda k, name: mock_values[name]
        term = discovery._parse_registry_app_key(mock_key)

    assert term is not None
    assert term.name == "FTMO MetaTrader 4"
    assert term.version == "MT4"
    assert term.is_64bit is False
    assert term.source == "registry"


# =============================================================================
# 9. Live Host Smoke Test
# =============================================================================

def test_live_host_scan_smoke_test():
    """Smoke test ensuring scan_all runs safely on host machine without exceptions."""
    discovery = TerminalDiscovery()
    # Should execute cleanly and return list (may be empty on clean dev machines)
    results = discovery.scan_all()
    assert isinstance(results, list)
    for r in results:
        assert r.version in ("MT4", "MT5")
        assert r.named_pipe_address.startswith(r"\\.\pipe\pipvane_")
        assert r.shm_map_name.startswith(r"Local\Pipvane_SHM_")
