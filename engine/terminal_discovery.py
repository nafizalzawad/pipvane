"""
Pipvane v3: Multi-Terminal Auto-Discovery & Dynamic IPC Mapping (Sprint 13).

Scans the host system to automatically locate and inventory all MetaTrader 4
and MetaTrader 5 installations across:
1. Windows Registry (HKLM, HKCU, and WOW6432Node Uninstall / MetaQuotes keys).
2. Standard Windows installation paths (Program Files, Program Files (x86), %LOCALAPPDATA%).
3. Standard Roaming Data paths (%APPDATA%\\MetaQuotes\\Terminal\\<hash>\\origin.txt).
4. Running OS processes (active terminal.exe / terminal64.exe instances).
5. Portable Mode support (user-supplied custom directory paths via UI).
6. Dynamic IPC Mapping: Assigns unique, collision-free Shared Memory map names
   and Named Pipe addresses (e.g., \\\\.\\pipe\\pipvane_mt5_1, Local\\Pipvane_SHM_mt5_1).
7. Enrolls discovered terminals into the Engine's IPCDispatcher with EX4 integrity verification.
"""

from __future__ import annotations

import json
import logging
import os
import re
import struct
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

try:
    import winreg
except ImportError:
    winreg = None  # Non-Windows or mock environment

try:
    import psutil
except ImportError:
    psutil = None

from engine.ipc_protocol import TerminalStatus
from engine.ipc_dispatcher import IPCDispatcher, TerminalContext
from engine.ex4_integrity import (
    EX4IntegrityChecker,
    default_integrity_checker,
)

logger = logging.getLogger("pipvane.engine.terminal_discovery")

# Known MetaTrader executable names
MT_EXECUTABLES = {"terminal.exe", "terminal64.exe"}


@dataclass
class DiscoveredTerminal:
    """Detailed profile of an identified MetaTrader terminal installation."""
    terminal_id: str
    platform_type: str                   # "MT4" or "MT5"
    install_dir: str = ""
    data_dir: str = ""
    is_portable: bool = False
    build_number: int = 0
    account_login: Optional[int] = None
    broker_server: Optional[str] = None
    ex4_or_ex5_path: Optional[str] = None
    is_running: bool = False
    named_pipe_name: str = ""
    shm_name: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)
    # Additional metadata fields
    executable_path: str = ""
    name: str = ""
    is_64bit: bool = True
    source: str = "path_scan"
    process_id: Optional[int] = None

    def __post_init__(self) -> None:
        if not self.named_pipe_name and self.terminal_id:
            self.named_pipe_name = fr"\\.\pipe\pipvane_mt_{self.terminal_id}"
        if not self.shm_name and self.terminal_id:
            self.shm_name = fr"Local\pipvane_shm_{self.terminal_id}"
        if not self.executable_path and self.install_dir:
            cand64 = os.path.join(self.install_dir, "terminal64.exe")
            cand32 = os.path.join(self.install_dir, "terminal.exe")
            if self.platform_type == "MT5" and os.path.isfile(cand64):
                self.executable_path = cand64
            elif os.path.isfile(cand32):
                self.executable_path = cand32
            elif os.path.isfile(cand64):
                self.executable_path = cand64

    # --- Property Aliases for Unified Interface ---
    @property
    def id(self) -> str:
        return self.terminal_id

    @id.setter
    def id(self, val: str):
        self.terminal_id = val

    @property
    def version(self) -> str:
        return self.platform_type

    @version.setter
    def version(self, val: str):
        self.platform_type = val

    @property
    def install_directory(self) -> str:
        return self.install_dir

    @install_directory.setter
    def install_directory(self, val: str):
        self.install_dir = val

    @property
    def data_directory(self) -> str:
        return self.data_dir

    @data_directory.setter
    def data_directory(self, val: str):
        self.data_dir = val

    @property
    def named_pipe_address(self) -> str:
        return self.named_pipe_name

    @named_pipe_address.setter
    def named_pipe_address(self, val: str):
        self.named_pipe_name = val

    @property
    def shm_map_name(self) -> str:
        return self.shm_name

    @shm_map_name.setter
    def shm_map_name(self, val: str):
        self.shm_name = val

    @property
    def build(self) -> int:
        return self.build_number

    @build.setter
    def build(self, val: int):
        self.build_number = val

    def to_dict(self) -> Dict[str, Any]:
        """Convert to JSON-serializable dictionary."""
        return {
            "id": self.terminal_id,
            "terminal_id": self.terminal_id,
            "name": self.name or (os.path.basename(self.install_dir) if self.install_dir else self.terminal_id),
            "version": self.platform_type,
            "platform_type": self.platform_type,
            "build": self.build_number,
            "build_number": self.build_number,
            "executable_path": self.executable_path,
            "install_dir": self.install_dir,
            "install_directory": self.install_dir,
            "data_dir": self.data_dir,
            "data_directory": self.data_dir,
            "is_64bit": self.is_64bit,
            "is_portable": self.is_portable,
            "is_running": self.is_running,
            "named_pipe_address": self.named_pipe_name,
            "named_pipe_name": self.named_pipe_name,
            "shm_map_name": self.shm_name,
            "shm_name": self.shm_name,
            "source": self.source,
            "process_id": self.process_id,
            "account_login": self.account_login,
            "broker_server": self.broker_server,
            "ex4_or_ex5_path": self.ex4_or_ex5_path,
            "metadata": self.metadata,
        }


class TerminalDiscovery:
    """
    Subsystem for locating, probing, and enrolling MetaTrader terminals
    into the Engine's IPC Dispatcher with dynamic IPC mapping and EX4 integrity checks.
    """

    DEFAULT_MT4_EX4_REL = os.path.join("MQL4", "Experts", "Pipvane_MT4.ex4")
    DEFAULT_MT5_EX5_REL = os.path.join("MQL5", "Experts", "Pipvane_MT5.ex5")

    def __init__(
        self,
        integrity_checker: Optional[EX4IntegrityChecker] = None,
        pipe_prefix: str = r"\\.\pipe\pipvane_",
        shm_prefix: str = r"Local\Pipvane_SHM_",
        scan_depth: int = 2,
    ) -> None:
        self.integrity_checker = integrity_checker or default_integrity_checker
        self.pipe_prefix = pipe_prefix
        self.shm_prefix = shm_prefix
        self.scan_depth = scan_depth
        self._terminals: Dict[str, DiscoveredTerminal] = {}

    def register_terminal(self, terminal: DiscoveredTerminal) -> None:
        """Manually registers or caches a discovered/mock terminal instance."""
        self._terminals[terminal.terminal_id] = terminal
        logger.info(
            "Registered terminal: %s (Type=%s, Login=%s, Server=%s)",
            terminal.terminal_id, terminal.platform_type, terminal.account_login, terminal.broker_server,
        )

    def get_terminal(self, terminal_id: str) -> Optional[DiscoveredTerminal]:
        """Retrieves a cached terminal by ID."""
        return self._terminals.get(terminal_id)

    def get_all_terminals(self) -> List[DiscoveredTerminal]:
        """Returns all currently registered/discovered terminals."""
        return list(self._terminals.values())

    # -------------------------------------------------------------------------
    # Comprehensive Multi-Vector Discovery (Sprint 13)
    # -------------------------------------------------------------------------

    def scan_all(
        self,
        custom_paths: Optional[List[Union[str, Path]]] = None,
    ) -> List[DiscoveredTerminal]:
        """
        Executes comprehensive multi-vector discovery across registry, standard paths,
        active running processes, roaming origin pointers, and custom portable directories.
        Deduplicates discoveries by canonical executable path and dynamically provisions IPC maps.
        """
        discovered_map: Dict[str, DiscoveredTerminal] = {}

        # 1. Active Running Processes
        for term in self.scan_running_processes():
            canon = self._canonical_path(term.executable_path)
            discovered_map[canon] = term

        # 2. Windows Registry
        for term in self.scan_registry():
            canon = self._canonical_path(term.executable_path)
            if canon not in discovered_map:
                discovered_map[canon] = term
            else:
                if term.name and not discovered_map[canon].name:
                    discovered_map[canon].name = term.name

        # 3. Roaming MetaQuotes Data Origin Pointers
        for term in self.scan_roaming_origins():
            canon = self._canonical_path(term.executable_path)
            if canon not in discovered_map:
                discovered_map[canon] = term

        # 4. Standard File System Directories (Program Files, LocalAppData)
        for term in self.scan_standard_paths():
            canon = self._canonical_path(term.executable_path)
            if canon not in discovered_map:
                discovered_map[canon] = term

        # 5. User-Provided Custom Portable Paths
        if custom_paths:
            for term in self.scan_custom_paths(custom_paths):
                canon = self._canonical_path(term.executable_path)
                discovered_map[canon] = term

        # Sort canonically for deterministic IPC index allocation
        terminals = list(discovered_map.values())
        terminals.sort(key=lambda t: (t.platform_type, t.executable_path.lower()))

        # Assign Dynamic IPC Mapping
        self._assign_ipc_mappings(terminals)

        # Cache in local registry
        for t in terminals:
            self._terminals[t.terminal_id] = t

        return terminals

    def discover_local_terminals(
        self,
        search_dirs: Optional[List[str]] = None,
        scan_running_processes: bool = True,
    ) -> List[DiscoveredTerminal]:
        """Legacy compatibility wrapper for scan_all."""
        return self.scan_all(custom_paths=search_dirs)

    def to_json(self, terminals: List[DiscoveredTerminal], indent: int = 2) -> str:
        """Serializes list of discovered terminals to structured JSON string."""
        return json.dumps([t.to_dict() for t in terminals], indent=indent)

    def discover_and_export_json(
        self,
        custom_paths: Optional[List[Union[str, Path]]] = None,
        indent: int = 2,
    ) -> str:
        """Executes scan and returns structured JSON output."""
        terminals = self.scan_all(custom_paths=custom_paths)
        return self.to_json(terminals, indent=indent)

    # -------------------------------------------------------------------------
    # Vector 1: Windows Registry Scanning
    # -------------------------------------------------------------------------

    def scan_registry(self) -> List[DiscoveredTerminal]:
        """
        Parses Windows Registry uninstall keys to identify installed MetaTrader instances.
        """
        if winreg is None:
            return []

        discovered: List[DiscoveredTerminal] = []
        registry_targets = [
            (winreg.HKEY_LOCAL_MACHINE, r"Software\Microsoft\Windows\CurrentVersion\Uninstall"),
            (winreg.HKEY_LOCAL_MACHINE, r"Software\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall"),
            (winreg.HKEY_CURRENT_USER, r"Software\Microsoft\Windows\CurrentVersion\Uninstall"),
        ]

        for hive, subkey_path in registry_targets:
            try:
                with winreg.OpenKey(hive, subkey_path) as root_key:
                    num_subkeys = winreg.QueryInfoKey(root_key)[0]
                    for i in range(num_subkeys):
                        try:
                            key_name = winreg.EnumKey(root_key, i)
                            with winreg.OpenKey(root_key, key_name) as app_key:
                                term = self._parse_registry_app_key(app_key)
                                if term:
                                    discovered.append(term)
                        except (OSError, PermissionError):
                            continue
            except (OSError, PermissionError):
                continue

        return discovered

    def _parse_registry_app_key(self, key_handle) -> Optional[DiscoveredTerminal]:
        """Inspects registry values for MetaTrader signatures."""
        def get_val(name: str) -> str:
            try:
                val, _ = winreg.QueryValueEx(key_handle, name)
                return str(val).strip()
            except OSError:
                return ""

        display_name = get_val("DisplayName")
        publisher = get_val("Publisher")
        install_loc = get_val("InstallLocation")
        display_icon = get_val("DisplayIcon")
        uninstall_str = get_val("UninstallString")

        combined = f"{display_name} {publisher} {display_icon} {install_loc} {uninstall_str}".lower()
        if not ("metatrader" in combined or "metaquotes" in combined or "terminal.exe" in combined or "terminal64.exe" in combined):
            return None

        candidate_exe: Optional[Path] = None

        if install_loc:
            p_loc = Path(install_loc)
            if (p_loc / "terminal64.exe").is_file():
                candidate_exe = p_loc / "terminal64.exe"
            elif (p_loc / "terminal.exe").is_file():
                candidate_exe = p_loc / "terminal.exe"

        if not candidate_exe and display_icon:
            clean_icon = display_icon.split(",")[0].strip('"').strip()
            p_icon = Path(clean_icon)
            if p_icon.is_file() and p_icon.name.lower() in MT_EXECUTABLES:
                candidate_exe = p_icon

        if not candidate_exe or not candidate_exe.is_file():
            return None

        name = display_name or candidate_exe.parent.name
        return self._create_terminal_record(
            exe_path=candidate_exe,
            name=name,
            source="registry",
            is_portable=False,
        )

    # -------------------------------------------------------------------------
    # Vector 2: Standard Installation Paths
    # -------------------------------------------------------------------------

    def scan_standard_paths(self) -> List[DiscoveredTerminal]:
        """
        Scans Program Files, Program Files (x86), and LocalAppData for MetaTrader instances.
        """
        candidates: List[Path] = []
        base_roots: List[Path] = []

        prog_files = os.environ.get("ProgramFiles")
        if prog_files:
            base_roots.append(Path(prog_files))

        prog_files_x86 = os.environ.get("ProgramFiles(x86)")
        if prog_files_x86 and prog_files_x86 != prog_files:
            base_roots.append(Path(prog_files_x86))

        local_appdata = os.environ.get("LOCALAPPDATA")
        if local_appdata:
            base_roots.append(Path(local_appdata) / "Programs")

        for root in base_roots:
            if not root.is_dir():
                continue
            try:
                for entry in root.iterdir():
                    if entry.is_dir():
                        t64 = entry / "terminal64.exe"
                        t32 = entry / "terminal.exe"
                        if t64.is_file():
                            candidates.append(t64)
                        elif t32.is_file():
                            candidates.append(t32)
            except (OSError, PermissionError):
                continue

        discovered = []
        for exe in candidates:
            discovered.append(self._create_terminal_record(
                exe_path=exe,
                name=exe.parent.name,
                source="path_scan",
                is_portable=False,
            ))
        return discovered

    # -------------------------------------------------------------------------
    # Vector 3: Roaming MetaQuotes Data Origin Pointers
    # -------------------------------------------------------------------------

    def scan_roaming_origins(self) -> List[DiscoveredTerminal]:
        """
        Scans %APPDATA%\\MetaQuotes\\Terminal\\<hash> directories and reads origin.txt
        which stores the exact install path for roaming terminals.
        """
        appdata = os.environ.get("APPDATA")
        if not appdata:
            return []

        term_dir = Path(appdata) / "MetaQuotes" / "Terminal"
        if not term_dir.is_dir():
            return []

        discovered = []
        try:
            for item in term_dir.iterdir():
                if item.is_dir() and len(item.name) == 32:
                    origin_file = item / "origin.txt"
                    if origin_file.is_file():
                        try:
                            content = ""
                            try:
                                content = origin_file.read_text(encoding="utf-16").strip()
                            except UnicodeDecodeError:
                                content = origin_file.read_text(encoding="utf-8", errors="ignore").strip()

                            if content:
                                p_origin = Path(content)
                                exe = None
                                if (p_origin / "terminal64.exe").is_file():
                                    exe = p_origin / "terminal64.exe"
                                elif (p_origin / "terminal.exe").is_file():
                                    exe = p_origin / "terminal.exe"

                                if exe:
                                    discovered.append(self._create_terminal_record(
                                        exe_path=exe,
                                        name=exe.parent.name,
                                        source="roaming_origin",
                                        is_portable=False,
                                        data_directory=str(item),
                                    ))
                        except Exception:
                            continue
        except (OSError, PermissionError):
            pass

        return discovered

    # -------------------------------------------------------------------------
    # Vector 4: Active Running Processes
    # -------------------------------------------------------------------------

    def scan_running_processes(self) -> List[DiscoveredTerminal]:
        """
        Discovers currently running MetaTrader instances using OS process enumeration.
        """
        if psutil is None:
            return []

        discovered: List[DiscoveredTerminal] = []
        for proc in psutil.process_iter(["pid", "name", "exe", "cmdline"]):
            try:
                name = (proc.info.get("name") or "").lower()
                if name in MT_EXECUTABLES:
                    exe_path = proc.info.get("exe")
                    if exe_path and Path(exe_path).is_file():
                        p_exe = Path(exe_path)
                        cmdline = proc.info.get("cmdline") or []
                        is_portable = any("/portable" in arg.lower() or "-portable" in arg.lower() for arg in cmdline)

                        discovered.append(self._create_terminal_record(
                            exe_path=p_exe,
                            name=p_exe.parent.name,
                            source="running_process",
                            is_portable=is_portable,
                            is_running=True,
                            process_id=proc.info.get("pid"),
                        ))
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue

        return discovered

    # -------------------------------------------------------------------------
    # Vector 5: Portable Mode & Custom Directories
    # -------------------------------------------------------------------------

    def scan_custom_paths(
        self,
        paths: List[Union[str, Path]],
    ) -> List[DiscoveredTerminal]:
        """
        Scans custom directories or direct executable paths passed by the user or UI.
        Identifies portable installations by checking folder structure (MQL4/MQL5).
        """
        discovered: List[DiscoveredTerminal] = []

        for p_raw in paths:
            p = Path(p_raw)
            if not p.exists():
                continue

            candidates: List[Path] = []
            if p.is_file() and p.name.lower() in MT_EXECUTABLES:
                candidates.append(p)
            elif p.is_dir():
                t64 = p / "terminal64.exe"
                t32 = p / "terminal.exe"
                if t64.is_file():
                    candidates.append(t64)
                if t32.is_file():
                    candidates.append(t32)
                try:
                    for sub in p.iterdir():
                        if sub.is_dir():
                            st64 = sub / "terminal64.exe"
                            st32 = sub / "terminal.exe"
                            if st64.is_file():
                                candidates.append(st64)
                            if st32.is_file():
                                candidates.append(st32)
                except (OSError, PermissionError):
                    pass

            for exe in candidates:
                parent_dir = exe.parent
                is_portable = (parent_dir / "MQL4").is_dir() or (parent_dir / "MQL5").is_dir()
                discovered.append(self._create_terminal_record(
                    exe_path=exe,
                    name=parent_dir.name,
                    source="custom_path",
                    is_portable=is_portable,
                ))

        return discovered

    # -------------------------------------------------------------------------
    # Version, PE Architecture & Metadata Extraction
    # -------------------------------------------------------------------------

    def _create_terminal_record(
        self,
        exe_path: Path,
        name: str,
        source: str,
        is_portable: bool = False,
        is_running: bool = False,
        process_id: Optional[int] = None,
        data_directory: Optional[str] = None,
    ) -> DiscoveredTerminal:
        """Constructs DiscoveredTerminal record with accurate PE machine and platform detection."""
        canon_exe = exe_path.resolve()
        parent_dir = canon_exe.parent

        is_64bit, version = self._inspect_executable_binary(canon_exe)

        if not data_directory:
            if is_portable:
                data_directory = str(parent_dir)
            else:
                data_directory = self._resolve_roaming_data_path(canon_exe) or ""

        build = self._extract_terminal_build(canon_exe)
        ex_path = self._resolve_bridge_path(parent_dir, version)

        return DiscoveredTerminal(
            terminal_id="",  # Assigned dynamically in _assign_ipc_mappings
            platform_type=version,
            name=name or parent_dir.name,
            build_number=build,
            executable_path=str(canon_exe),
            install_dir=str(parent_dir),
            data_dir=data_directory or "",
            is_64bit=is_64bit,
            is_portable=is_portable,
            is_running=is_running,
            named_pipe_name="",
            shm_name="",
            source=source,
            process_id=process_id,
            ex4_or_ex5_path=ex_path,
        )

    def _inspect_executable_binary(self, exe_path: Path) -> Tuple[bool, str]:
        """
        Parses PE header to identify machine architecture (x64 vs x86) and platform (MT4 vs MT5).
        """
        is_64bit = False
        version = "MT5"

        if exe_path.name.lower() == "terminal64.exe":
            is_64bit = True
            version = "MT5"
            return is_64bit, version

        try:
            with open(exe_path, "rb") as f:
                magic = f.read(2)
                if magic == b"MZ":
                    f.seek(0x3C)
                    pe_offset = struct.unpack("<I", f.read(4))[0]
                    f.seek(pe_offset)
                    sig = f.read(4)
                    if sig == b"PE\x00\x00":
                        machine = struct.unpack("<H", f.read(2))[0]
                        if machine == 0x8664:
                            is_64bit = True
                            version = "MT5"
                        elif machine == 0x014C:
                            is_64bit = False
                            version = "MT4"
        except Exception:
            pass

        parent = exe_path.parent
        if (parent / "MQL5").is_dir():
            version = "MT5"
        elif (parent / "MQL4").is_dir():
            version = "MT4"

        return is_64bit, version

    def _extract_terminal_build(self, exe_path: Path) -> int:
        """Attempts to extract terminal build number from binary or configuration."""
        parent = exe_path.parent
        ini_file = parent / "terminal.ini"
        if ini_file.is_file():
            try:
                content = ini_file.read_text(encoding="utf-8", errors="ignore")
                m = re.search(r"build\s*=\s*(\d+)", content, re.IGNORECASE)
                if m:
                    return int(m.group(1))
            except Exception:
                pass

        if exe_path.name.lower() == "terminal64.exe":
            return 4450
        return 1420

    def _resolve_bridge_path(self, parent_dir: Path, version: str) -> Optional[str]:
        """Resolves default expected path for Pipvane Expert Advisor binary."""
        if version == "MT4":
            ex4 = parent_dir / self.DEFAULT_MT4_EX4_REL
            return str(ex4) if ex4.is_file() else None
        else:
            ex5 = parent_dir / self.DEFAULT_MT5_EX5_REL
            return str(ex5) if ex5.is_file() else None

    def _resolve_roaming_data_path(self, exe_path: Path) -> Optional[str]:
        """Resolves %APPDATA%\\MetaQuotes\\Terminal\\<hash> directory for the executable."""
        appdata = os.environ.get("APPDATA")
        if not appdata:
            return None

        term_dir = Path(appdata) / "MetaQuotes" / "Terminal"
        if not term_dir.is_dir():
            return None

        canon_target = str(exe_path.parent).lower()
        try:
            for item in term_dir.iterdir():
                if item.is_dir() and len(item.name) == 32:
                    origin_file = item / "origin.txt"
                    if origin_file.is_file():
                        try:
                            content = origin_file.read_text(encoding="utf-16", errors="ignore").strip()
                            if content.lower() == canon_target:
                                return str(item)
                        except Exception:
                            continue
        except Exception:
            pass

        return None

    # -------------------------------------------------------------------------
    # Dynamic IPC Mapping Assignment
    # -------------------------------------------------------------------------

    def _assign_ipc_mappings(self, terminals: List[DiscoveredTerminal]) -> None:
        """
        Assigns unique, deterministic Shared Memory map names and Named Pipe addresses
        grouped by platform type (e.g. \\\\.\\pipe\\pipvane_mt5_1, Local\\Pipvane_SHM_mt5_1).
        """
        counters: Dict[str, int] = {"MT5": 0, "MT4": 0}

        for term in terminals:
            v_key = term.platform_type.upper()
            counters[v_key] = counters.get(v_key, 0) + 1
            idx = counters[v_key]

            v_slug = v_key.lower()  # "mt5" or "mt4"
            term_id = f"{v_slug}_{idx}"
            term.terminal_id = term_id
            term.named_pipe_name = f"{self.pipe_prefix}{v_slug}_{idx}"
            term.shm_name = f"{self.shm_prefix}{v_slug}_{idx}"

    def _canonical_path(self, path_str: str) -> str:
        """Returns normalized canonical case-folded path for reliable deduplication."""
        return os.path.realpath(os.path.normpath(path_str)).lower()

    # -------------------------------------------------------------------------
    # Terminal Enrollment into IPCDispatcher
    # -------------------------------------------------------------------------

    def connect_terminal(
        self,
        dispatcher: IPCDispatcher,
        terminal_id: str,
        use_mock: Optional[bool] = None,
    ) -> TerminalContext:
        """
        Enrolls a discovered terminal into the IPCDispatcher.
        Performs EX4 integrity verification for MT4 terminals before attachment.
        """
        terminal = self.get_terminal(terminal_id)
        if not terminal:
            raise ValueError(f"Terminal '{terminal_id}' not found in discovery registry")

        ex4_path = terminal.ex4_or_ex5_path if terminal.platform_type == "MT4" else None

        ctx = dispatcher.register_terminal(
            terminal_id=terminal.terminal_id,
            shm_name=terminal.shm_name,
            pipe_name=terminal.named_pipe_name,
            use_mock=use_mock,
            ex4_path=ex4_path,
        )
        logger.info("Connected terminal '%s' to IPCDispatcher", terminal_id)
        return ctx

    def connect_all_discovered(
        self,
        dispatcher: IPCDispatcher,
        max_terminals: int = 7,
        use_mock: Optional[bool] = None,
    ) -> Dict[str, TerminalContext]:
        """
        Connects all discovered terminals up to max_terminals (licensed account limit)
        into the IPCDispatcher.
        """
        connected: Dict[str, TerminalContext] = {}
        terminals = list(self._terminals.values())[:max_terminals]

        for term in terminals:
            ctx = self.connect_terminal(dispatcher, term.terminal_id, use_mock=use_mock)
            connected[term.terminal_id] = ctx

        logger.info("Enrolled %d terminals into IPCDispatcher (limit: %d)", len(connected), max_terminals)
        return connected
