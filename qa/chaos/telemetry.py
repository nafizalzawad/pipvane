#!/usr/bin/env python3
"""
Pipvane v3: Telegram Soak & Chaos Telemetry Logger
--------------------------------------------------
Records real-time session health, TDLib memory footprint, flood-wait occurrences,
uptime metrics, and ban-risk telemetry to a daily CSV file: `ban_risk_telemetry.csv`.
"""

import csv
import datetime
import os
import sys
import psutil

# Ensure project root is in sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from typing import Dict, List, Optional  # noqa: E402

from engine.telegram_gateway import TelegramGateway  # noqa: E402


class BanRiskTelemetryLogger:
    """
    Manages daily telemetry logging for Telegram accounts undergoing continuous
    30-day soak and chaos testing.
    """

    CSV_COLUMNS = [
        "timestamp",
        "date",
        "account_id",
        "phone_number",
        "session_uptime_seconds",
        "session_uptime_hours",
        "connection_state",
        "flood_wait_count",
        "flood_wait_total_seconds",
        "network_disruptions_count",
        "messages_received",
        "messages_backfilled",
        "termination_code",
        "tdlib_rss_mb",
        "tdlib_vms_mb",
        "cpu_percent",
        "ban_risk_level",
    ]

    def __init__(self, output_dir: Optional[str] = None, file_name: str = "ban_risk_telemetry.csv"):
        if not output_dir:
            output_dir = os.path.dirname(os.path.abspath(__file__))
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        self.file_path = os.path.join(self.output_dir, file_name)

        self._process = psutil.Process(os.getpid())
        self._ensure_header()

    def _ensure_header(self) -> None:
        """Create CSV file with header if it doesn't exist."""
        if not os.path.exists(self.file_path) or os.path.getsize(self.file_path) == 0:
            with open(self.file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(self.CSV_COLUMNS)

    def get_process_memory_mb(self) -> Dict[str, float]:
        """Query process memory usage in Megabytes (RSS and VMS)."""
        try:
            mem = self._process.memory_info()
            cpu = self._process.cpu_percent(interval=None)
            return {
                "rss_mb": round(mem.rss / (1024 * 1024), 2),
                "vms_mb": round(mem.vms / (1024 * 1024), 2),
                "cpu_percent": round(cpu, 2),
            }
        except Exception:
            return {"rss_mb": 0.0, "vms_mb": 0.0, "cpu_percent": 0.0}

    def assess_ban_risk(self, gw: TelegramGateway) -> str:
        """
        Assess current ban risk level based on termination code and flood wait occurrences.
        """
        if gw.termination_code in ["USER_DEACTIVATED", "AUTH_KEY_UNREGISTERED"]:
            return "CRITICAL_BANNED"
        if gw.flood_wait_count > 5 or gw.flood_wait_total_seconds > 120:
            return "ELEVATED_FLOOD_WAIT"
        return "LOW_SAFE"

    def record_snapshot(self, gateways: List[TelegramGateway]) -> List[Dict[str, any]]:
        """
        Collect telemetry across all active Telegram accounts and append to CSV.
        """
        now = datetime.datetime.now(datetime.timezone.utc)
        iso_ts = now.isoformat()
        date_str = now.strftime("%Y-%m-%d")
        mem = self.get_process_memory_mb()

        rows = []
        with open(self.file_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)

            for gw in gateways:
                uptime_sec = gw.get_uptime_seconds()
                uptime_hrs = round(uptime_sec / 3600.0, 4)
                ban_risk = self.assess_ban_risk(gw)

                row_dict = {
                    "timestamp": iso_ts,
                    "date": date_str,
                    "account_id": gw.profile_id,
                    "phone_number": gw.phone_number,
                    "session_uptime_seconds": round(uptime_sec, 2),
                    "session_uptime_hours": uptime_hrs,
                    "connection_state": gw.connection_state,
                    "flood_wait_count": gw.flood_wait_count,
                    "flood_wait_total_seconds": gw.flood_wait_total_seconds,
                    "network_disruptions_count": gw.network_disruptions_count,
                    "messages_received": gw.messages_received_count,
                    "messages_backfilled": gw.messages_backfilled_count,
                    "termination_code": gw.termination_code,
                    "tdlib_rss_mb": mem["rss_mb"],
                    "tdlib_vms_mb": mem["vms_mb"],
                    "cpu_percent": mem["cpu_percent"],
                    "ban_risk_level": ban_risk,
                }

                writer.writerow([row_dict[col] for col in self.CSV_COLUMNS])
                rows.append(row_dict)

        return rows

    def read_latest_telemetry(self, limit: int = 10) -> List[Dict[str, str]]:
        """Read recent entries from CSV file."""
        if not os.path.exists(self.file_path):
            return []

        with open(self.file_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            entries = list(reader)
            return entries[-limit:]
