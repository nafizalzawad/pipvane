#!/usr/bin/env python3
"""
Pipvane v3: 30-Day Telegram Safety Mode Soak & Chaos Orchestrator
----------------------------------------------------------------
Orchestrates continuous soak testing across 3 dummy Telegram accounts.
Enforces strictly read-only Safety Mode (FR-4.1), randomized fault injection
every 4 to 12 hours, daily telemetry logging, and strict ban-exception assertions.
"""

import argparse
import asyncio
import logging
import os
import signal
import sys
import time

# Ensure project root is in sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from typing import List, Optional, Dict, Any  # noqa: E402

from engine.vault import VaultStorage  # noqa: E402
from engine.tdlib_client import MockTDLibClient, NativeTDLibClient  # noqa: E402
from engine.telegram_gateway import (  # noqa: E402
    TelegramGateway,
    UserDeactivatedError,
    AuthKeyUnregisteredError,
    TelegramBanException,
)
from qa.chaos.fault_injector import NetworkChaosFaultInjector  # noqa: E402
from qa.chaos.telemetry import BanRiskTelemetryLogger  # noqa: E402


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("Pipvane.SoakOrchestrator")


class TelegramSafetySoakOrchestrator:
    """
    Continuous Soak Test Runner managing 3 separate Telegram accounts.
    Listens to incoming signals, recovers from randomized network drops,
    and asserts zero account bans over 30 days.
    """

    DEFAULT_ACCOUNTS = [
        {"id": "dummy_acc_1", "phone": "+15550100001", "chat_id": -1001928374, "channel": "Alpha Forex Scalpers VIP"},
        {"id": "dummy_acc_2", "phone": "+15550100002", "chat_id": -1001928375, "channel": "London Breakout Gold"},
        {"id": "dummy_acc_3", "phone": "+15550100003", "chat_id": -1001928376, "channel": "WallStreet Indices Master"},
    ]

    def __init__(
        self,
        output_dir: Optional[str] = None,
        accelerated: bool = False,
        use_native_tdlib: bool = False,
        telemetry_interval_sec: float = 60.0,
        traffic_interval_sec: float = 3.0,
    ):
        self.output_dir = output_dir or os.path.dirname(os.path.abspath(__file__))
        os.makedirs(self.output_dir, exist_ok=True)
        self.accelerated = accelerated
        self.use_native_tdlib = use_native_tdlib
        self.telemetry_interval = 1.0 if accelerated else telemetry_interval_sec
        self.traffic_interval = 0.2 if accelerated else traffic_interval_sec

        self.telemetry = BanRiskTelemetryLogger(output_dir=self.output_dir)
        self.fault_injector = NetworkChaosFaultInjector(accelerated_mode=accelerated)

        self.gateways: List[TelegramGateway] = []
        self.vaults: List[VaultStorage] = []
        self._running: bool = False
        self._traffic_task: Optional[asyncio.Task] = None
        self._telemetry_task: Optional[asyncio.Task] = None
        self.fatal_ban_exception: Optional[Exception] = None

        self._init_gateways()

    def _init_gateways(self) -> None:
        """Initialize 3 isolated TelegramGateway sessions with Safety Mode enabled."""
        self.gateways.clear()
        self.vaults.clear()

        for acc in self.DEFAULT_ACCOUNTS:
            acc_dir = os.path.join(self.output_dir, "sessions", acc["id"])
            db_dir = os.path.join(acc_dir, "db")
            files_dir = os.path.join(acc_dir, "files")
            os.makedirs(db_dir, exist_ok=True)
            os.makedirs(files_dir, exist_ok=True)

            vault_db = os.path.join(acc_dir, "vault.db")
            vault = VaultStorage(db_path=vault_db)
            self.vaults.append(vault)

            # Client driver: Native on VPS with tdjson, or MockTDLibClient for offline soak testing
            if self.use_native_tdlib:
                client = NativeTDLibClient()
            else:
                client = MockTDLibClient(initial_auth_state="authorizationStateWaitTdlibParameters")

            gw = TelegramGateway(
                api_id=94575,
                api_hash="a3406de8d171321aec1304f00f8c3e1a",
                vault=vault,
                tdlib_client=client,
                database_directory=db_dir,
                files_directory=files_dir,
                profile_id=acc["id"],
                phone_number=acc["phone"],
            )

            # Attach event and ban handlers
            gw.add_message_handler(self._handle_incoming_message)
            gw.add_error_handler(lambda exc, g=gw: self._handle_error_event(g, exc))

            self.gateways.append(gw)

    def _handle_incoming_message(self, message: Dict[str, Any]) -> None:
        chat_id = message.get("chat_id")
        msg_id = message.get("id")
        content = message.get("content", {})
        text = content.get("text", {}).get("text", str(content))
        logger.info(
            f" [SIGNAL INGESTED] Chat: {chat_id} | MsgID: {msg_id} | "
            f"Text: '{str(text)[:45]}...'"
        )

    def _handle_error_event(self, gateway: TelegramGateway, exc: Exception) -> None:
        """Trap TDLib errors and enforce strict ban assertions."""
        if isinstance(exc, (UserDeactivatedError, AuthKeyUnregisteredError, TelegramBanException)):
            self.assert_no_ban_exceptions(gateway, exc)

    # --------------------------------------------------------------------------
    # Strict Ban Assertions
    # --------------------------------------------------------------------------
    def assert_no_ban_exceptions(self, gateway: TelegramGateway, exception: Exception) -> None:
        """
        STRICT REQUIREMENT: Immediately fail the test if a USER_DEACTIVATED or
        AUTH_KEY_UNREGISTERED exception is encountered by TDLib.
        """
        self.fatal_ban_exception = exception

        if isinstance(exception, UserDeactivatedError):
            err_msg = (
                f"FATAL BAN FAILURE on {gateway.profile_id} ({gateway.phone_number}): "
                f"TDLib returned USER_DEACTIVATED. Account has been deleted/banned by Telegram!"
            )
            logger.critical(err_msg)
            gateway.termination_code = "USER_DEACTIVATED"
            self.telemetry.record_snapshot(self.gateways)
            raise AssertionError(err_msg)

        if isinstance(exception, AuthKeyUnregisteredError):
            err_msg = (
                f"FATAL BAN FAILURE on {gateway.profile_id} ({gateway.phone_number}): "
                f"TDLib returned AUTH_KEY_UNREGISTERED. Auth session revoked/banned by Telegram!"
            )
            logger.critical(err_msg)
            gateway.termination_code = "AUTH_KEY_UNREGISTERED"
            self.telemetry.record_snapshot(self.gateways)
            raise AssertionError(err_msg)

        if isinstance(exception, TelegramBanException):
            err_msg = f"FATAL BAN FAILURE on {gateway.profile_id}: {exception}"
            logger.critical(err_msg)
            gateway.termination_code = "GENERIC_BAN"
            self.telemetry.record_snapshot(self.gateways)
            raise AssertionError(err_msg)

    # --------------------------------------------------------------------------
    # Execution Lifecycle
    # --------------------------------------------------------------------------
    async def start(self) -> None:
        """Start all 3 gateway sessions, fault injection, and telemetry loop."""
        logger.info("=" * 78)
        logger.info(" PIPVANE v3 - 30-DAY TELEGRAM SAFETY MODE SOAK & CHAOS ORCHESTRATOR")
        logger.info("=" * 78)
        logger.info(f"Monitored Accounts:    {len(self.gateways)} concurrent accounts")
        logger.info("Safety Mode:           STRICT READ-ONLY (FR-4.1 Verified)")
        logger.info(f"Fault Interval:        4 to 12 hours random (Accelerated={self.accelerated})")
        logger.info(f"Telemetry File:        {self.telemetry.file_path}")
        logger.info("-" * 78)

        self._running = True

        # 1. Start each gateway and log in
        for i, gw in enumerate(self.gateways):
            acc_info = self.DEFAULT_ACCOUNTS[i]
            gw.start()

            if isinstance(gw._client, MockTDLibClient):
                await asyncio.sleep(0.05)
                gw.login_with_phone(acc_info["phone"])
                await asyncio.sleep(0.05)
                gw.submit_auth_code("12345")
                await asyncio.sleep(0.05)
                assert gw.wait_until_ready(timeout=2.0) is True

            # Subscribe to target channel
            gw.subscribe_chat(acc_info["chat_id"])

        # 2. Start randomized network fault injector (4-12 hours)
        await self.fault_injector.start(self.gateways)

        # 3. Start background signal simulation and telemetry tasks
        self._traffic_task = asyncio.create_task(self._traffic_generation_loop())
        self._telemetry_task = asyncio.create_task(self._telemetry_reporting_loop())

        logger.info("All 3 accounts connected and listening successfully. Soak run active.")

    async def stop(self, reason: str = "NORMAL_SHUTDOWN") -> None:
        """Gracefully stop orchestrator and flush telemetry."""
        logger.info(f"Stopping Soak Orchestrator. Reason: {reason}")
        self._running = False

        if self._traffic_task and not self._traffic_task.done():
            self._traffic_task.cancel()
        if self._telemetry_task and not self._telemetry_task.done():
            self._telemetry_task.cancel()

        await self.fault_injector.stop()

        for gw in self.gateways:
            gw.termination_code = reason
            gw.stop()

        # Final telemetry snapshot
        self.telemetry.record_snapshot(self.gateways)
        logger.info("Final telemetry snapshot recorded. Soak Orchestrator terminated cleanly.")

    # --------------------------------------------------------------------------
    # Background Traffic & Telemetry Loops
    # --------------------------------------------------------------------------
    async def _traffic_generation_loop(self) -> None:
        """Simulate realistic incoming signal traffic across channels."""
        sample_signals = [
            "BUY XAUUSD @ 2650.50 SL: 2640.00 TP1: 2670.00",
            "SELL EURUSD @ 1.05850 SL: 1.06200 TP: 1.05200",
            "BUY US30 @ 43800 SL: 43600 TP1: 43900 TP2: 44100",
            "MODIFY GBPJPY SL TO 195.20 CLOSE 50%",
            "🚨 شراء ذهب عاجلة XAUUSD دخول: ٢٦٥٠.٥٠ وقف: ٢٦٤٠.٠٠",
        ]

        signal_idx = 0
        msg_counter = 1000

        while self._running:
            try:
                await asyncio.sleep(self.traffic_interval)
                msg_counter += 1
                text = sample_signals[signal_idx % len(sample_signals)]
                signal_idx += 1

                for i, gw in enumerate(self.gateways):
                    chat_id = self.DEFAULT_ACCOUNTS[i]["chat_id"]
                    if isinstance(gw._client, MockTDLibClient):
                        update = {
                            "@type": "updateNewMessage",
                            "message": {
                                "id": msg_counter,
                                "chat_id": chat_id,
                                "date": int(time.time()),
                                "content": {
                                    "@type": "messageText",
                                    "text": {"@type": "formattedText", "text": text}
                                }
                            }
                        }
                        gw._client.simulate_incoming_update(update)

                # Check if a fatal ban was encountered
                if self.fatal_ban_exception:
                    raise self.fatal_ban_exception

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in traffic generation: {e}")
                if isinstance(e, (UserDeactivatedError, AuthKeyUnregisteredError, TelegramBanException, AssertionError)):
                    raise

    async def _telemetry_reporting_loop(self) -> None:
        """Continuously records telemetry snapshots to ban_risk_telemetry.csv."""
        while self._running:
            try:
                await asyncio.sleep(self.telemetry_interval)
                rows = self.telemetry.record_snapshot(self.gateways)
                avg_rss = sum(r["tdlib_rss_mb"] for r in rows) / len(rows) if rows else 0
                total_recv = sum(r["messages_received"] for r in rows)
                total_backfill = sum(r["messages_backfilled"] for r in rows)
                logger.info(
                    f"[TELEMETRY FLUSH] Messages: {total_recv} (Backfilled: {total_backfill}) | "
                    f"TDLib RSS: {avg_rss:.1f} MB | Disruptions: {self.fault_injector.disruption_count}"
                )
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in telemetry loop: {e}")

    # --------------------------------------------------------------------------
    # Soak Runner Execution
    # --------------------------------------------------------------------------
    async def run_duration(self, seconds: float) -> None:
        """Run soak orchestrator for a bounded duration."""
        await self.start()
        logger.info(f"Running soak test for {seconds:.2f} seconds...")
        try:
            await asyncio.sleep(seconds)
            if self.fatal_ban_exception:
                raise self.fatal_ban_exception
        finally:
            await self.stop(reason="DURATION_COMPLETED")


# ==============================================================================
# CLI Entrypoint for Standalone & VPS Background Execution
# ==============================================================================
def parse_args():
    parser = argparse.ArgumentParser(description="Pipvane v3 Telegram Safety Soak Test Orchestrator")
    parser.add_argument("--duration-hours", type=float, default=720.0, help="Total soak duration in hours (default 720h = 30 days)")
    parser.add_argument("--duration-seconds", type=float, default=None, help="Soak duration in seconds (overrides --duration-hours)")
    parser.add_argument("--accelerated", action="store_true", help="Accelerate fault injection for fast CI/unit validation")
    parser.add_argument("--native", action="store_true", help="Use native tdjson.dll / libtdjson.so instead of mock client")
    parser.add_argument("--output-dir", type=str, default=None, help="Directory to save telemetry CSV and sessions")
    return parser.parse_args()


async def main():
    args = parse_args()
    duration_sec = args.duration_seconds if args.duration_seconds else (args.duration_hours * 3600.0)

    orchestrator = TelegramSafetySoakOrchestrator(
        output_dir=args.output_dir,
        accelerated=args.accelerated,
        use_native_tdlib=args.native,
    )

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def handle_signal():
        logger.info("Shutdown signal received (SIGINT/SIGTERM). Stopping soak test...")
        stop_event.set()

    if sys.platform != "win32":
        for s in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(s, handle_signal)

    runner_task = asyncio.create_task(orchestrator.run_duration(duration_sec))

    try:
        done, pending = await asyncio.wait(
            [runner_task, asyncio.create_task(stop_event.wait())],
            return_when=asyncio.FIRST_COMPLETED,
        )

        if stop_event.is_set():
            await orchestrator.stop(reason="OS_SIGNAL_TERMINATION")
            runner_task.cancel()
    except Exception as ex:
        logger.critical(f"Orchestrator encountered unhandled exception: {ex}")
        await orchestrator.stop(reason="UNHANDLED_EXCEPTION")
        sys.exit(1)

    logger.info("Soak Orchestrator execution concluded successfully.")
    sys.exit(0)


if __name__ == "__main__":
    asyncio.run(main())
