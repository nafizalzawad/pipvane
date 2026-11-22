"""
Pipvane v3 - Automated Notification Load & Batching Test Suite (Sprint 18 Directive)
=====================================================================================
Validates that the NotificationEngine intelligently batches high-frequency bursts
of trades into a single consolidated digest message, guaranteeing strict compliance
with Telegram Bot API rate limits (FR-VR-3.1 / VR-4.2).

Sprint 18 Mandates:
1. Spam Simulation:
   - Feeds a burst of 50 simultaneous mock signals into the Engine, resulting in 50 executed trades.
2. Batching Assertion:
   - Asserts that NotificationEngine DOES NOT send 50 individual Telegram messages (which would
     breach Telegram API rate limits).
   - Asserts that it intelligently batches the trades into a SINGLE digest message if they occur
     within a 2-second window.
3. Rate-Limit Compliance:
   - Proves 100% compliance with Telegram Bot API quotas (<= 1 msg/sec per chat, <= 30 msgs/sec global).
"""

from __future__ import annotations

import asyncio
import logging
import time
import unittest
from typing import Any, Dict, List, Optional

import pytest

from engine.ipc_protocol import ActionType, ExecutionFeedback, ExecutionFlags, ExecutionStatus, OrderIntent, OrderSide
from engine.notifications import (
    DEFAULT_BATCH_WINDOW_SECONDS,
    DigestFormatter,
    MockTelegramBotClient,
    NotificationEngine,
    OutboundTelegramMessage,
    TelegramRateLimiter,
    TradeNotificationEvent,
)


# =============================================================================
# MOCK BROKER DISPATCHER FOR INTEGRATION TESTING
# =============================================================================

class MockBrokerDispatcherForNotifications:
    """Mock dispatcher that executes orders synchronously or with sub-millisecond latency."""

    def __init__(self):
        self._next_ticket = 10001
        self._feedback_callbacks: List[Any] = []
        self._account_callbacks: List[Any] = []
        self.dispatched_orders: List[Tuple[str, OrderIntent]] = []

    def on_execution_feedback(self, cb: Any) -> None:
        self._feedback_callbacks.append(cb)

    def on_account_state(self, cb: Any) -> None:
        self._account_callbacks.append(cb)

    def get_registered_terminals(self) -> List[str]:
        return ["demo_terminal_1", "demo_terminal_2"]

    def can_route_to_terminal(self, terminal_id: str) -> bool:
        return True

    def dispatch_order_intent(self, terminal_id: str, intent: OrderIntent) -> bool:
        self.dispatched_orders.append((terminal_id, intent))
        ticket = self._next_ticket
        self._next_ticket += 1

        feedback = ExecutionFeedback(
            signal_id=intent.signal_id,
            broker_retcode=10009,  # TRADE_RETCODE_DONE
            order_ticket=ticket,
            executed_volume=intent.volume,
            executed_price=intent.price or 1.0850,
            execution_status=ExecutionStatus.FILLED,
            slippage_pips=0.2,
            execution_latency_us=45,
            error_description="Order executed successfully",
        )

        # Dispatch feedback to registered listeners
        for cb in self._feedback_callbacks:
            try:
                cb(terminal_id, feedback)
            except Exception:
                pass
        return True


# =============================================================================
# TEST SUITE: SPRINT 18 NOTIFICATION LOAD TESTS
# =============================================================================

class TestNotificationBatchingAndRateLimits(unittest.IsolatedAsyncioTestCase):
    """Asynchronous test suite validating 2-second batching and rate-limit compliance."""

    async def asyncSetUp(self):
        self.chat_id = "test_user_chat_777"
        self.bot_client = MockTelegramBotClient(simulate_rate_limit_errors=True)
        self.engine = NotificationEngine(
            bot_client=self.bot_client,
            default_chat_id=self.chat_id,
            batch_window_seconds=2.0,  # Strict 2-second batching window per Sprint 18
            enable_rate_limiter=True,
        )

    async def asyncTearDown(self):
        await self.engine.stop()

    # -------------------------------------------------------------------------
    # Task 1 & 2: Spam Simulation & Batching Assertion
    # -------------------------------------------------------------------------

    async def test_01_spam_simulation_50_trades_batched_into_single_digest(self):
        """
        SPRINT 18 PRIMARY MANDATE:
        1. Spam Simulation: Burst of 50 simultaneous mock signals resulting in 50 executed trades.
        2. Batching Assertion: NotificationEngine does NOT send 50 individual Telegram messages.
           Asserts that it intelligently batches the trades into a single digest message within the 2-second window.
        3. Rate-Limit Compliance: Proves 0 rate-limit violations.
        """
        burst_size = 50
        symbols = ["EURUSD", "GBPUSD", "USDJPY", "XAUUSD", "BTCUSD", "AUDUSD", "NZDUSD", "USDCAD"]

        # Step 1: Generate 50 simultaneous trade execution events
        start_time = time.time()
        for i in range(1, burst_size + 1):
            sym = symbols[i % len(symbols)]
            side = "BUY" if i % 2 == 0 else "SELL"
            vol = round(0.10 + (i * 0.02), 2)
            price = round(1.0800 + (i * 0.0010), 4)

            event = TradeNotificationEvent(
                signal_id=i,
                ticket=20000 + i,
                symbol=sym,
                side=side,
                volume=vol,
                price=price,
                stop_loss=price - 0.0030 if side == "BUY" else price + 0.0030,
                take_profit=price + 0.0060 if side == "BUY" else price - 0.0060,
                terminal_id="demo_terminal_1",
                timestamp=time.time(),
            )
            # Enqueue into notification engine (simulating burst arrival)
            await self.engine.notify_trade(event)

        # Confirm all 50 trades were received and buffered within sub-millisecond time
        self.assertEqual(self.engine.total_trades_received, burst_size)
        self.assertEqual(self.engine.pending_count, burst_size)
        # Verify 0 messages sent yet (awaiting the 2.0-second batch window)
        self.assertEqual(len(self.bot_client.sent_messages), 0)

        # Step 2: Await the 2.0-second batch window (wait 2.2 seconds to allow timer to fire)
        await asyncio.sleep(2.2)

        # Step 3: Batching Assertion:
        # Asserts that NotificationEngine did NOT send 50 individual Telegram messages!
        sent_messages = self.bot_client.sent_messages
        self.assertNotEqual(
            len(sent_messages),
            burst_size,
            "CRITICAL FAILURE: NotificationEngine sent 50 individual messages, violating Telegram rate limits!",
        )
        self.assertEqual(
            len(sent_messages),
            1,
            f"Expected exactly 1 consolidated digest message, but received {len(sent_messages)}",
        )

        digest_message = sent_messages[0]
        self.assertTrue(digest_message.is_digest)
        self.assertEqual(digest_message.trade_count, burst_size)
        self.assertEqual(digest_message.chat_id, self.chat_id)

        # Step 4: Verify Digest Content Integrity:
        # Asserts that all 50 executed trades are documented in the digest message
        self.assertIn("PIPVANE DIGEST: 50 Trades Executed", digest_message.text)
        for i in range(1, burst_size + 1):
            expected_ticket = f"#{20000 + i}"
            self.assertIn(
                expected_ticket,
                digest_message.text,
                f"Digest message is missing ticket {expected_ticket}",
            )

        # Step 5: Telegram Rate-Limit Compliance Assertion:
        # Asserts 0 rate limit violations occurred during delivery
        self.assertEqual(
            len(self.bot_client.rate_limit_violations),
            0,
            f"Rate limit violations detected: {self.bot_client.rate_limit_violations}",
        )
        # Asserts telemetry confirms 49 API calls were saved
        self.assertEqual(self.engine.total_messages_saved, 49)
        self.assertEqual(self.engine.total_digests_sent, 1)
        self.assertEqual(self.engine.total_single_messages_sent, 0)

    # -------------------------------------------------------------------------
    # Test 2: Negative Proof - Unbatched 50 Messages Trigger Rate Limit Breaches
    # -------------------------------------------------------------------------

    async def test_02_negative_proof_unbatched_spam_violates_telegram_rate_limits(self):
        """
        Proves why batching is essential: Sending 50 individual messages directly
        to a single chat within 1 second triggers Telegram API rate-limit violations (HTTP 429).
        """
        unbatched_client = MockTelegramBotClient(simulate_rate_limit_errors=False)

        # Attempt to blast 50 individual messages directly
        for i in range(50):
            await unbatched_client.send_message(
                chat_id=self.chat_id,
                text=f"Trade #{i} Executed",
            )

        # Without batching, all 49 subsequent messages violate the 1 msg/sec Telegram rate limit
        self.assertTrue(len(unbatched_client.rate_limit_violations) > 0)
        self.assertEqual(len(unbatched_client.rate_limit_violations), 49)
        self.assertIn("RATE LIMIT BREACH", unbatched_client.rate_limit_violations[0])

    # -------------------------------------------------------------------------
    # Test 3: Isolated Single Trade Execution (Normal Non-Spam Flow)
    # -------------------------------------------------------------------------

    async def test_03_isolated_single_trade_sends_single_notification(self):
        """
        Validates that a single trade occurring in isolation (no spam) is formatted
        as an individual trade notification (not a digest) once the window expires.
        """
        single_event = TradeNotificationEvent(
            signal_id=999,
            ticket=55555,
            symbol="EURUSD",
            side="BUY",
            volume=0.50,
            price=1.08500,
            stop_loss=1.08200,
            take_profit=1.09100,
            terminal_id="demo_terminal_1",
        )

        await self.engine.notify_trade(single_event)
        self.assertEqual(self.engine.pending_count, 1)

        # Wait for 2.0s window
        await asyncio.sleep(2.2)

        sent = self.bot_client.sent_messages
        self.assertEqual(len(sent), 1)
        msg = sent[0]
        self.assertFalse(msg.is_digest)
        self.assertEqual(msg.trade_count, 1)
        self.assertIn("TRADE EXECUTED", msg.text)
        self.assertIn("#55555", msg.text)
        self.assertIn("EURUSD", msg.text)
        self.assertEqual(self.engine.total_single_messages_sent, 1)
        self.assertEqual(self.engine.total_digests_sent, 0)
        self.assertEqual(self.engine.total_messages_saved, 0)

    # -------------------------------------------------------------------------
    # Test 4: Two Consecutive Bursts Separated by 3 Seconds
    # -------------------------------------------------------------------------

    async def test_04_two_consecutive_bursts_yield_two_digests(self):
        """
        Burst 1 (30 trades at t=0s) -> Flushes as Digest 1 at t=2.0s.
        Burst 2 (20 trades at t=2.5s) -> Flushes as Digest 2 at t=4.5s.
        Total messages sent: Exactly 2 digests (preventing 48 individual messages).
        """
        # Burst 1: 30 trades
        for i in range(1, 31):
            await self.engine.notify_trade(TradeNotificationEvent(
                signal_id=i, ticket=30000 + i, symbol="XAUUSD", side="BUY", volume=0.1, price=2030.0
            ))

        # Await Flush 1
        await asyncio.sleep(2.2)
        self.assertEqual(len(self.bot_client.sent_messages), 1)
        self.assertEqual(self.bot_client.sent_messages[0].trade_count, 30)

        # Burst 2: 20 trades
        for i in range(31, 51):
            await self.engine.notify_trade(TradeNotificationEvent(
                signal_id=i, ticket=30000 + i, symbol="GBPUSD", side="SELL", volume=0.2, price=1.2650
            ))

        # Await Flush 2
        await asyncio.sleep(2.2)
        self.assertEqual(len(self.bot_client.sent_messages), 2)
        self.assertEqual(self.bot_client.sent_messages[1].trade_count, 20)

        self.assertEqual(self.engine.total_digests_sent, 2)
        self.assertEqual(self.engine.total_trades_received, 50)
        self.assertEqual(self.engine.total_messages_saved, 48)
        self.assertEqual(len(self.bot_client.rate_limit_violations), 0)

    # -------------------------------------------------------------------------
    # Test 5: Multi-Chat Isolation Under High Load
    # -------------------------------------------------------------------------

    async def test_05_multi_chat_isolation_under_burst_load(self):
        """
        Simultaneous burst of 50 trades partitioned across 2 distinct user chats:
        Chat A receives 25 trades -> Batched into 1 digest.
        Chat B receives 25 trades -> Batched into 1 digest.
        Total 2 messages sent across system (not 50).
        """
        chat_a = "user_chat_alpha"
        chat_b = "user_chat_beta"

        for i in range(1, 51):
            target_chat = chat_a if i % 2 == 1 else chat_b
            await self.engine.notify_trade(
                TradeNotificationEvent(
                    signal_id=i,
                    ticket=40000 + i,
                    symbol="BTCUSD",
                    side="BUY",
                    volume=0.05,
                    price=90000.0,
                ),
                chat_id=target_chat,
            )

        # Await batch window
        await asyncio.sleep(2.2)

        sent = self.bot_client.sent_messages
        self.assertEqual(len(sent), 2)

        chat_a_msgs = [m for m in sent if m.chat_id == chat_a]
        chat_b_msgs = [m for m in sent if m.chat_id == chat_b]

        self.assertEqual(len(chat_a_msgs), 1)
        self.assertEqual(len(chat_b_msgs), 1)

        self.assertEqual(chat_a_msgs[0].trade_count, 25)
        self.assertEqual(chat_b_msgs[0].trade_count, 25)

        self.assertEqual(len(self.bot_client.rate_limit_violations), 0)

    # -------------------------------------------------------------------------
    # Test 6: End-to-End Orchestrator Integration (50 Concurrent Orders)
    # -------------------------------------------------------------------------

    async def test_06_end_to_end_orchestrator_integration_50_concurrent_orders(self):
        """
        End-to-End Integration:
        Spins up ExecutionOrchestrator connected to MockBrokerDispatcher and NotificationEngine.
        Fires 50 concurrent submit_order calls via asyncio.gather.
        Asserts:
        1. All 50 orders execute and return valid Ticket IDs.
        2. NotificationEngine captures all 50 execution events and delivers exactly 1 digest.
        """
        from engine.execution_orchestrator import ExecutionOrchestrator, TerminalExecutionConfig
        from engine.risk_engine import RiskEngine

        dispatcher = MockBrokerDispatcherForNotifications()
        risk_engine = RiskEngine()
        orchestrator = ExecutionOrchestrator(
            ipc_dispatcher=dispatcher,
            risk_engine=risk_engine,
            notification_engine=self.engine,
            db_path=":memory:",
        )
        orchestrator.register_terminal_config(TerminalExecutionConfig(
            terminal_id="demo_terminal_1",
            platform_type=1,  # MT5
            supports_hedging=True,
        ))

        # Launch 50 concurrent order submissions
        tasks = []
        for i in range(1, 51):
            intent = OrderIntent(
                symbol="EURUSD",
                side=OrderSide.BUY,
                action_type=ActionType.MARKET_ENTRY,
                volume=0.10,
                price=1.08500,
                signal_id=5000 + i,
                magic_number=888888,
                flags=int(ExecutionFlags.IOC),
            )
            tasks.append(orchestrator.submit_order("demo_terminal_1", intent))

        results = await asyncio.gather(*tasks)

        # All 50 orders executed successfully on broker
        self.assertEqual(len(results), 50)
        self.assertTrue(all(r.success for r in results))
        self.assertEqual(len({r.ticket for r in results}), 50)

        # Wait for the 2.0-second batch window to elapse
        await asyncio.sleep(2.2)

        # Verify NotificationEngine sent exactly 1 digest message
        sent = self.bot_client.sent_messages
        self.assertEqual(len(sent), 1)
        digest = sent[0]
        self.assertTrue(digest.is_digest)
        self.assertEqual(digest.trade_count, 50)
        self.assertIn("PIPVANE DIGEST: 50 Trades Executed", digest.text)
        self.assertEqual(len(self.bot_client.rate_limit_violations), 0)

    # -------------------------------------------------------------------------
    # Test 7: Telegram 4096-Character Limit Truncation / Chunking Protection
    # -------------------------------------------------------------------------

    async def test_07_extreme_spam_120_trades_respects_telegram_message_length_limit(self):
        """
        Extreme load test: Burst of 120 simultaneous trades.
        Ensures that when the digest length exceeds Telegram's 4096-character limit,
        the formatter cleanly splits into multipart digests without text truncation or HTML errors.
        """
        for i in range(1, 121):
            await self.engine.notify_trade(TradeNotificationEvent(
                signal_id=i,
                ticket=60000 + i,
                symbol="XAUUSD",
                side="BUY" if i % 2 == 0 else "SELL",
                volume=0.5,
                price=2050.25,
            ))

        await asyncio.sleep(2.2)

        sent = self.bot_client.sent_messages
        self.assertTrue(len(sent) >= 1)

        # Every sent message must strictly adhere to Telegram's 4096 character maximum
        for msg in sent:
            self.assertLessEqual(
                len(msg.text),
                4096,
                f"Telegram message exceeded 4096 characters (actual: {len(msg.text)})",
            )
            self.assertTrue(msg.is_digest)

        # Total trades across the multipart digest equals 120
        total_batched = sum(m.trade_count for m in sent)
        self.assertEqual(total_batched, 120)


if __name__ == "__main__":
    unittest.main()
