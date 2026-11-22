"""
Pipvane v3: Broker Error Translator Subsystem (Sprint 23).
===========================================================
Maps arcane broker return codes (MT4, MT5, cTrader) into plain-language
explanations and concrete actionable user advice ("Next Action").

Directives (Sprint 23):
1. Error Dictionary: Translation map for every possible MT4/MT5/cTrader error code.
2. Plain English Mapping:
   - MT4 130: "Stop Loss is too close to the current price. Broker rejected the trade."
   - MT5 10015: "Invalid price. The requested entry price is no longer available."
3. Actionable Output: Include a "Next Action" string for each error
   (e.g., "Check your Broker's Freeze Levels in Settings").
4. Integration: Integrated directly into NotificationEngine so users receive clean alerts.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union

logger = logging.getLogger("pipvane.engine.error_translator")


@dataclass(frozen=True)
class TranslatedError:
    """Standardized translated error structure providing actionable advice."""
    code: Union[int, str]
    platform: str  # "MT4", "MT5", "cTrader", "Generic"
    raw_name: str  # e.g. "ERR_INVALID_STOPS", "TRADE_RETCODE_INVALID_PRICE"
    plain_english: str
    next_action: str
    severity: str = "ERROR"  # "INFO", "WARNING", "ERROR", "CRITICAL"
    is_retryable: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "code": self.code,
            "platform": self.platform,
            "raw_name": self.raw_name,
            "plain_english": self.plain_english,
            "next_action": self.next_action,
            "severity": self.severity,
            "is_retryable": self.is_retryable,
        }

    def format_user_message(self, symbol: Optional[str] = None) -> str:
        sym_str = f" for {symbol}" if symbol else ""
        return (
            f"❌ Broker Error ({self.platform} #{self.code}: {self.raw_name}){sym_str}\n"
            f"• Issue: {self.plain_english}\n"
            f"• Next Action: {self.next_action}"
        )


# =============================================================================
# 1. MT4 ERROR DICTIONARY (MQL4 Server Return Codes)
# =============================================================================

MT4_ERROR_DICT: Dict[int, Dict[str, Any]] = {
    0: {
        "raw_name": "ERR_NO_ERROR",
        "plain_english": "The trade request completed successfully without errors.",
        "next_action": "No action required.",
        "severity": "INFO",
        "is_retryable": False,
    },
    1: {
        "raw_name": "ERR_NO_RESULT",
        "plain_english": "Trade request returned with no result.",
        "next_action": "Verify position in terminal before submitting duplicate orders.",
        "severity": "WARNING",
        "is_retryable": True,
    },
    2: {
        "raw_name": "ERR_COMMON_ERROR",
        "plain_english": "Common broker trade server error encountered.",
        "next_action": "Wait a few seconds for server synchronization and retry.",
        "severity": "ERROR",
        "is_retryable": True,
    },
    3: {
        "raw_name": "ERR_INVALID_TRADE_PARAMETERS",
        "plain_english": "Invalid trade parameters passed to broker (e.g. invalid price or volume).",
        "next_action": "Check symbol volume precision and order parameters in Risk Settings.",
        "severity": "ERROR",
        "is_retryable": False,
    },
    4: {
        "raw_name": "ERR_SERVER_BUSY",
        "plain_english": "Broker trade server is temporarily overloaded.",
        "next_action": "Allow the engine retry loop to execute with exponential backoff.",
        "severity": "WARNING",
        "is_retryable": True,
    },
    5: {
        "raw_name": "ERR_OLD_VERSION",
        "plain_english": "MetaTrader client terminal version is outdated and unsupported by broker.",
        "next_action": "Update your MetaTrader 4 terminal to the latest build provided by your broker.",
        "severity": "CRITICAL",
        "is_retryable": False,
    },
    6: {
        "raw_name": "ERR_NO_CONNECTION",
        "plain_english": "Terminal has lost connection to the broker trading server.",
        "next_action": "Verify VPS internet connectivity and broker server status.",
        "severity": "CRITICAL",
        "is_retryable": True,
    },
    7: {
        "raw_name": "ERR_NOT_ENOUGH_RIGHTS",
        "plain_english": "Trading rights not granted (logged in with read-only Investor password).",
        "next_action": "Log into MetaTrader using your Master Trading Password instead of Investor password.",
        "severity": "CRITICAL",
        "is_retryable": False,
    },
    8: {
        "raw_name": "ERR_TOO_FREQUENT_REQUESTS",
        "plain_english": "Too many requests submitted to the broker within a short timeframe.",
        "next_action": "Increase request throttling delays in Engine Advanced Settings.",
        "severity": "WARNING",
        "is_retryable": True,
    },
    9: {
        "raw_name": "ERR_MALFUNCTIONAL_TRADE",
        "plain_english": "Broker execution subsystem malfunctioned during order processing.",
        "next_action": "Contact broker technical support if this issue persists.",
        "severity": "ERROR",
        "is_retryable": True,
    },
    64: {
        "raw_name": "ERR_ACCOUNT_DISABLED",
        "plain_english": "Broker trading account is disabled, archived, or suspended.",
        "next_action": "Contact your broker to reactivate your trading account.",
        "severity": "CRITICAL",
        "is_retryable": False,
    },
    65: {
        "raw_name": "ERR_INVALID_ACCOUNT",
        "plain_english": "Invalid account login number or account does not exist on this server.",
        "next_action": "Verify account login number and broker server name in Account Profile.",
        "severity": "CRITICAL",
        "is_retryable": False,
    },
    128: {
        "raw_name": "ERR_TRADE_TIMEOUT",
        "plain_english": "Broker trade request timed out waiting for server response.",
        "next_action": "Check terminal trade history to confirm if order filled before retrying.",
        "severity": "WARNING",
        "is_retryable": True,
    },
    129: {
        "raw_name": "ERR_INVALID_PRICE",
        "plain_english": "Invalid order entry price. Broker refused the off-market price quote.",
        "next_action": "Ensure live quote streaming is active and symbol digits match broker.",
        "severity": "ERROR",
        "is_retryable": True,
    },
    130: {
        # Explicitly required by Sprint 23 Directive
        "raw_name": "ERR_INVALID_STOPS",
        "plain_english": "Stop Loss is too close to the current price. Broker rejected the trade.",
        "next_action": "Check your Broker's Freeze Levels in Settings",
        "severity": "ERROR",
        "is_retryable": False,
    },
    131: {
        "raw_name": "ERR_INVALID_TRADE_VOLUME",
        "plain_english": "Order volume is invalid. Does not meet broker min, max, or lot step rules.",
        "next_action": "Verify symbol min lot, max lot, and lot step in Broker Symbol Specifications.",
        "severity": "ERROR",
        "is_retryable": False,
    },
    132: {
        "raw_name": "ERR_MARKET_CLOSED",
        "plain_english": "The market for this financial instrument is currently closed.",
        "next_action": "Wait for trading session open or verify holiday market hours with broker.",
        "severity": "WARNING",
        "is_retryable": False,
    },
    133: {
        "raw_name": "ERR_TRADE_DISABLED",
        "plain_english": "Trading is disabled for this symbol or account by broker administration.",
        "next_action": "Check if symbol is expired/read-only or if account trading is enabled by broker.",
        "severity": "ERROR",
        "is_retryable": False,
    },
    134: {
        "raw_name": "ERR_NOT_ENOUGH_MONEY",
        "plain_english": "Not enough free margin in account to open the requested position size.",
        "next_action": "Reduce lot size in Risk Settings or deposit additional margin into account.",
        "severity": "ERROR",
        "is_retryable": False,
    },
    135: {
        "raw_name": "ERR_PRICE_CHANGED",
        "plain_english": "Price changed while order was transmitting (slippage detected).",
        "next_action": "Increase slippage tolerance in Account Profile or enable market orders.",
        "severity": "WARNING",
        "is_retryable": True,
    },
    136: {
        "raw_name": "ERR_OFF_QUOTES",
        "plain_english": "Broker has no active price quotes streaming for this instrument.",
        "next_action": "Verify symbol is actively quoting in Market Watch window of terminal.",
        "severity": "ERROR",
        "is_retryable": True,
    },
    137: {
        "raw_name": "ERR_BROKER_BUSY",
        "plain_english": "Broker trade desk or execution bridge is busy processing other transactions.",
        "next_action": "Wait for automatic engine retry with exponential backoff.",
        "severity": "WARNING",
        "is_retryable": True,
    },
    138: {
        "raw_name": "ERR_REQUOTE",
        "plain_english": "Broker requoted the order because market moved during transmission.",
        "next_action": "Increase maximum allowed slippage pips in Risk Settings.",
        "severity": "WARNING",
        "is_retryable": True,
    },
    139: {
        "raw_name": "ERR_ORDER_LOCKED",
        "plain_english": "Order is currently locked and being processed by the broker trade desk.",
        "next_action": "Wait a few seconds for pending execution to complete before modifying.",
        "severity": "WARNING",
        "is_retryable": True,
    },
    140: {
        "raw_name": "ERR_LONG_POSITIONS_ONLY_ALLOWED",
        "plain_english": "Broker only permits BUY (long) positions for this instrument. Short selling blocked.",
        "next_action": "Verify broker instrument restrictions; short selling is prohibited for this asset.",
        "severity": "ERROR",
        "is_retryable": False,
    },
    141: {
        "raw_name": "ERR_TOO_MANY_REQUESTS",
        "plain_english": "Order execution throttled due to exceeding broker request rate limits.",
        "next_action": "Allow engine backoff retry loop to execute.",
        "severity": "WARNING",
        "is_retryable": True,
    },
    145: {
        "raw_name": "ERR_TRADE_MODIFY_DENIED",
        "plain_english": "Order modification denied because order is already in queue or price reached.",
        "next_action": "Check if position is already closed or if stops breach broker freeze level.",
        "severity": "WARNING",
        "is_retryable": False,
    },
    146: {
        "raw_name": "ERR_TRADE_CONTEXT_BUSY",
        "plain_english": "MetaTrader trade execution thread (context) is busy handling another order.",
        "next_action": "Engine will automatically retry once the terminal trade thread clears.",
        "severity": "WARNING",
        "is_retryable": True,
    },
    147: {
        "raw_name": "ERR_TRADE_EXPIRATION_DENIED",
        "plain_english": "Broker rejected order expiration date parameter.",
        "next_action": "Verify broker pending order expiration policies in symbol settings.",
        "severity": "ERROR",
        "is_retryable": False,
    },
    148: {
        "raw_name": "ERR_TRADE_TOO_MANY_ORDERS",
        "plain_english": "Total open and pending orders count exceeds broker account maximum limit.",
        "next_action": "Close old or pending orders to free up slots under broker order limits.",
        "severity": "ERROR",
        "is_retryable": False,
    },
    149: {
        "raw_name": "ERR_TRADE_HEDGE_PROHIBITED",
        "plain_english": "Hedging is prohibited by broker. Opposing position exists for this symbol.",
        "next_action": "Enable FIFO / Netting mode in Account Profile to prevent hedged entries.",
        "severity": "ERROR",
        "is_retryable": False,
    },
    150: {
        "raw_name": "ERR_TRADE_PROHIBITED_BY_FIFO",
        "plain_english": "Trade rejected under US NFA FIFO (Rule 2-43b) compliance restrictions.",
        "next_action": "Enable FIFO mode in Pipvane Account Profile to enforce oldest-order closes.",
        "severity": "ERROR",
        "is_retryable": False,
    },
}


# =============================================================================
# 2. MT5 RETURN CODE DICTIONARY (MQL5 Trade Server Return Codes)
# =============================================================================

MT5_RETCODE_DICT: Dict[int, Dict[str, Any]] = {
    10004: {
        "raw_name": "TRADE_RETCODE_REQUOTE",
        "plain_english": "Price changed during order routing; broker offered a requote.",
        "next_action": "Increase maximum allowed slippage in Account Profile settings.",
        "severity": "WARNING",
        "is_retryable": True,
    },
    10006: {
        "raw_name": "TRADE_RETCODE_REJECT",
        "plain_english": "Trade request was rejected by broker server.",
        "next_action": "Check terminal journal tab or contact broker to confirm rejection reason.",
        "severity": "ERROR",
        "is_retryable": False,
    },
    10007: {
        "raw_name": "TRADE_RETCODE_CANCEL",
        "plain_english": "Trade request was cancelled by trader, engine, or broker desk.",
        "next_action": "Verify order status before re-transmitting signal.",
        "severity": "INFO",
        "is_retryable": False,
    },
    10008: {
        "raw_name": "TRADE_RETCODE_PLACED",
        "plain_english": "Pending order successfully placed in broker order book.",
        "next_action": "No action required; pending order is waiting for entry trigger.",
        "severity": "INFO",
        "is_retryable": False,
    },
    10009: {
        "raw_name": "TRADE_RETCODE_DONE",
        "plain_english": "Trade request successfully executed and filled by broker.",
        "next_action": "No action required; order is filled.",
        "severity": "INFO",
        "is_retryable": False,
    },
    10010: {
        "raw_name": "TRADE_RETCODE_DONE_PARTIAL",
        "plain_english": "Order was partially filled due to available liquidity.",
        "next_action": "Remaining volume cancelled or pending depending on order fill policy.",
        "severity": "INFO",
        "is_retryable": False,
    },
    10011: {
        "raw_name": "TRADE_RETCODE_ERROR",
        "plain_english": "Generic trade request processing error encountered on broker server.",
        "next_action": "Retry order placement or check server logs if error repeats.",
        "severity": "ERROR",
        "is_retryable": True,
    },
    10012: {
        "raw_name": "TRADE_RETCODE_TIMEOUT",
        "plain_english": "Trade request cancelled by timeout waiting for server response.",
        "next_action": "Check open positions to verify if order executed before re-attempting.",
        "severity": "WARNING",
        "is_retryable": True,
    },
    10013: {
        "raw_name": "TRADE_RETCODE_INVALID",
        "plain_english": "Invalid trade request structure or unsupported order parameters.",
        "next_action": "Verify symbol mapping and order type parameters in Engine Settings.",
        "severity": "ERROR",
        "is_retryable": False,
    },
    10014: {
        "raw_name": "TRADE_RETCODE_INVALID_VOLUME",
        "plain_english": "Invalid volume requested. Order volume breaches broker lot constraints.",
        "next_action": "Check minimum lot size and lot step in Broker Symbol Specifications.",
        "severity": "ERROR",
        "is_retryable": False,
    },
    10015: {
        # Explicitly required by Sprint 23 Directive
        "raw_name": "TRADE_RETCODE_INVALID_PRICE",
        "plain_english": "Invalid price. The requested entry price is no longer available.",
        "next_action": "Increase slippage tolerance or enable Market Execution mode in Risk Settings.",
        "severity": "ERROR",
        "is_retryable": True,
    },
    10016: {
        "raw_name": "TRADE_RETCODE_INVALID_STOPS",
        "plain_english": "Invalid Stop Loss or Take Profit price. Distance violates broker Stops Level.",
        "next_action": "Check your Broker's Stops Level in Settings or set StopsLevelPolicy to ADJUST.",
        "severity": "ERROR",
        "is_retryable": False,
    },
    10017: {
        "raw_name": "TRADE_RETCODE_TRADE_DISABLED",
        "plain_english": "Trade execution is currently disabled for this symbol on broker server.",
        "next_action": "Check broker symbol trading permissions or market status.",
        "severity": "ERROR",
        "is_retryable": False,
    },
    10018: {
        "raw_name": "TRADE_RETCODE_MARKET_CLOSED",
        "plain_english": "Market is currently closed for this instrument.",
        "next_action": "Wait for session open or check broker holiday schedule.",
        "severity": "WARNING",
        "is_retryable": False,
    },
    10019: {
        "raw_name": "TRADE_RETCODE_NO_MONEY",
        "plain_english": "Insufficient funds in account to cover required margin.",
        "next_action": "Reduce position lot size in Risk Settings or deposit funds.",
        "severity": "ERROR",
        "is_retryable": False,
    },
    10020: {
        "raw_name": "TRADE_RETCODE_PRICE_CHANGED",
        "plain_english": "Market price changed during execution request.",
        "next_action": "Allow automated retry or increase slippage tolerance in settings.",
        "severity": "WARNING",
        "is_retryable": True,
    },
    10021: {
        "raw_name": "TRADE_RETCODE_PRICE_OFF",
        "plain_english": "No quotes available from broker liquidity providers for this symbol.",
        "next_action": "Ensure Market Watch has enabled quotes for this symbol.",
        "severity": "ERROR",
        "is_retryable": True,
    },
    10022: {
        "raw_name": "TRADE_RETCODE_INVALID_EXPIRATION",
        "plain_english": "Invalid pending order expiration timestamp.",
        "next_action": "Check broker pending order expiration rules.",
        "severity": "ERROR",
        "is_retryable": False,
    },
    10023: {
        "raw_name": "TRADE_RETCODE_ORDER_CHANGED",
        "plain_english": "Order state changed while request was in-flight (already closed or modified).",
        "next_action": "Re-sync terminal positions via Reconciliation Loop before retrying.",
        "severity": "WARNING",
        "is_retryable": False,
    },
    10024: {
        "raw_name": "TRADE_RETCODE_TOO_MANY_REQUESTS",
        "plain_english": "Frequent trade requests throttled by broker server.",
        "next_action": "Allow engine backoff retry loop to execute.",
        "severity": "WARNING",
        "is_retryable": True,
    },
    10025: {
        "raw_name": "TRADE_RETCODE_NO_CHANGES",
        "plain_english": "Order modification request contained identical values (no changes made).",
        "next_action": "No action needed; order already carries requested SL/TP.",
        "severity": "INFO",
        "is_retryable": False,
    },
    10026: {
        "raw_name": "TRADE_RETCODE_SERVER_DISABLES_AT",
        "plain_english": "Automated Expert Advisor trading disabled by broker server.",
        "next_action": "Contact broker to enable automated algorithmic trading for your account.",
        "severity": "CRITICAL",
        "is_retryable": False,
    },
    10027: {
        "raw_name": "TRADE_RETCODE_CLIENT_DISABLES_AT",
        "plain_english": "Automated Trading (Algo Trading) disabled in MetaTrader client terminal.",
        "next_action": "Click the 'Algo Trading' / 'AutoTrading' button in MetaTrader top toolbar.",
        "severity": "CRITICAL",
        "is_retryable": False,
    },
    10028: {
        "raw_name": "TRADE_RETCODE_LOCKED",
        "plain_english": "Order request locked on broker side for processing.",
        "next_action": "Wait for order processing to complete before sending modifications.",
        "severity": "WARNING",
        "is_retryable": True,
    },
    10029: {
        "raw_name": "TRADE_RETCODE_FROZEN",
        "plain_english": "Order or position is frozen because price is too close to market execution.",
        "next_action": "Wait for market price to move away from active SL/TP or broker freeze zone.",
        "severity": "WARNING",
        "is_retryable": False,
    },
    10030: {
        "raw_name": "TRADE_RETCODE_INVALID_FILL",
        "plain_english": "Unsupported order execution fill type (e.g. FOK vs IOC vs Return).",
        "next_action": "Update Fill Policy in Account Profile (try IOC or Return).",
        "severity": "ERROR",
        "is_retryable": False,
    },
    10031: {
        "raw_name": "TRADE_RETCODE_CONNECTION",
        "plain_english": "Lost connection to the trade server during execution.",
        "next_action": "Check VPS network connection and broker server status.",
        "severity": "CRITICAL",
        "is_retryable": True,
    },
    10032: {
        "raw_name": "TRADE_RETCODE_ONLY_REAL",
        "plain_english": "Operation allowed only on live accounts.",
        "next_action": "Verify account tier and credentials in Account Profile.",
        "severity": "ERROR",
        "is_retryable": False,
    },
    10033: {
        "raw_name": "TRADE_RETCODE_LIMIT_ORDERS",
        "plain_english": "Maximum number of pending orders reached on broker account.",
        "next_action": "Delete unneeded pending orders to free capacity.",
        "severity": "ERROR",
        "is_retryable": False,
    },
    10034: {
        "raw_name": "TRADE_RETCODE_LIMIT_VOLUME",
        "plain_english": "Accumulated position volume on symbol exceeds broker maximum ceiling.",
        "next_action": "Close or reduce existing positions for this symbol before opening more.",
        "severity": "ERROR",
        "is_retryable": False,
    },
    10035: {
        "raw_name": "TRADE_RETCODE_POSITION_CLOSED",
        "plain_english": "The specified position was already closed.",
        "next_action": "Reconciliation loop will clear the closed position from tracking.",
        "severity": "INFO",
        "is_retryable": False,
    },
    10036: {
        "raw_name": "TRADE_RETCODE_INVALID_CLOSE_VOLUME",
        "plain_english": "Close volume exceeds current open position volume.",
        "next_action": "Check current position volume in MetaTrader terminal.",
        "severity": "ERROR",
        "is_retryable": False,
    },
    10038: {
        "raw_name": "TRADE_RETCODE_CLOSE_ORDER_EXIST",
        "plain_english": "A close order already exists for this position.",
        "next_action": "Wait for pending close order to execute.",
        "severity": "WARNING",
        "is_retryable": False,
    },
    10039: {
        "raw_name": "TRADE_RETCODE_LIMIT_POSITIONS",
        "plain_english": "Total open positions limit reached on broker account.",
        "next_action": "Close open positions to allow new trades.",
        "severity": "ERROR",
        "is_retryable": False,
    },
    10040: {
        "raw_name": "TRADE_RETCODE_REJECT_CANCEL",
        "plain_english": "Broker rejected pending order cancellation request.",
        "next_action": "Order may be currently executing; refresh terminal positions.",
        "severity": "WARNING",
        "is_retryable": False,
    },
    10041: {
        "raw_name": "TRADE_RETCODE_LONG_ONLY",
        "plain_english": "Only BUY orders are allowed on this symbol by broker.",
        "next_action": "Short selling is disabled for this instrument.",
        "severity": "ERROR",
        "is_retryable": False,
    },
    10042: {
        "raw_name": "TRADE_RETCODE_SHORT_ONLY",
        "plain_english": "Only SELL orders are allowed on this symbol by broker.",
        "next_action": "Long buying is disabled for this instrument.",
        "severity": "ERROR",
        "is_retryable": False,
    },
    10043: {
        "raw_name": "TRADE_RETCODE_CLOSE_ONLY",
        "plain_english": "Symbol is in close-only mode. New positions cannot be opened.",
        "next_action": "Instrument is expiring or restricted to closing existing trades only.",
        "severity": "ERROR",
        "is_retryable": False,
    },
    10044: {
        "raw_name": "TRADE_RETCODE_FIFO_CLOSE",
        "plain_english": "Position close rejected because older position must be closed first under FIFO.",
        "next_action": "Enable FIFO mode in Pipvane settings to automatically route closes to oldest ticket.",
        "severity": "ERROR",
        "is_retryable": False,
    },
    10045: {
        "raw_name": "TRADE_RETCODE_HEDGE_PROHIBITED",
        "plain_english": "Hedging is prohibited on this account (opposing position open).",
        "next_action": "Enable FIFO mode or close opposing trade before opening new direction.",
        "severity": "ERROR",
        "is_retryable": False,
    },
}


# =============================================================================
# 3. CTRADER / FIX ERROR DICTIONARY (cTrader Open API Return Codes)
# =============================================================================

CTRADER_ERROR_DICT: Dict[str, Dict[str, Any]] = {
    "MARKET_CLOSED": {
        "raw_name": "OA_MARKET_CLOSED",
        "plain_english": "The market for this financial instrument is currently closed.",
        "next_action": "Wait for market open hours or check broker trading session calendar.",
        "severity": "WARNING",
        "is_retryable": False,
    },
    "NOT_ENOUGH_MONEY": {
        "raw_name": "OA_NOT_ENOUGH_MONEY",
        "plain_english": "Insufficient margin available on cTrader account.",
        "next_action": "Reduce lot size in Risk Settings or deposit additional margin.",
        "severity": "ERROR",
        "is_retryable": False,
    },
    "INSUFFICIENT_MARGIN": {
        "raw_name": "OA_INSUFFICIENT_MARGIN",
        "plain_english": "Free margin is insufficient to support requested position leverage.",
        "next_action": "Reduce position volume or deposit funds to increase margin.",
        "severity": "ERROR",
        "is_retryable": False,
    },
    "POSITION_NOT_FOUND": {
        "raw_name": "OA_POSITION_NOT_FOUND",
        "plain_english": "The target position ticket does not exist on cTrader server (already closed).",
        "next_action": "Run Reconciliation Loop to synchronize active positions.",
        "severity": "INFO",
        "is_retryable": False,
    },
    "ORDER_NOT_FOUND": {
        "raw_name": "OA_ORDER_NOT_FOUND",
        "plain_english": "The target pending order was not found on cTrader server.",
        "next_action": "Re-audit pending orders via terminal reconciliation.",
        "severity": "INFO",
        "is_retryable": False,
    },
    "INVALID_REQUEST": {
        "raw_name": "OA_INVALID_REQUEST",
        "plain_english": "cTrader rejected the request due to invalid parameters or formatting.",
        "next_action": "Check symbol mapping and volume step configuration.",
        "severity": "ERROR",
        "is_retryable": False,
    },
    "TRADING_DISABLED": {
        "raw_name": "OA_TRADING_DISABLED",
        "plain_english": "Trading is currently disabled for this symbol or account on cTrader.",
        "next_action": "Check cTrader account permissions and market session status.",
        "severity": "ERROR",
        "is_retryable": False,
    },
    "INVALID_VOLUME": {
        "raw_name": "OA_INVALID_VOLUME",
        "plain_english": "Order volume violates cTrader minimum lot or volume step constraints.",
        "next_action": "Check symbol volume specifications and configure lot sizing rules.",
        "severity": "ERROR",
        "is_retryable": False,
    },
    "INVALID_PRICE": {
        "raw_name": "OA_INVALID_PRICE",
        "plain_english": "Invalid price quote for cTrader limit/stop order.",
        "next_action": "Ensure price quotes are actively streaming and within broker tolerances.",
        "severity": "ERROR",
        "is_retryable": True,
    },
    "INVALID_STOPS": {
        "raw_name": "OA_INVALID_STOPS",
        "plain_english": "Stop Loss or Take Profit distance is invalid for cTrader execution rules.",
        "next_action": "Check your Broker's Freeze Levels in Settings",
        "severity": "ERROR",
        "is_retryable": False,
    },
    "CONNECTION_LOST": {
        "raw_name": "OA_CONNECTION_LOST",
        "plain_english": "cTrader Open API connection dropped during request transmission.",
        "next_action": "Check internet connectivity; engine will auto-reconnect.",
        "severity": "CRITICAL",
        "is_retryable": True,
    },
    "TIMEOUT": {
        "raw_name": "OA_TIMEOUT",
        "plain_english": "cTrader server did not respond within the network timeout window.",
        "next_action": "Check open positions before retrying to verify order state.",
        "severity": "WARNING",
        "is_retryable": True,
    },
    "RATE_LIMIT_EXCEEDED": {
        "raw_name": "OA_RATE_LIMIT_EXCEEDED",
        "plain_english": "Too many requests submitted to cTrader API per second.",
        "next_action": "Allow engine exponential backoff retry loop to execute.",
        "severity": "WARNING",
        "is_retryable": True,
    },
    "TECHNICAL_ERROR": {
        "raw_name": "OA_TECHNICAL_ERROR",
        "plain_english": "Internal technical error on cTrader broker server.",
        "next_action": "Retry order placement or contact broker if issue persists.",
        "severity": "ERROR",
        "is_retryable": True,
    },
}


# =============================================================================
# ERROR TRANSLATOR UTILITY CLASS
# =============================================================================

class ErrorTranslator:
    """
    Central Broker Error Translation Engine.
    Converts MT4, MT5, and cTrader error codes into plain language and actionable advice.
    """

    @classmethod
    def detect_platform(cls, code: Union[int, str], hint_platform: Optional[str] = None) -> str:
        """Heuristically detects platform type (MT4, MT5, cTrader) from error code structure."""
        if hint_platform:
            h = hint_platform.upper().strip()
            if "MT4" in h:
                return "MT4"
            if "MT5" in h:
                return "MT5"
            if "CTRADER" in h or "CTRADE" in h:
                return "cTrader"

        # Check integer range
        if isinstance(code, int) or (isinstance(code, str) and code.isdigit()):
            int_code = int(code)
            if 10000 <= int_code < 20000:
                return "MT5"
            if 0 <= int_code < 1000:
                return "MT4"

        # Check string match in cTrader dictionary
        str_code = str(code).upper().strip()
        if str_code in CTRADER_ERROR_DICT or str_code.startswith("OA_"):
            return "cTrader"

        return "Generic"

    @classmethod
    def translate(
        cls,
        code: Union[int, str],
        platform: Optional[str] = None,
    ) -> TranslatedError:
        """
        Translates a raw broker error code into a structured TranslatedError object.
        Guarantees strict compliance with sprint requirements (e.g. MT4 130 and MT5 10015).
        """
        detected_plat = cls.detect_platform(code, platform)

        # Normalize code
        int_code: Optional[int] = None
        str_code = str(code).upper().strip()
        if isinstance(code, int) or (isinstance(code, str) and code.isdigit()):
            int_code = int(code)

        # ---------------------------------------------------------------------
        # MT4 Lookup
        # ---------------------------------------------------------------------
        if detected_plat == "MT4" or (int_code is not None and int_code in MT4_ERROR_DICT):
            if int_code is not None and int_code in MT4_ERROR_DICT:
                spec = MT4_ERROR_DICT[int_code]
                return TranslatedError(
                    code=int_code,
                    platform="MT4",
                    raw_name=spec["raw_name"],
                    plain_english=spec["plain_english"],
                    next_action=spec["next_action"],
                    severity=spec.get("severity", "ERROR"),
                    is_retryable=spec.get("is_retryable", False),
                )

        # ---------------------------------------------------------------------
        # MT5 Lookup
        # ---------------------------------------------------------------------
        if detected_plat == "MT5" or (int_code is not None and int_code in MT5_RETCODE_DICT):
            if int_code is not None and int_code in MT5_RETCODE_DICT:
                spec = MT5_RETCODE_DICT[int_code]
                return TranslatedError(
                    code=int_code,
                    platform="MT5",
                    raw_name=spec["raw_name"],
                    plain_english=spec["plain_english"],
                    next_action=spec["next_action"],
                    severity=spec.get("severity", "ERROR"),
                    is_retryable=spec.get("is_retryable", False),
                )

        # ---------------------------------------------------------------------
        # cTrader Lookup
        # ---------------------------------------------------------------------
        clean_ct_code = str_code.replace("OA_", "")
        if clean_ct_code in CTRADER_ERROR_DICT:
            spec = CTRADER_ERROR_DICT[clean_ct_code]
            return TranslatedError(
                code=code,
                platform="cTrader",
                raw_name=spec["raw_name"],
                plain_english=spec["plain_english"],
                next_action=spec["next_action"],
                severity=spec.get("severity", "ERROR"),
                is_retryable=spec.get("is_retryable", False),
            )

        # ---------------------------------------------------------------------
        # Fallback / Generic
        # ---------------------------------------------------------------------
        return TranslatedError(
            code=code,
            platform=detected_plat,
            raw_name=f"ERROR_{code}",
            plain_english=f"Broker rejected trade with code {code}.",
            next_action="Review terminal journal logs or check Risk Settings.",
            severity="ERROR",
            is_retryable=False,
        )

    @classmethod
    def format_alert_text(
        cls,
        code: Union[int, str],
        symbol: Optional[str] = None,
        platform: Optional[str] = None,
        terminal_id: Optional[str] = None,
    ) -> str:
        """
        Renders a clean emoji-supported plain-text alert for notifications and logs.
        """
        trans = cls.translate(code, platform=platform)
        sym_str = f" | Symbol: {symbol}" if symbol else ""
        term_str = f" | Terminal: {terminal_id}" if terminal_id else ""

        emoji = "❌" if trans.severity == "ERROR" else ("⚠️" if trans.severity == "WARNING" else "🚨")

        return (
            f"{emoji} BROKER REJECTION ({trans.platform} #{trans.code}: {trans.raw_name}){sym_str}{term_str}\n"
            f"Reason: {trans.plain_english}\n"
            f"Next Action: {trans.next_action}"
        )


def translate_error(code: Union[int, str], platform: Optional[str] = None) -> TranslatedError:
    """Convenience helper function for translating broker return codes."""
    return ErrorTranslator.translate(code, platform=platform)
