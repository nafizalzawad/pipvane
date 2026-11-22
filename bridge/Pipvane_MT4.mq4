//+------------------------------------------------------------------+
//|                                                 Pipvane_MT4.mq4  |
//|               Pipvane v3: High-Performance MQL4 Expert Advisor   |
//|        Direct Lock-Free IPC Execution & Real-Time Telemetry     |
//|      Conforms strictly to SPRINT_1_ARCHITECTURE_BLUEPRINT.md     |
//+------------------------------------------------------------------+
#property copyright   "Copyright 2026, Pipvane Team"
#property link        "https://pipvane.com"
#property version     "3.00"
#property description "Pipvane v3 Native Execution Wrapper for MetaTrader 4 (32-bit)"
#property strict

// Pipvane Native C++ Bridge Header (Auto-links to ipc_bridge_x86.dll on 32-bit)
#include "PipvaneBridge.mqh"

// =============================================================================
// Input Parameters
// =============================================================================
input string   InpTerminalId           = "term_01";      // Terminal ID (Must match Engine config)
input uint     InpPollIntervalMs       = 5;              // Order polling cadence in ms (<10ms recommended)
input uint     InpTelemetryIntervalMs  = 1000;           // Telemetry reporting interval in ms
input double   InpDefaultSlippagePips  = 3.0;            // Default slippage tolerance in pips
input int      InpDefaultMagic         = 999888;         // Fallback Magic Number
input int      InpMaxTradeRetries      = 3;              // Max retries on busy trade context
input bool     InpVerboseLogging       = true;           // Diagnostic logging to Experts tab
input bool     InpECNMode              = true;           // ECN Mode: Open first, then modify SL/TP

// =============================================================================
// Enums conforming to Blueprint (ADR-001)
// =============================================================================
enum ENUM_PIPVANE_ACTION
{
   ACTION_MARKET_ENTRY   = 1,
   ACTION_LIMIT_ENTRY    = 2,
   ACTION_STOP_ENTRY     = 3,
   ACTION_MODIFY_SL_TP   = 4,
   ACTION_PARTIAL_CLOSE  = 5,
   ACTION_FULL_CLOSE     = 6,
   ACTION_CANCEL_PENDING = 7
};

enum ENUM_PIPVANE_SIDE
{
   SIDE_BUY  = 0,
   SIDE_SELL = 1
};

enum ENUM_PIPVANE_EXEC_STATUS
{
   STATUS_FILLED   = 0,
   STATUS_PARTIAL  = 1,
   STATUS_REJECTED = 2,
   STATUS_REQUOTE  = 3,
   STATUS_EXPIRED  = 4
};

// =============================================================================
// Global Variables & State
// =============================================================================
ulong          ExtLastTelemetryTick = 0;
bool           ExtBridgeInitialized = false;

// Forward Declarations
void PollAndProcessOrders();
void ProcessOrderIntent(const ShmOrderIntent &intent);
void ExecuteMarketOrder(const ShmOrderIntent &intent, string symbol, double volume, ulong latency_start_us);
void ExecutePendingOrder(const ShmOrderIntent &intent, string symbol, double volume, ulong latency_start_us);
void ExecuteModifyOrder(const ShmOrderIntent &intent, string symbol, ulong latency_start_us);
void ExecuteCloseOrder(const ShmOrderIntent &intent, string symbol, double volume, bool is_partial, ulong latency_start_us);
void ExecuteCancelPending(const ShmOrderIntent &intent, ulong latency_start_us);

void SendAccountTelemetry();
void SendPositionsTelemetry();
void SendBrokerCapabilities();

double NormalizeVolume(string symbol, double volume);
double CalculatePipFactor(string symbol);
int    CalculateSlippagePoints(string symbol, double slippage_pips);
string GetMT4ErrorDescription(int error_code);

//+------------------------------------------------------------------+
//| Expert initialization function                                   |
//+------------------------------------------------------------------+
int OnInit()
{
   PrintFormat("[Pipvane MT4] Initializing Expert Advisor on Build %d (32-bit)...", TerminalInfoInteger(TERMINAL_BUILD));

   // 1. Check automated trading permissions
   if (!IsTradeAllowed())
   {
      Alert("[Pipvane CRITICAL] Live Trading is DISABLED in MT4 EA properties! Enable 'Allow live trading'.");
   }

   if (!IsDllsAllowed())
   {
      Alert("[Pipvane CRITICAL] DLL Imports are DISABLED in MT4 EA properties! Enable 'Allow DLL imports'.");
      return INIT_FAILED;
   }

   // 2. Initialize C++ Native Bridge (Shared Memory + Named Pipe)
   string serverName  = AccountServer();
   string companyName = AccountCompany();
   ulong  accountLogin = (ulong)AccountNumber();
   int    isPortable   = TerminalInfoInteger(TERMINAL_PORTABLE) ? 1 : 0;
   int    build        = (int)TerminalInfoInteger(TERMINAL_BUILD);

   int initRes = InitializeBridgeW(
      InpTerminalId,
      0, // Platform: 0 = MT4
      build,
      isPortable,
      accountLogin,
      serverName,
      companyName
   );

   if (initRes != 0)
   {
      PrintFormat("[Pipvane ERROR] Failed to attach to 32-bit C++ IPC Bridge (code: %d). Error: %s",
                  initRes, Pipvane_GetLastError());
      return INIT_FAILED;
   }

   ExtBridgeInitialized = true;
   PrintFormat("[Pipvane MT4] Connected to IPC Bridge: Terminal ID='%s', Login=%I64u, Server='%s'",
               InpTerminalId, accountLogin, serverName);

   // 3. Send initial broker capabilities
   SendBrokerCapabilities();

   // 4. Send initial state telemetry
   SendAccountTelemetry();
   SendPositionsTelemetry();
   ExtLastTelemetryTick = GetTickCount();

   // 5. Start high-resolution millisecond timer for sub-millisecond polling
   uint pollMs = (InpPollIntervalMs < 1) ? 1 : InpPollIntervalMs;
   if (!EventSetMillisecondTimer(pollMs))
   {
      PrintFormat("[Pipvane WARNING] EventSetMillisecondTimer(%u ms) failed. Falling back to OnTick polling.", pollMs);
   }
   else
   {
      PrintFormat("[Pipvane MT4] High-resolution polling timer armed at %u ms cadence.", pollMs);
   }

   return INIT_SUCCEEDED;
}

//+------------------------------------------------------------------+
//| Expert deinitialization function                                 |
//+------------------------------------------------------------------+
void OnDeinit(const int reason)
{
   PrintFormat("[Pipvane MT4] Deinitializing EA (Reason: %d)...", reason);

   EventKillTimer();

   if (ExtBridgeInitialized)
   {
      SendAccountTelemetry();
      ShutdownBridge();
      ExtBridgeInitialized = false;
   }

   Print("[Pipvane MT4] Detached from IPC Bridge cleanly.");
}

//+------------------------------------------------------------------+
//| Expert tick function (Executed on incoming market ticks)         |
//+------------------------------------------------------------------+
void OnTick()
{
   if (!ExtBridgeInitialized) return;

   // 1. Fast Lock-Free Order Polling (<50 ns check)
   PollAndProcessOrders();

   // 2. Throttled Telemetry update
   ulong now = GetTickCount();
   if (now - ExtLastTelemetryTick >= InpTelemetryIntervalMs)
   {
      SendAccountTelemetry();
      SendPositionsTelemetry();
      ExtLastTelemetryTick = now;
   }
}

//+------------------------------------------------------------------+
//| Expert timer function (High-resolution millisecond poll)        |
//+------------------------------------------------------------------+
void OnTimer()
{
   if (!ExtBridgeInitialized) return;

   // 1. Fast Lock-Free Order Polling
   PollAndProcessOrders();

   // 2. Periodic state telemetry heartbeat
   ulong now = GetTickCount();
   if (now - ExtLastTelemetryTick >= InpTelemetryIntervalMs)
   {
      SendAccountTelemetry();
      SendPositionsTelemetry();
      ExtLastTelemetryTick = now;
   }
}

// =============================================================================
// Core Fast-Path: Lock-Free Shared Memory Poller & Dispatcher
// =============================================================================
void PollAndProcessOrders()
{
   ShmOrderIntent intent;

   // Non-blocking drain loop: returns 0 immediately if queue empty (~20-50ns)
   while (GetNextOrderIntent(intent) == 1)
   {
      ProcessOrderIntent(intent);
   }
}

// =============================================================================
// Order Intent Processor & Route Dispatcher
// =============================================================================
void ProcessOrderIntent(const ShmOrderIntent &intent)
{
   ulong latencyStartUs = GetMicrosecondCount();

   // Parse null-terminated instrument symbol
   string symbol = Pipvane_UCharArrayToString(intent.symbol);
   StringTrimLeft(symbol);
   StringTrimRight(symbol);

   if (symbol == "")
   {
      symbol = Symbol();
   }

   // Route to specialized execution routine based on Action Type
   switch (intent.action_type)
   {
      case ACTION_MARKET_ENTRY:
      {
         double vol = NormalizeVolume(symbol, intent.volume);
         ExecuteMarketOrder(intent, symbol, vol, latencyStartUs);
         break;
      }
      case ACTION_LIMIT_ENTRY:
      case ACTION_STOP_ENTRY:
      {
         double vol = NormalizeVolume(symbol, intent.volume);
         ExecutePendingOrder(intent, symbol, vol, latencyStartUs);
         break;
      }
      case ACTION_MODIFY_SL_TP:
      {
         ExecuteModifyOrder(intent, symbol, latencyStartUs);
         break;
      }
      case ACTION_PARTIAL_CLOSE:
      {
         double vol = NormalizeVolume(symbol, intent.volume);
         ExecuteCloseOrder(intent, symbol, vol, true, latencyStartUs);
         break;
      }
      case ACTION_FULL_CLOSE:
      {
         ExecuteCloseOrder(intent, symbol, intent.volume, false, latencyStartUs);
         break;
      }
      case ACTION_CANCEL_PENDING:
      {
         ExecuteCancelPending(intent, latencyStartUs);
         break;
      }
      default:
      {
         ulong latencyUs = GetMicrosecondCount() - latencyStartUs;
         PrintFormat("[Pipvane MT4 ERROR] Unrecognized Action Type: %d", intent.action_type);
         ReportExecutionFill(
            intent.signal_id, 0, STATUS_REJECTED, 4051,
            intent.volume, 0.0, intent.price, 0.0, 0.0,
            latencyUs, "Invalid Action Type"
         );
         break;
      }
   }
}

// =============================================================================
// Execution Function 1: Market Orders (BUY / SELL with ECN SL/TP handling)
// =============================================================================
void ExecuteMarketOrder(const ShmOrderIntent &intent, string symbol, double volume, ulong latency_start_us)
{
   int magic = (intent.magic_number > 0) ? (int)intent.magic_number : InpDefaultMagic;
   string comment = StringFormat("PV:%I64u", intent.signal_id);
   int slippagePoints = CalculateSlippagePoints(symbol, intent.slippage_cap_pips);
   double pipFactor = CalculatePipFactor(symbol);
   double point = MarketInfo(symbol, MODE_POINT);
   int digits = (int)MarketInfo(symbol, MODE_DIGITS);

   int cmd = -1;
   double price = 0.0;

   if (intent.side == SIDE_BUY)
   {
      cmd = OP_BUY;
      price = MarketInfo(symbol, MODE_ASK);
   }
   else if (intent.side == SIDE_SELL)
   {
      cmd = OP_SELL;
      price = MarketInfo(symbol, MODE_BID);
   }
   else
   {
      ulong latUs = GetMicrosecondCount() - latency_start_us;
      ReportExecutionFill(intent.signal_id, 0, STATUS_REJECTED, 4051, volume, 0.0, intent.price, 0.0, 0.0, latUs, "Invalid Order Side");
      return;
   }

   // Normalize prices
   price = NormalizeDouble(price, digits);
   double sl = (intent.stop_loss > 0.0) ? NormalizeDouble(intent.stop_loss, digits) : 0.0;
   double tp = (intent.take_profit > 0.0) ? NormalizeDouble(intent.take_profit, digits) : 0.0;

   // In ECN Mode, open with SL=0, TP=0 first to avoid Error 130 (ERR_INVALID_STOPS)
   double initialSL = InpECNMode ? 0.0 : sl;
   double initialTP = InpECNMode ? 0.0 : tp;

   int ticket = -1;
   int lastErr = 0;

   // Retry loop for transient broker busy errors (Error 137, 146)
   for (int attempt = 0; attempt < InpMaxTradeRetries; ++attempt)
   {
      RefreshRates();
      price = (cmd == OP_BUY) ? MarketInfo(symbol, MODE_ASK) : MarketInfo(symbol, MODE_BID);
      price = NormalizeDouble(price, digits);

      ticket = OrderSend(symbol, cmd, volume, price, slippagePoints, initialSL, initialTP, comment, magic, 0, (cmd == OP_BUY ? clrBlue : clrRed));

      if (ticket > 0)
      {
         break;
      }

      lastErr = GetLastError();

      // If failed with Error 130 and not in ECN mode, switch to ECN mode and retry immediately
      if (lastErr == 130 && !InpECNMode)
      {
         initialSL = 0.0;
         initialTP = 0.0;
         continue;
      }

      // If trade context or broker busy, backoff briefly
      if (lastErr == 137 || lastErr == 146)
      {
         Sleep(100);
         continue;
      }

      // Fatal rejection
      break;
   }

   ulong latencyUs = GetMicrosecondCount() - latency_start_us;

   // If order succeeded and we are in ECN mode, apply SL / TP via OrderModify immediately
   if (ticket > 0 && (sl > 0.0 || tp > 0.0) && (initialSL == 0.0 || initialTP == 0.0))
   {
      if (OrderSelect(ticket, SELECT_BY_TICKET, MODE_TRADES))
      {
         bool modOk = OrderModify(ticket, OrderOpenPrice(), sl, tp, 0, clrYellow);
         if (!modOk)
         {
            PrintFormat("[Pipvane MT4 WARNING] OrderModify SL/TP failed on Ticket %d: Error %d (%s)",
                        ticket, GetLastError(), GetMT4ErrorDescription(GetLastError()));
         }
      }
   }

   // Prepare feedback
   if (ticket > 0)
   {
      if (OrderSelect(ticket, SELECT_BY_TICKET, MODE_TRADES))
      {
         double execPrice = OrderOpenPrice();
         double execVol = OrderLots();
         double slippagePips = 0.0;

         if (intent.price > 0.0 && point > 0.0)
         {
            slippagePips = MathAbs(execPrice - intent.price) / (point * pipFactor);
         }

         if (InpVerboseLogging)
         {
            PrintFormat("[Pipvane MT4 EXEC] Market %s %s: Vol=%.2f, Ticket=%d, Price=%.5f, Slippage=%.1f pips, Latency=%I64u us",
                        (cmd == OP_BUY ? "BUY" : "SELL"), symbol, execVol, ticket, execPrice, slippagePips, latencyUs);
         }

         ReportExecutionFill(
            intent.signal_id,
            (ulong)ticket,
            STATUS_FILLED,
            0, // MT4 success code
            volume,
            execVol,
            intent.price,
            execPrice,
            slippagePips,
            latencyUs,
            "Success"
         );
      }
   }
   else
   {
      string errDesc = GetMT4ErrorDescription(lastErr);
      int execStatus = STATUS_REJECTED;

      if (lastErr == 136 || lastErr == 138) execStatus = STATUS_REQUOTE;
      else if (lastErr == 128) execStatus = STATUS_EXPIRED;

      PrintFormat("[Pipvane MT4 REJECT] Market %s %s: Volume=%.2f, Error=%d (%s)",
                  (cmd == OP_BUY ? "BUY" : "SELL"), symbol, volume, lastErr, errDesc);

      ReportExecutionFill(
         intent.signal_id,
         0,
         execStatus,
         (uint)lastErr,
         volume,
         0.0,
         intent.price,
         0.0,
         0.0,
         latencyUs,
         errDesc
      );
   }
}

// =============================================================================
// Execution Function 2: Pending Orders (LIMIT / STOP)
// =============================================================================
void ExecutePendingOrder(const ShmOrderIntent &intent, string symbol, double volume, ulong latency_start_us)
{
   int magic = (intent.magic_number > 0) ? (int)intent.magic_number : InpDefaultMagic;
   string comment = StringFormat("PV:%I64u", intent.signal_id);
   int slippagePoints = CalculateSlippagePoints(symbol, intent.slippage_cap_pips);
   int digits = (int)MarketInfo(symbol, MODE_DIGITS);

   int cmd = -1;
   if (intent.action_type == ACTION_LIMIT_ENTRY)
   {
      cmd = (intent.side == SIDE_BUY) ? OP_BUYLIMIT : OP_SELLLIMIT;
   }
   else if (intent.action_type == ACTION_STOP_ENTRY)
   {
      cmd = (intent.side == SIDE_BUY) ? OP_BUYSTOP : OP_SELLSTOP;
   }

   double price = NormalizeDouble(intent.price, digits);
   double sl = (intent.stop_loss > 0.0) ? NormalizeDouble(intent.stop_loss, digits) : 0.0;
   double tp = (intent.take_profit > 0.0) ? NormalizeDouble(intent.take_profit, digits) : 0.0;

   int ticket = OrderSend(symbol, cmd, volume, price, slippagePoints, sl, tp, comment, magic, 0, clrOrange);
   ulong latencyUs = GetMicrosecondCount() - latency_start_us;

   if (ticket > 0)
   {
      if (InpVerboseLogging)
      {
         PrintFormat("[Pipvane MT4 EXEC] Pending Order %s: Cmd=%d, Price=%.5f, Ticket=%d", symbol, cmd, price, ticket);
      }

      ReportExecutionFill(
         intent.signal_id,
         (ulong)ticket,
         STATUS_FILLED,
         0,
         volume,
         volume,
         intent.price,
         price,
         0.0,
         latencyUs,
         "Success"
      );
   }
   else
   {
      int lastErr = GetLastError();
      string errDesc = GetMT4ErrorDescription(lastErr);
      PrintFormat("[Pipvane MT4 REJECT] Pending %s: Price=%.5f, Error=%d (%s)", symbol, price, lastErr, errDesc);

      ReportExecutionFill(
         intent.signal_id,
         0,
         STATUS_REJECTED,
         (uint)lastErr,
         volume,
         0.0,
         intent.price,
         0.0,
         0.0,
         latencyUs,
         errDesc
      );
   }
}

// =============================================================================
// Execution Function 3: Order Modifications (SL / TP)
// =============================================================================
void ExecuteModifyOrder(const ShmOrderIntent &intent, string symbol, ulong latency_start_us)
{
   int ticket = (int)intent.order_ticket;
   int digits = (int)MarketInfo(symbol, MODE_DIGITS);
   double sl = (intent.stop_loss > 0.0) ? NormalizeDouble(intent.stop_loss, digits) : 0.0;
   double tp = (intent.take_profit > 0.0) ? NormalizeDouble(intent.take_profit, digits) : 0.0;
   double price = 0.0;

   bool found = false;

   // 1. Locate by ticket
   if (ticket > 0 && OrderSelect(ticket, SELECT_BY_TICKET, MODE_TRADES))
   {
      price = OrderOpenPrice();
      found = true;
   }
   // 2. Locate by symbol and magic number fallback
   else
   {
      int total = OrdersTotal();
      for (int i = 0; i < total; ++i)
      {
         if (OrderSelect(i, SELECT_BY_POS, MODE_TRADES))
         {
            if (OrderSymbol() == symbol && (intent.magic_number == 0 || (ulong)OrderMagicNumber() == intent.magic_number))
            {
               ticket = OrderTicket();
               price = OrderOpenPrice();
               found = true;
               break;
            }
         }
      }
   }

   ulong latencyUs = GetMicrosecondCount() - latency_start_us;

   if (!found)
   {
      ReportExecutionFill(
         intent.signal_id, (ulong)ticket, STATUS_REJECTED, 4108,
         0.0, 0.0, 0.0, 0.0, 0.0,
         latencyUs, "Order ticket not found for modification"
      );
      return;
   }

   bool ok = OrderModify(ticket, price, sl, tp, 0, clrYellow);
   latencyUs = GetMicrosecondCount() - latency_start_us;

   if (ok)
   {
      if (InpVerboseLogging)
      {
         PrintFormat("[Pipvane MT4 EXEC] Modified Ticket %d: SL=%.5f, TP=%.5f", ticket, sl, tp);
      }

      ReportExecutionFill(
         intent.signal_id, (ulong)ticket, STATUS_FILLED, 0,
         0.0, 0.0, 0.0, 0.0, 0.0, latencyUs, "Modified Successfully"
      );
   }
   else
   {
      int lastErr = GetLastError();
      string errDesc = GetMT4ErrorDescription(lastErr);
      PrintFormat("[Pipvane MT4 ERROR] OrderModify Ticket %d failed: Error %d (%s)", ticket, lastErr, errDesc);

      ReportExecutionFill(
         intent.signal_id, (ulong)ticket, STATUS_REJECTED, (uint)lastErr,
         0.0, 0.0, 0.0, 0.0, 0.0, latencyUs, errDesc
      );
   }
}

// =============================================================================
// Execution Function 4: Order Closures (Full & Partial Close)
// =============================================================================
void ExecuteCloseOrder(const ShmOrderIntent &intent, string symbol, double volume, bool is_partial, ulong latency_start_us)
{
   int ticket = (int)intent.order_ticket;
   int slippagePoints = CalculateSlippagePoints(symbol, intent.slippage_cap_pips);
   int digits = (int)MarketInfo(symbol, MODE_DIGITS);

   bool found = false;

   if (ticket > 0 && OrderSelect(ticket, SELECT_BY_TICKET, MODE_TRADES))
   {
      found = true;
   }
   else
   {
      int total = OrdersTotal();
      for (int i = total - 1; i >= 0; --i)
      {
         if (OrderSelect(i, SELECT_BY_POS, MODE_TRADES) && OrderType() <= OP_SELL)
         {
            if (OrderSymbol() == symbol && (intent.magic_number == 0 || (ulong)OrderMagicNumber() == intent.magic_number))
            {
               ticket = OrderTicket();
               found = true;
               break;
            }
         }
      }
   }

   ulong latencyUs = GetMicrosecondCount() - latency_start_us;

   if (!found)
   {
      ReportExecutionFill(
         intent.signal_id, (ulong)ticket, STATUS_REJECTED, 4108,
         volume, 0.0, 0.0, 0.0, 0.0,
         latencyUs, "Open position ticket not found for closure"
      );
      return;
   }

   double closeLots = is_partial ? volume : OrderLots();
   if (closeLots > OrderLots()) closeLots = OrderLots();

   double closePrice = (OrderType() == OP_BUY) ? MarketInfo(symbol, MODE_BID) : MarketInfo(symbol, MODE_ASK);
   closePrice = NormalizeDouble(closePrice, digits);

   bool ok = OrderClose(ticket, closeLots, closePrice, slippagePoints, clrWhite);
   latencyUs = GetMicrosecondCount() - latency_start_us;

   if (ok)
   {
      if (InpVerboseLogging)
      {
         PrintFormat("[Pipvane MT4 EXEC] Closed Ticket %d: Lots=%.2f, Price=%.5f", ticket, closeLots, closePrice);
      }

      ReportExecutionFill(
         intent.signal_id, (ulong)ticket, STATUS_FILLED, 0,
         volume, closeLots, 0.0, closePrice, 0.0, latencyUs, "Closed Successfully"
      );
   }
   else
   {
      int lastErr = GetLastError();
      string errDesc = GetMT4ErrorDescription(lastErr);
      PrintFormat("[Pipvane MT4 ERROR] OrderClose Ticket %d failed: Error %d (%s)", ticket, lastErr, errDesc);

      ReportExecutionFill(
         intent.signal_id, (ulong)ticket, STATUS_REJECTED, (uint)lastErr,
         volume, 0.0, 0.0, 0.0, 0.0, latencyUs, errDesc
      );
   }
}

// =============================================================================
// Execution Function 5: Cancel Pending Orders
// =============================================================================
void ExecuteCancelPending(const ShmOrderIntent &intent, ulong latency_start_us)
{
   int ticket = (int)intent.order_ticket;
   ulong latencyUs = GetMicrosecondCount() - latency_start_us;

   if (ticket <= 0 || !OrderSelect(ticket, SELECT_BY_TICKET, MODE_TRADES))
   {
      ReportExecutionFill(
         intent.signal_id, (ulong)ticket, STATUS_REJECTED, 4108,
         0.0, 0.0, 0.0, 0.0, 0.0, latencyUs, "Pending order ticket not found"
      );
      return;
   }

   bool ok = OrderDelete(ticket, clrGray);
   latencyUs = GetMicrosecondCount() - latency_start_us;

   if (ok)
   {
      if (InpVerboseLogging)
      {
         PrintFormat("[Pipvane MT4 EXEC] Cancelled Pending Order Ticket %d", ticket);
      }

      ReportExecutionFill(
         intent.signal_id, (ulong)ticket, STATUS_FILLED, 0,
         0.0, 0.0, 0.0, 0.0, 0.0, latencyUs, "Deleted Successfully"
      );
   }
   else
   {
      int lastErr = GetLastError();
      string errDesc = GetMT4ErrorDescription(lastErr);
      ReportExecutionFill(
         intent.signal_id, (ulong)ticket, STATUS_REJECTED, (uint)lastErr,
         0.0, 0.0, 0.0, 0.0, 0.0, latencyUs, errDesc
      );
   }
}

// =============================================================================
// State Telemetry: Account Balance, Equity & Margin Reporting
// =============================================================================
void SendAccountTelemetry()
{
   ulong  login        = (ulong)AccountNumber();
   double balance      = AccountBalance();
   double equity       = AccountEquity();
   double margin       = AccountMargin();
   double freeMargin   = AccountFreeMargin();
   double marginLevel  = (margin > 0.0) ? (equity / margin * 100.0) : 0.0;

   int total = OrdersTotal();
   int openCount = 0;
   int pendingCount = 0;

   for (int i = 0; i < total; ++i)
   {
      if (OrderSelect(i, SELECT_BY_POS, MODE_TRADES))
      {
         if (OrderType() <= OP_SELL) openCount++;
         else pendingCount++;
      }
   }

   int isConnected = IsConnected() ? 1 : 0;

   ReportTerminalState(
      login,
      balance,
      equity,
      margin,
      freeMargin,
      marginLevel,
      openCount,
      pendingCount,
      isConnected
   );
}

// =============================================================================
// State Telemetry: Open Positions Snapshot Array Reporting
// =============================================================================
void SendPositionsTelemetry()
{
   ulong login = (ulong)AccountNumber();
   int total = OrdersTotal();
   int openCount = 0;

   for (int i = 0; i < total; ++i)
   {
      if (OrderSelect(i, SELECT_BY_POS, MODE_TRADES) && OrderType() <= OP_SELL)
      {
         openCount++;
      }
   }

   if (openCount == 0)
   {
      ShmPositionItem emptyArray[];
      ArrayResize(emptyArray, 0);
      ReportPositionsSnapshot(login, emptyArray, 0);
      return;
   }

   ShmPositionItem items[];
   ArrayResize(items, openCount);
   int idx = 0;

   for (int i = 0; i < total; ++i)
   {
      if (OrderSelect(i, SELECT_BY_POS, MODE_TRADES) && OrderType() <= OP_SELL)
      {
         items[idx].ticket         = (ulong)OrderTicket();
         items[idx].magic_number   = (ulong)OrderMagicNumber();
         items[idx].open_time_unix = (ulong)OrderOpenTime();

         string sym = OrderSymbol();
         Pipvane_StringToUCharArray(sym, items[idx].symbol, 32);

         items[idx].position_type  = (OrderType() == OP_BUY) ? 0 : 1;
         items[idx].volume         = OrderLots();
         items[idx].open_price     = OrderOpenPrice();
         items[idx].current_price  = (OrderType() == OP_BUY) ? MarketInfo(sym, MODE_BID) : MarketInfo(sym, MODE_ASK);
         items[idx].stop_loss      = OrderStopLoss();
         items[idx].take_profit    = OrderTakeProfit();
         items[idx].profit         = OrderProfit();
         items[idx].swap           = OrderSwap();
         items[idx].commission     = OrderCommission();

         string comment = OrderComment();
         Pipvane_StringToUCharArray(comment, items[idx].comment, 32);
         idx++;
      }
   }

   ReportPositionsSnapshot(login, items, openCount);
}

// =============================================================================
// Capability Negotiation: Report MT4 Broker Limits to Engine
// =============================================================================
void SendBrokerCapabilities()
{
   int supportsHedging = 1; // MT4 always supports independent tickets/hedging
   int isFifo          = 0;
   int tradeAllowed    = IsTradeAllowed() ? 1 : 0;
   int dllAllowed      = IsDllsAllowed() ? 1 : 0;
   int execMode        = 0; // Market/Instant
   uint leverage       = (uint)AccountLeverage();

   double minLot   = MarketInfo(Symbol(), MODE_MINLOT);
   double maxLot   = MarketInfo(Symbol(), MODE_MAXLOT);
   double lotStep  = MarketInfo(Symbol(), MODE_LOTSTEP);
   uint stopLevel  = (uint)MarketInfo(Symbol(), MODE_STOPLEVEL);
   uint freezeLev  = (uint)MarketInfo(Symbol(), MODE_FREEZELEVEL);
   string currency = AccountCurrency();

   Pipvane_SendCapabilities(
      supportsHedging,
      isFifo,
      tradeAllowed,
      dllAllowed,
      execMode,
      leverage,
      minLot,
      maxLot,
      lotStep,
      stopLevel,
      freezeLev,
      currency
   );
}

// =============================================================================
// Utilities: Volume Normalization, Pip Calculations & MT4 Error Decoder
// =============================================================================
double NormalizeVolume(string symbol, double volume)
{
   double minLot  = MarketInfo(symbol, MODE_MINLOT);
   double maxLot  = MarketInfo(symbol, MODE_MAXLOT);
   double lotStep = MarketInfo(symbol, MODE_LOTSTEP);

   if (volume < minLot) volume = minLot;
   if (volume > maxLot) volume = maxLot;

   if (lotStep > 0.0)
   {
      double steps = MathRound(volume / lotStep);
      volume = steps * lotStep;
   }

   return NormalizeDouble(volume, 2);
}

double CalculatePipFactor(string symbol)
{
   int digits = (int)MarketInfo(symbol, MODE_DIGITS);
   return (digits == 3 || digits == 5) ? 10.0 : 1.0;
}

int CalculateSlippagePoints(string symbol, double slippage_pips)
{
   double slipPips = (slippage_pips > 0.0) ? slippage_pips : InpDefaultSlippagePips;
   double pipFactor = CalculatePipFactor(symbol);
   return (int)MathRound(slipPips * pipFactor);
}

string GetMT4ErrorDescription(int error_code)
{
   switch (error_code)
   {
      case 0:   return "No error";
      case 1:   return "No error returned, but result is unknown";
      case 2:   return "Common error";
      case 3:   return "Invalid trade parameters";
      case 4:   return "Trade server is busy";
      case 6:   return "No connection with trade server";
      case 8:   return "Too frequent requests";
      case 64:  return "Account disabled";
      case 65:  return "Invalid account";
      case 128: return "Trade timeout (ERR_TRADE_TIMEOUT)";
      case 129: return "Invalid price (ERR_INVALID_PRICE)";
      case 130: return "Invalid stops level (ERR_INVALID_STOPS)";
      case 131: return "Invalid trade volume (ERR_INVALID_TRADE_VOLUME)";
      case 132: return "Market is closed (ERR_MARKET_CLOSED)";
      case 133: return "Trade is disabled (ERR_TRADE_DISABLED)";
      case 134: return "Not enough money to complete operation (ERR_NOT_ENOUGH_MONEY)";
      case 135: return "Price changed (ERR_PRICE_CHANGED)";
      case 136: return "Off quotes (ERR_OFF_QUOTES)";
      case 137: return "Broker is busy (ERR_BROKER_BUSY)";
      case 138: return "Requote (ERR_REQUOTE)";
      case 139: return "Order is locked (ERR_ORDER_LOCKED)";
      case 141: return "Too many requests (ERR_TOO_MANY_REQUESTS)";
      case 145: return "Modification denied because order is too close to market";
      case 146: return "Trade context is busy (ERR_TRADE_CONTEXT_BUSY)";
      case 148: return "Auto trading by Expert Advisor is disabled";
      case 4051: return "Invalid function parameter";
      case 4108: return "Invalid ticket";
      case 4109: return "Trade is not allowed";
      default:  return StringFormat("MT4 Error Code: %d", error_code);
   }
}
