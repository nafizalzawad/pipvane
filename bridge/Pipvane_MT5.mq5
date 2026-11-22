//+------------------------------------------------------------------+
//|                                                 Pipvane_MT5.mq5  |
//|               Pipvane v3: High-Performance MQL5 Expert Advisor   |
//|        Direct Lock-Free IPC Execution & Real-Time Telemetry     |
//|      Conforms strictly to SPRINT_1_ARCHITECTURE_BLUEPRINT.md     |
//+------------------------------------------------------------------+
#property copyright   "Copyright 2026, Pipvane Team"
#property link        "https://pipvane.com"
#property version     "3.00"
#property description "Pipvane v3 Native Execution Wrapper for MetaTrader 5"
#property strict

// Standard MQL5 Trade Library Includes
#include <Trade\Trade.mqh>
#include <Trade\SymbolInfo.mqh>
#include <Trade\AccountInfo.mqh>
#include <Trade\PositionInfo.mqh>
#include <Trade\OrderInfo.mqh>

// Pipvane Native C++ Bridge Header
#include "PipvaneBridge.mqh"

// =============================================================================
// Input Parameters
// =============================================================================
input group "=== Pipvane IPC Bridge Configuration ==="
input string   InpTerminalId           = "term_01";      // Terminal ID (Must match Engine config)
input uint     InpPollIntervalMs       = 5;              // Order polling cadence in ms (<10ms recommended)
input uint     InpTelemetryIntervalMs  = 1000;           // Telemetry reporting interval in ms
input double   InpDefaultSlippagePips  = 3.0;            // Default slippage tolerance (pips)
input ulong    InpDefaultMagic         = 999888;         // Fallback Magic Number
input bool     InpVerboseLogging       = true;           // Diagnostic logging to Experts tab
input bool     InpAutoSelectSymbol     = true;           // Auto-add instrument to Market Watch if missing

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
// Global Execution Objects & State
// =============================================================================
CTrade         ExtTrade;
CSymbolInfo    ExtSymbolInfo;
CAccountInfo   ExtAccountInfo;
CPositionInfo  ExtPositionInfo;
COrderInfo     ExtOrderInfo;

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
void   ConfigureSymbolFilling(string symbol);
double CalculatePipFactor(string symbol);

//+------------------------------------------------------------------+
//| Expert initialization function                                   |
//+------------------------------------------------------------------+
int OnInit()
{
   PrintFormat("[Pipvane] Initializing MT5 Expert Advisor on Build %d (64-bit)...", TerminalInfoInteger(TERMINAL_BUILD));

   // 1. Check automated trading permissions
   if (!TerminalInfoInteger(TERMINAL_TRADE_ALLOWED))
   {
      Alert("[Pipvane CRITICAL] Algo Trading button is DISABLED on MetaTrader terminal! Enable it to execute trades.");
   }

   if (!MQLInfoInteger(MQL_TRADE_ALLOWED))
   {
      Alert("[Pipvane CRITICAL] Automated trading is not allowed in EA properties! Check 'Allow Algo Trading'.");
   }

   // 2. Initialize C++ Native Bridge (Shared Memory + Named Pipe)
   string serverName  = AccountInfoString(ACCOUNT_SERVER);
   string companyName = AccountInfoString(ACCOUNT_COMPANY);
   ulong  accountLogin = (ulong)AccountInfoInteger(ACCOUNT_LOGIN);
   int    isPortable   = TerminalInfoInteger(TERMINAL_PORTABLE) ? 1 : 0;
   int    build        = (int)TerminalInfoInteger(TERMINAL_BUILD);

   int initRes = Pipvane_InitW(
      InpTerminalId,
      1, // Platform: 1 = MT5
      build,
      isPortable,
      accountLogin,
      serverName,
      companyName
   );

   if (initRes != 0)
   {
      PrintFormat("[Pipvane ERROR] Failed to attach to C++ IPC Bridge (code: %d). Error: %s",
                  initRes, Pipvane_GetLastError());
      return INIT_FAILED;
   }

   ExtBridgeInitialized = true;
   PrintFormat("[Pipvane] Connected to IPC Bridge: Terminal ID='%s', Login=%I64u, Server='%s'",
               InpTerminalId, accountLogin, serverName);

   // 3. Configure default Trade parameters
   ExtTrade.SetExpertMagicNumber(InpDefaultMagic);
   ExtTrade.SetMarginMode();

   // 4. Send broker capability negotiation packet to Engine
   SendBrokerCapabilities();

   // 5. Initial state telemetry sync
   SendAccountTelemetry();
   SendPositionsTelemetry();
   ExtLastTelemetryTick = GetTickCount64();

   // 6. Start high-resolution millisecond timer for sub-millisecond polling
   uint pollMs = (InpPollIntervalMs < 1) ? 1 : InpPollIntervalMs;
   if (!EventSetMillisecondTimer(pollMs))
   {
      PrintFormat("[Pipvane WARNING] EventSetMillisecondTimer(%u ms) failed. Falling back to OnTick polling.", pollMs);
   }
   else
   {
      PrintFormat("[Pipvane] High-resolution polling timer armed at %u ms cadence.", pollMs);
   }

   return INIT_SUCCEEDED;
}

//+------------------------------------------------------------------+
//| Expert deinitialization function                                 |
//+------------------------------------------------------------------+
void OnDeinit(const int reason)
{
   PrintFormat("[Pipvane] Deinitializing EA (Reason: %d)...", reason);

   // 1. Kill polling timer
   EventKillTimer();

   // 2. Send final telemetry state before detaching
   if (ExtBridgeInitialized)
   {
      SendAccountTelemetry();
      Pipvane_Shutdown();
      ExtBridgeInitialized = false;
   }

   Print("[Pipvane] Detached from IPC Bridge cleanly.");
}

//+------------------------------------------------------------------+
//| Expert tick function (Executed on every incoming market tick)   |
//+------------------------------------------------------------------+
void OnTick()
{
   if (!ExtBridgeInitialized) return;

   // 1. Fast Lock-Free Order Polling (<50 ns uncontended check)
   PollAndProcessOrders();

   // 2. Throttled Telemetry update on market tick
   ulong now = GetTickCount64();
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

   // 1. Fast Lock-Free Order Polling (guarantees execution even when market tick is idle)
   PollAndProcessOrders();

   // 2. Periodic state telemetry heartbeat
   ulong now = GetTickCount64();
   if (now - ExtLastTelemetryTick >= InpTelemetryIntervalMs)
   {
      SendAccountTelemetry();
      SendPositionsTelemetry();
      ExtLastTelemetryTick = now;
   }
}

//+------------------------------------------------------------------+
//| Trade Transaction Event (Instant state sync upon order fill)     |
//+------------------------------------------------------------------+
void OnTradeTransaction(
   const MqlTradeTransaction &trans,
   const MqlTradeRequest     &request,
   const MqlTradeResult      &result
)
{
   if (!ExtBridgeInitialized) return;

   // Immediately push state to Engine when position/order state transitions
   if (trans.type == TRADE_TRANSACTION_DEAL_ADD ||
       trans.type == TRADE_TRANSACTION_ORDER_DELETE ||
       trans.type == TRADE_TRANSACTION_POSITION)
   {
      SendAccountTelemetry();
      SendPositionsTelemetry();
   }
}

// =============================================================================
// Core Fast-Path: Lock-Free Shared Memory Poller & Dispatcher
// =============================================================================
void PollAndProcessOrders()
{
   ShmOrderIntent intent;

   // Lock-free ring buffer drain loop:
   // Pipvane_PollOrderIntent executes in ~20-50ns and returns 0 immediately if empty.
   // Never blocks the main GUI or tick processing thread.
   while (Pipvane_PollOrderIntent(intent) == 1)
   {
      ProcessOrderIntent(intent);
   }
}

// =============================================================================
// Order Intent Processor & Route Dispatcher
// =============================================================================
void ProcessOrderIntent(const ShmOrderIntent &intent)
{
   // Measure internal execution latency
   ulong latencyStartUs = GetMicrosecondCount();

   // Parse null-terminated instrument symbol
   string symbol = Pipvane_UCharArrayToString(intent.symbol);
   StringTrimLeft(symbol);
   StringTrimRight(symbol);

   // Fallback to current chart symbol if empty
   if (symbol == "")
   {
      symbol = _Symbol;
   }

   // Ensure symbol is available in Market Watch
   if (InpAutoSelectSymbol && !SymbolInfoInteger(symbol, SYMBOL_SELECT))
   {
      if (!SymbolSelect(symbol, true))
      {
         ulong latencyUs = GetMicrosecondCount() - latencyStartUs;
         PrintFormat("[Pipvane ERROR] Failed to select symbol '%s' in Market Watch.", symbol);
         Pipvane_ReportFeedback(
            intent.signal_id, 0, STATUS_REJECTED, 4301,
            intent.volume, 0.0, intent.price, 0.0, 0.0,
            latencyUs, "Symbol not available in Market Watch"
         );
         return;
      }
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
         PrintFormat("[Pipvane ERROR] Unrecognized Action Type: %d", intent.action_type);
         Pipvane_ReportFeedback(
            intent.signal_id, 0, STATUS_REJECTED, 10013,
            intent.volume, 0.0, intent.price, 0.0, 0.0,
            latencyUs, "Invalid Action Type"
         );
         break;
      }
   }
}

// =============================================================================
// Execution Function 1: Market Orders (BUY / SELL)
// =============================================================================
void ExecuteMarketOrder(const ShmOrderIntent &intent, string symbol, double volume, ulong latency_start_us)
{
   // Configure Magic Number
   ulong magic = (intent.magic_number > 0) ? intent.magic_number : InpDefaultMagic;
   ExtTrade.SetExpertMagicNumber(magic);

   // Configure Broker Filling Mode (FOK, IOC, RETURN)
   ConfigureSymbolFilling(symbol);

   // Configure Slippage Deviation in Points
   double pipFactor = CalculatePipFactor(symbol);
   double slipPips = (intent.slippage_cap_pips > 0.0) ? intent.slippage_cap_pips : InpDefaultSlippagePips;
   ulong deviationPoints = (ulong)(slipPips * pipFactor);
   ExtTrade.SetDeviationInPoints(deviationPoints);

   // Set comment with signal identifier
   string comment = StringFormat("PV:%I64u", intent.signal_id);

   // Execute OrderSend via CTrade
   bool success = false;
   if (intent.side == SIDE_BUY)
   {
      double ask = SymbolInfoDouble(symbol, SYMBOL_ASK);
      double entryPrice = (intent.price > 0.0) ? intent.price : ask;
      success = ExtTrade.Buy(volume, symbol, entryPrice, intent.stop_loss, intent.take_profit, comment);
   }
   else if (intent.side == SIDE_SELL)
   {
      double bid = SymbolInfoDouble(symbol, SYMBOL_BID);
      double entryPrice = (intent.price > 0.0) ? intent.price : bid;
      success = ExtTrade.Sell(volume, symbol, entryPrice, intent.stop_loss, intent.take_profit, comment);
   }
   else
   {
      ulong latUs = GetMicrosecondCount() - latency_start_us;
      Pipvane_ReportFeedback(intent.signal_id, 0, STATUS_REJECTED, 10013, volume, 0.0, intent.price, 0.0, 0.0, latUs, "Invalid Order Side");
      return;
   }

   // Measure internal execution latency in microseconds
   ulong latencyUs = GetMicrosecondCount() - latency_start_us;

   // Retrieve Broker Result
   uint   retcode   = ExtTrade.ResultRetcode();
   ulong  orderNum  = ExtTrade.ResultOrder();
   ulong  dealNum   = ExtTrade.ResultDeal();
   ulong  ticket    = (dealNum > 0) ? dealNum : orderNum;
   double execVol   = ExtTrade.ResultVolume();
   double execPrice = ExtTrade.ResultPrice();

   // Compute measured slippage in pips
   double point = SymbolInfoDouble(symbol, SYMBOL_POINT);
   double slippagePips = 0.0;
   if (intent.price > 0.0 && execPrice > 0.0 && point > 0.0)
   {
      slippagePips = MathAbs(execPrice - intent.price) / (point * pipFactor);
   }

   // Evaluate Execution Status
   int execStatus = STATUS_REJECTED;
   if (retcode == TRADE_RETCODE_DONE)
   {
      execStatus = (execVol < volume && execVol > 0.0) ? STATUS_PARTIAL : STATUS_FILLED;
   }
   else if (retcode == TRADE_RETCODE_DONE_PARTIAL)
   {
      execStatus = STATUS_PARTIAL;
   }
   else if (retcode == TRADE_RETCODE_REQUOTE || retcode == TRADE_RETCODE_PRICE_CHANGED)
   {
      execStatus = STATUS_REQUOTE;
   }
   else
   {
      execStatus = STATUS_REJECTED;
   }

   string desc = ExtTrade.ResultRetcodeDescription();
   if (desc == "") desc = StringFormat("Retcode: %u", retcode);

   if (InpVerboseLogging)
   {
      PrintFormat("[Pipvane EXEC] Market %s %s: Vol=%.2f, Ticket=%I64u, Price=%.5f, Status=%d, Latency=%I64u us, Retcode=%u (%s)",
                  (intent.side == SIDE_BUY ? "BUY" : "SELL"), symbol, execVol, ticket, execPrice, execStatus, latencyUs, retcode, desc);
   }

   // Send Execution Feedback over Named Pipe to Engine
   Pipvane_ReportFeedback(
      intent.signal_id,
      ticket,
      execStatus,
      retcode,
      volume,
      execVol,
      intent.price,
      execPrice,
      slippagePips,
      latencyUs,
      desc
   );
}

// =============================================================================
// Execution Function 2: Pending Orders (LIMIT / STOP)
// =============================================================================
void ExecutePendingOrder(const ShmOrderIntent &intent, string symbol, double volume, ulong latency_start_us)
{
   ulong magic = (intent.magic_number > 0) ? intent.magic_number : InpDefaultMagic;
   ExtTrade.SetExpertMagicNumber(magic);
   ConfigureSymbolFilling(symbol);

   string comment = StringFormat("PV:%I64u", intent.signal_id);
   bool success = false;

   if (intent.action_type == ACTION_LIMIT_ENTRY)
   {
      if (intent.side == SIDE_BUY)
      {
         success = ExtTrade.BuyLimit(volume, intent.price, symbol, intent.stop_loss, intent.take_profit, ORDER_TIME_GTC, 0, comment);
      }
      else
      {
         success = ExtTrade.SellLimit(volume, intent.price, symbol, intent.stop_loss, intent.take_profit, ORDER_TIME_GTC, 0, comment);
      }
   }
   else if (intent.action_type == ACTION_STOP_ENTRY)
   {
      if (intent.side == SIDE_BUY)
      {
         success = ExtTrade.BuyStop(volume, intent.price, symbol, intent.stop_loss, intent.take_profit, ORDER_TIME_GTC, 0, comment);
      }
      else
      {
         success = ExtTrade.SellStop(volume, intent.price, symbol, intent.stop_loss, intent.take_profit, ORDER_TIME_GTC, 0, comment);
      }
   }

   ulong latencyUs = GetMicrosecondCount() - latency_start_us;
   uint  retcode   = ExtTrade.ResultRetcode();
   ulong orderNum  = ExtTrade.ResultOrder();
   string desc     = ExtTrade.ResultRetcodeDescription();

   int execStatus = (retcode == TRADE_RETCODE_DONE) ? STATUS_FILLED : STATUS_REJECTED;

   if (InpVerboseLogging)
   {
      PrintFormat("[Pipvane EXEC] Pending Order %s: Vol=%.2f, Price=%.5f, Ticket=%I64u, Retcode=%u (%s)",
                  symbol, volume, intent.price, orderNum, retcode, desc);
   }

   Pipvane_ReportFeedback(
      intent.signal_id,
      orderNum,
      execStatus,
      retcode,
      volume,
      (execStatus == STATUS_FILLED ? volume : 0.0),
      intent.price,
      intent.price,
      0.0,
      latencyUs,
      desc
   );
}

// =============================================================================
// Execution Function 3: Order Modifications (SL / TP)
// =============================================================================
void ExecuteModifyOrder(const ShmOrderIntent &intent, string symbol, ulong latency_start_us)
{
   ulong ticket = intent.order_ticket;
   bool success = false;
   uint retcode = 0;
   string desc = "";

   // Check if ticket is an open position
   if (ExtPositionInfo.SelectByTicket(ticket))
   {
      success = ExtTrade.PositionModify(ticket, intent.stop_loss, intent.take_profit);
      retcode = ExtTrade.ResultRetcode();
      desc    = ExtTrade.ResultRetcodeDescription();
   }
   // Check if ticket is an active pending order
   else if (ExtOrderInfo.Select(ticket))
   {
      double orderPrice = (intent.price > 0.0) ? intent.price : ExtOrderInfo.PriceOpen();
      success = ExtTrade.OrderModify(ticket, orderPrice, intent.stop_loss, intent.take_profit, ExtOrderInfo.TypeTime(), ExtOrderInfo.TimeExpiration());
      retcode = ExtTrade.ResultRetcode();
      desc    = ExtTrade.ResultRetcodeDescription();
   }
   else
   {
      // If specific ticket was not found, attempt to find position by symbol and magic number
      int posTotal = PositionsTotal();
      bool found = false;
      for (int i = 0; i < posTotal; ++i)
      {
         ulong posTicket = PositionGetTicket(i);
         if (posTicket > 0 && PositionGetString(POSITION_SYMBOL) == symbol)
         {
            if (intent.magic_number == 0 || (ulong)PositionGetInteger(POSITION_MAGIC) == intent.magic_number)
            {
               ticket = posTicket;
               success = ExtTrade.PositionModify(ticket, intent.stop_loss, intent.take_profit);
               retcode = ExtTrade.ResultRetcode();
               desc    = ExtTrade.ResultRetcodeDescription();
               found = true;
               break;
            }
         }
      }

      if (!found)
      {
         ulong latencyUs = GetMicrosecondCount() - latency_start_us;
         Pipvane_ReportFeedback(
            intent.signal_id, ticket, STATUS_REJECTED, 10017,
            0.0, 0.0, 0.0, 0.0, 0.0,
            latencyUs, "Target position or order ticket not found"
         );
         return;
      }
   }

   ulong latencyUs = GetMicrosecondCount() - latency_start_us;
   int execStatus = (retcode == TRADE_RETCODE_DONE) ? STATUS_FILLED : STATUS_REJECTED;

   if (InpVerboseLogging)
   {
      PrintFormat("[Pipvane EXEC] Modify Ticket %I64u: SL=%.5f, TP=%.5f, Retcode=%u (%s)",
                  ticket, intent.stop_loss, intent.take_profit, retcode, desc);
   }

   Pipvane_ReportFeedback(
      intent.signal_id,
      ticket,
      execStatus,
      retcode,
      0.0,
      0.0,
      0.0,
      0.0,
      0.0,
      latencyUs,
      desc
   );
}

// =============================================================================
// Execution Function 4: Order Closures (Full & Partial Close)
// =============================================================================
void ExecuteCloseOrder(const ShmOrderIntent &intent, string symbol, double volume, bool is_partial, ulong latency_start_us)
{
   ulong ticket = intent.order_ticket;
   bool success = false;
   uint retcode = 0;
   string desc = "";
   double closedVol = 0.0;

   // 1. Close specific ticket if provided
   if (ticket > 0 && ExtPositionInfo.SelectByTicket(ticket))
   {
      if (is_partial && volume > 0.0 && volume < ExtPositionInfo.Volume())
      {
         success = ExtTrade.PositionClosePartial(ticket, volume);
         closedVol = volume;
      }
      else
      {
         success = ExtTrade.PositionClose(ticket);
         closedVol = ExtPositionInfo.Volume();
      }
      retcode = ExtTrade.ResultRetcode();
      desc    = ExtTrade.ResultRetcodeDescription();
   }
   // 2. Close by symbol and magic number if no ticket specified
   else
   {
      int posTotal = PositionsTotal();
      bool found = false;
      for (int i = posTotal - 1; i >= 0; --i)
      {
         ulong posTicket = PositionGetTicket(i);
         if (posTicket > 0 && PositionGetString(POSITION_SYMBOL) == symbol)
         {
            if (intent.magic_number == 0 || (ulong)PositionGetInteger(POSITION_MAGIC) == intent.magic_number)
            {
               ticket = posTicket;
               if (is_partial && volume > 0.0 && volume < PositionGetDouble(POSITION_VOLUME))
               {
                  success = ExtTrade.PositionClosePartial(ticket, volume);
                  closedVol = volume;
               }
               else
               {
                  success = ExtTrade.PositionClose(ticket);
                  closedVol = PositionGetDouble(POSITION_VOLUME);
               }
               retcode = ExtTrade.ResultRetcode();
               desc    = ExtTrade.ResultRetcodeDescription();
               found = true;
               break;
            }
         }
      }

      if (!found)
      {
         ulong latencyUs = GetMicrosecondCount() - latency_start_us;
         Pipvane_ReportFeedback(
            intent.signal_id, ticket, STATUS_REJECTED, 10017,
            volume, 0.0, 0.0, 0.0, 0.0,
            latencyUs, "No matching open position found to close"
         );
         return;
      }
   }

   ulong latencyUs = GetMicrosecondCount() - latency_start_us;
   int execStatus = (retcode == TRADE_RETCODE_DONE) ? STATUS_FILLED : STATUS_REJECTED;

   if (InpVerboseLogging)
   {
      PrintFormat("[Pipvane EXEC] Close Position %I64u: Vol=%.2f, Partial=%s, Retcode=%u (%s)",
                  ticket, closedVol, (is_partial ? "Yes" : "No"), retcode, desc);
   }

   Pipvane_ReportFeedback(
      intent.signal_id,
      ticket,
      execStatus,
      retcode,
      volume,
      (execStatus == STATUS_FILLED ? closedVol : 0.0),
      0.0,
      ExtTrade.ResultPrice(),
      0.0,
      latencyUs,
      desc
   );
}

// =============================================================================
// Execution Function 5: Cancel Pending Orders
// =============================================================================
void ExecuteCancelPending(const ShmOrderIntent &intent, ulong latency_start_us)
{
   ulong ticket = intent.order_ticket;
   bool success = false;
   uint retcode = 0;
   string desc = "";

   if (ticket > 0 && ExtOrderInfo.Select(ticket))
   {
      success = ExtTrade.OrderDelete(ticket);
      retcode = ExtTrade.ResultRetcode();
      desc    = ExtTrade.ResultRetcodeDescription();
   }
   else
   {
      ulong latencyUs = GetMicrosecondCount() - latency_start_us;
      Pipvane_ReportFeedback(
         intent.signal_id, ticket, STATUS_REJECTED, 10017,
         0.0, 0.0, 0.0, 0.0, 0.0,
         latencyUs, "Pending order ticket not found"
      );
      return;
   }

   ulong latencyUs = GetMicrosecondCount() - latency_start_us;
   int execStatus = (retcode == TRADE_RETCODE_DONE) ? STATUS_FILLED : STATUS_REJECTED;

   if (InpVerboseLogging)
   {
      PrintFormat("[Pipvane EXEC] Cancel Pending Order %I64u: Retcode=%u (%s)", ticket, retcode, desc);
   }

   Pipvane_ReportFeedback(
      intent.signal_id,
      ticket,
      execStatus,
      retcode,
      0.0,
      0.0,
      0.0,
      0.0,
      0.0,
      latencyUs,
      desc
   );
}

// =============================================================================
// State Telemetry: Account Balance, Equity & Margin Reporting
// =============================================================================
void SendAccountTelemetry()
{
   ulong  login        = (ulong)AccountInfoInteger(ACCOUNT_LOGIN);
   double balance      = AccountInfoDouble(ACCOUNT_BALANCE);
   double equity       = AccountInfoDouble(ACCOUNT_EQUITY);
   double margin       = AccountInfoDouble(ACCOUNT_MARGIN);
   double freeMargin   = AccountInfoDouble(ACCOUNT_MARGIN_FREE);
   double marginLevel  = AccountInfoDouble(ACCOUNT_MARGIN_LEVEL);
   int    openCount    = PositionsTotal();
   int    pendingCount = OrdersTotal();
   int    isConnected  = TerminalInfoInteger(TERMINAL_CONNECTED) ? 1 : 0;

   Pipvane_SendAccountState(
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
   ulong login = (ulong)AccountInfoInteger(ACCOUNT_LOGIN);
   int total = PositionsTotal();

   if (total == 0)
   {
      ShmPositionItem emptyArray[];
      ArrayResize(emptyArray, 0);
      Pipvane_SendPositionsSync(login, emptyArray, 0);
      return;
   }

   ShmPositionItem items[];
   ArrayResize(items, total);

   for (int i = 0; i < total; ++i)
   {
      ulong ticket = PositionGetTicket(i);
      if (ticket > 0)
      {
         items[i].ticket         = ticket;
         items[i].magic_number   = (ulong)PositionGetInteger(POSITION_MAGIC);
         items[i].open_time_unix = (ulong)PositionGetInteger(POSITION_TIME);

         string sym = PositionGetString(POSITION_SYMBOL);
         Pipvane_StringToUCharArray(sym, items[i].symbol, 32);

         ENUM_POSITION_TYPE pType = (ENUM_POSITION_TYPE)PositionGetInteger(POSITION_TYPE);
         items[i].position_type  = (pType == POSITION_TYPE_BUY) ? 0 : 1;

         items[i].volume         = PositionGetDouble(POSITION_VOLUME);
         items[i].open_price     = PositionGetDouble(POSITION_PRICE_OPEN);
         items[i].current_price  = PositionGetDouble(POSITION_PRICE_CURRENT);
         items[i].stop_loss      = PositionGetDouble(POSITION_SL);
         items[i].take_profit    = PositionGetDouble(POSITION_TP);
         items[i].profit         = PositionGetDouble(POSITION_PROFIT);
         items[i].swap           = PositionGetDouble(POSITION_SWAP);
         items[i].commission     = PositionGetDouble(POSITION_COMMISSION);

         string comment = PositionGetString(POSITION_COMMENT);
         Pipvane_StringToUCharArray(comment, items[i].comment, 32);
      }
   }

   Pipvane_SendPositionsSync(login, items, total);
}

// =============================================================================
// Capability Negotiation: Report Broker & Account Constraints to Engine
// =============================================================================
void SendBrokerCapabilities()
{
   ENUM_ACCOUNT_MARGIN_MODE marginMode = (ENUM_ACCOUNT_MARGIN_MODE)AccountInfoInteger(ACCOUNT_MARGIN_MODE);
   int supportsHedging = (marginMode == ACCOUNT_MARGIN_MODE_RETAIL_HEDGING) ? 1 : 0;
   int isFifo          = 0; // Standard MT5 retail default
   int tradeAllowed    = MQLInfoInteger(MQL_TRADE_ALLOWED) ? 1 : 0;
   int dllAllowed      = MQLInfoInteger(MQL_DLLS_ALLOWED) ? 1 : 0;
   int execMode        = (int)SymbolInfoInteger(_Symbol, SYMBOL_TRADE_EXEMODE);
   uint leverage       = (uint)AccountInfoInteger(ACCOUNT_LEVERAGE);

   double minLot   = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
   double maxLot   = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MAX);
   double lotStep  = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
   uint stopLevel  = (uint)SymbolInfoInteger(_Symbol, SYMBOL_TRADE_STOPS_LEVEL);
   uint freezeLev  = (uint)SymbolInfoInteger(_Symbol, SYMBOL_TRADE_FREEZE_LEVEL);
   string currency = AccountInfoString(ACCOUNT_CURRENCY);

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
// Utilities: Volume Normalization, Pip Calculations & Smart Filling Mode
// =============================================================================
double NormalizeVolume(string symbol, double volume)
{
   double minLot  = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MIN);
   double maxLot  = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MAX);
   double lotStep = SymbolInfoDouble(symbol, SYMBOL_VOLUME_STEP);

   if (volume < minLot) volume = minLot;
   if (volume > maxLot) volume = maxLot;

   if (lotStep > 0.0)
   {
      double steps = MathRound(volume / lotStep);
      volume = steps * lotStep;
   }

   int digits = 2;
   if (lotStep == 0.1) digits = 1;
   else if (lotStep == 0.01) digits = 2;
   else if (lotStep == 0.001) digits = 3;

   return NormalizeDouble(volume, digits);
}

void ConfigureSymbolFilling(string symbol)
{
   uint filling = (uint)SymbolInfoInteger(symbol, SYMBOL_FILLING_MODE);
   if ((filling & SYMBOL_FILLING_IOC) != 0)
   {
      ExtTrade.SetTypeFilling(ORDER_FILLING_IOC);
   }
   else if ((filling & SYMBOL_FILLING_FOK) != 0)
   {
      ExtTrade.SetTypeFilling(ORDER_FILLING_FOK);
   }
   else
   {
      ExtTrade.SetTypeFilling(ORDER_FILLING_RETURN);
   }
}

double CalculatePipFactor(string symbol)
{
   int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
   return (digits == 3 || digits == 5) ? 10.0 : 1.0;
}
