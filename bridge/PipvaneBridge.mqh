//+------------------------------------------------------------------+
//|                                             PipvaneBridge.mqh    |
//|               Pipvane v3: MetaTrader 4 & 5 Native C++ IPC Bridge  |
//|      Conforms strictly to SPRINT_1_ARCHITECTURE_BLUEPRINT.md     |
//+------------------------------------------------------------------+
#property copyright "Copyright 2026, Pipvane Team"
#property link      "https://pipvane.com"
#property strict

// =============================================================================
// Struct Definitions (Strict byte alignment matching C++ DLL)
// =============================================================================

// Order Intent POD Struct (128 bytes, 64-byte Cache Line Aligned)
struct ShmOrderIntent
{
   uchar    symbol[32];         // 0x00: Null-terminated broker symbol
   uchar    side;               // 0x20: 0 = BUY, 1 = SELL
   uchar    action_type;        // 0x21: 1 = Market, 2 = Limit, 4 = Modify, etc.
   uchar    _padding_1[6];      // 0x22 - 0x27: Alignment padding
   double   volume;             // 0x28: Lot size
   double   price;              // 0x30: Requested entry price
   double   stop_loss;          // 0x38: Absolute Stop Loss
   double   take_profit;        // 0x40: Absolute Take Profit (CL 1 boundary)
   double   slippage_cap_pips;  // 0x48: Max permitted slippage in pips
   ulong    magic_number;       // 0x50: EA unique magic number
   ulong    order_ticket;       // 0x58: Target ticket for modify/close
   ulong    signal_id;          // 0x60: Idempotency token hash
   ulong    timestamp_ns;       // 0x68: Dispatch timestamp in UTC nanoseconds
   uint     flags;              // 0x70: Execution flags (IOC, FOK, etc.)
   uint     crc32;              // 0x74: IEEE 802.3 CRC32 checksum
   ulong    reserved;           // 0x78: Expansion padding
};

// Terminal Account State Payload (64 bytes)
struct ShmAccountState
{
   ulong    account_login;
   double   balance;
   double   equity;
   double   margin;
   double   free_margin;
   double   margin_level;
   uint     open_positions_count;
   uint     pending_orders_count;
   uchar    is_connected_to_broker;
   uchar    reserved[7];
};

// Position Snapshot Item
struct ShmPositionItem
{
   ulong    ticket;
   ulong    magic_number;
   ulong    open_time_unix;
   uchar    symbol[32];
   uchar    position_type;      // 0: BUY, 1: SELL
   uchar    reserved[7];
   double   volume;
   double   open_price;
   double   current_price;
   double   stop_loss;
   double   take_profit;
   double   profit;
   double   swap;
   double   commission;
   uchar    comment[32];
};

// =============================================================================
// Native C++ DLL Imports
// MT5 (64-bit) links to PipvaneBridge.dll
// MT4 (32-bit) links to ipc_bridge_x86.dll
// =============================================================================
#ifdef __MQL5__
#import "PipvaneBridge.dll"
#else
#import "ipc_bridge_x86.dll"
#endif

// MT5 / MT4 Primary Direct Exports (Sprint 10 / Sprint 11)
int  GetNextOrderIntent(ShmOrderIntent &out_intent);

int  ReportTerminalState(
   ulong    account_login,
   double   balance,
   double   equity,
   double   margin,
   double   free_margin,
   double   margin_level,
   int      open_positions_count,
   int      pending_orders_count,
   int      is_connected_to_broker
);

int  ReportExecutionFill(
   ulong    signal_id,
   ulong    order_ticket,
   int      execution_status,
   uint     broker_retcode,
   double   requested_volume,
   double   executed_volume,
   double   requested_price,
   double   executed_price,
   double   slippage_pips,
   ulong    execution_latency_us,
   string   error_description
);

int  ReportPositionsSnapshot(
   ulong                 account_login,
   const ShmPositionItem &positions_array[],
   int                   position_count
);

int  InitializeBridgeW(
   string   terminal_id,
   int      platform_type,
   int      terminal_build,
   int      is_portable,
   ulong    account_login,
   string   broker_server,
   string   company_name
);

void ShutdownBridge();
int  IsBridgeConnected();

// Compatibility Aliases (Sprint 3)
int  Pipvane_InitW(
   string   terminal_id,
   int      platform_type,
   int      terminal_build,
   int      is_portable,
   ulong    account_login,
   string   broker_server,
   string   company_name
);

void Pipvane_Shutdown();
int  Pipvane_IsConnected();

int  Pipvane_PollOrderIntent(ShmOrderIntent &out_intent);

int  Pipvane_PollOrderIntentDirect(
   uchar    &out_symbol[],
   int      symbol_buf_len,
   int      &out_side,
   int      &out_action_type,
   double   &out_volume,
   double   &out_price,
   double   &out_stop_loss,
   double   &out_take_profit,
   double   &out_slippage_cap_pips,
   ulong    &out_magic_number,
   ulong    &out_order_ticket,
   ulong    &out_signal_id,
   uint     &out_flags
);

int  Pipvane_WaitForOrderIntent(uint timeout_ms, ShmOrderIntent &out_intent);

int  Pipvane_SendAccountState(
   ulong    account_login,
   double   balance,
   double   equity,
   double   margin,
   double   free_margin,
   double   margin_level,
   int      open_positions_count,
   int      pending_orders_count,
   int      is_connected_to_broker
);

int  Pipvane_SendExecutionFeedback(
   ulong    signal_id,
   ulong    order_ticket,
   int      execution_status,
   uint     broker_retcode,
   double   requested_volume,
   double   executed_volume,
   double   requested_price,
   double   executed_price,
   double   slippage_pips,
   ulong    execution_latency_us,
   string   error_description
);

int  Pipvane_SendCapabilities(
   int      supports_hedging,
   int      is_fifo_enforced,
   int      trade_allowed,
   int      dll_allowed,
   int      execution_mode,
   uint     leverage,
   double   min_lot,
   double   max_lot,
   double   lot_step,
   uint     stop_level_points,
   uint     freeze_level_points,
   string   account_currency
);

int  Pipvane_SendPositionsSync(
   ulong                 account_login,
   const ShmPositionItem &positions_array[],
   int                   position_count
);

int  Pipvane_VerifyFileSHA256(string file_path, string expected_hex_hash);

ulong Pipvane_GetLastTransitLatencyNs();
ulong Pipvane_GetProcessedIntentsCount();
ulong Pipvane_GetDroppedIntentsCount();
string Pipvane_GetLastError();

#import

// =============================================================================
// High-Level Helper Functions for Expert Advisors
// =============================================================================

// Helper: Converts MQL symbol string to uchar array in intent
void Pipvane_StringToUCharArray(string src, uchar &dst[], int max_len)
{
   StringToCharArray(src, dst, 0, max_len);
}

// Helper: Converts uchar array from intent to MQL symbol string
string Pipvane_UCharArrayToString(const uchar &src[])
{
   return CharArrayToString(src);
}

// Non-blocking poll for incoming OrderIntent
// Returns true if new intent retrieved, false if queue empty or corrupted
bool Pipvane_QuickPoll(ShmOrderIntent &intent)
{
   int res = Pipvane_PollOrderIntent(intent);
   return (res == 1);
}

// High-level account state reporter (e.g. called on OnTimer every 1s)
bool Pipvane_ReportCurrentAccount()
{
#ifdef __MQL5__
   ulong login = (ulong)AccountInfoInteger(ACCOUNT_LOGIN);
   double balance = AccountInfoDouble(ACCOUNT_BALANCE);
   double equity = AccountInfoDouble(ACCOUNT_EQUITY);
   double margin = AccountInfoDouble(ACCOUNT_MARGIN);
   double freeMargin = AccountInfoDouble(ACCOUNT_MARGIN_FREE);
   double marginLevel = AccountInfoDouble(ACCOUNT_MARGIN_LEVEL);
   int openCount = PositionsTotal();
   int pendingCount = OrdersTotal();
   int isConnected = TerminalInfoInteger(TERMINAL_CONNECTED) ? 1 : 0;
#else
   ulong login = (ulong)AccountNumber();
   double balance = AccountBalance();
   double equity = AccountEquity();
   double margin = AccountMargin();
   double freeMargin = AccountFreeMargin();
   double marginLevel = (margin > 0.0) ? (equity / margin * 100.0) : 0.0;
   int totalOrders = OrdersTotal();
   int openCount = 0;
   int pendingCount = 0;
   for (int i = 0; i < totalOrders; ++i) {
      if (OrderSelect(i, SELECT_BY_POS, MODE_TRADES)) {
         if (OrderType() <= 1) openCount++; // 0: OP_BUY, 1: OP_SELL
         else pendingCount++;
      }
   }
   int isConnected = IsConnected() ? 1 : 0;
#endif

   int res = Pipvane_SendAccountState(
      login, balance, equity, margin, freeMargin, marginLevel,
      openCount, pendingCount, isConnected
   );
   return (res == 0);
}

// High-level broker execution feedback reporter
bool Pipvane_ReportFeedback(
   ulong    signal_id,
   ulong    ticket,
   int      status,
   uint     retcode,
   double   req_vol,
   double   exec_vol,
   double   req_price,
   double   exec_price,
   double   slippage_pips,
   ulong    latency_us,
   string   error_desc
)
{
   int res = Pipvane_SendExecutionFeedback(
      signal_id, ticket, status, retcode,
      req_vol, exec_vol, req_price, exec_price,
      slippage_pips, latency_us, error_desc
   );
   return (res == 0);
}

// High-level positions synchronization reporter
bool Pipvane_ReportPositions()
{
#ifdef __MQL5__
   ulong login = (ulong)AccountInfoInteger(ACCOUNT_LOGIN);
   int total = PositionsTotal();
   if (total == 0) {
      ShmPositionItem emptyItems[];
      ArrayResize(emptyItems, 0);
      int res = Pipvane_SendPositionsSync(login, emptyItems, 0);
      return (res == 0);
   }

   ShmPositionItem items[];
   ArrayResize(items, total);

   for (int i = 0; i < total; ++i) {
      ulong ticket = PositionGetTicket(i);
      if (ticket > 0) {
         items[i].ticket = ticket;
         items[i].magic_number = (ulong)PositionGetInteger(POSITION_MAGIC);
         items[i].open_time_unix = (ulong)PositionGetInteger(POSITION_TIME);
         
         string sym = PositionGetString(POSITION_SYMBOL);
         Pipvane_StringToUCharArray(sym, items[i].symbol, 32);
         
         ENUM_POSITION_TYPE pType = (ENUM_POSITION_TYPE)PositionGetInteger(POSITION_TYPE);
         items[i].position_type = (pType == POSITION_TYPE_BUY) ? 0 : 1;
         
         items[i].volume = PositionGetDouble(POSITION_VOLUME);
         items[i].open_price = PositionGetDouble(POSITION_PRICE_OPEN);
         items[i].current_price = PositionGetDouble(POSITION_PRICE_CURRENT);
         items[i].stop_loss = PositionGetDouble(POSITION_SL);
         items[i].take_profit = PositionGetDouble(POSITION_TP);
         items[i].profit = PositionGetDouble(POSITION_PROFIT);
         items[i].swap = PositionGetDouble(POSITION_SWAP);
         items[i].commission = PositionGetDouble(POSITION_COMMISSION);
         
         string comment = PositionGetString(POSITION_COMMENT);
         Pipvane_StringToUCharArray(comment, items[i].comment, 32);
      }
   }

   int res = Pipvane_SendPositionsSync(login, items, total);
   return (res == 0);
#else
   ulong login = (ulong)AccountNumber();
   int totalOrders = OrdersTotal();
   int openCount = 0;
   for (int i = 0; i < totalOrders; ++i) {
      if (OrderSelect(i, SELECT_BY_POS, MODE_TRADES) && OrderType() <= 1) {
         openCount++;
      }
   }
   if (openCount == 0) {
      ShmPositionItem emptyItems[];
      ArrayResize(emptyItems, 0);
      int res = Pipvane_SendPositionsSync(login, emptyItems, 0);
      return (res == 0);
   }

   ShmPositionItem items[];
   ArrayResize(items, openCount);
   int idx = 0;
   for (int i = 0; i < totalOrders; ++i) {
      if (OrderSelect(i, SELECT_BY_POS, MODE_TRADES) && OrderType() <= 1) {
         items[idx].ticket = (ulong)OrderTicket();
         items[idx].magic_number = (ulong)OrderMagicNumber();
         items[idx].open_time_unix = (ulong)OrderOpenTime();
         
         string sym = OrderSymbol();
         Pipvane_StringToUCharArray(sym, items[idx].symbol, 32);
         
         items[idx].position_type = (OrderType() == 0) ? 0 : 1; // 0: OP_BUY, 1: OP_SELL
         items[idx].volume = OrderLots();
         items[idx].open_price = OrderOpenPrice();
         items[idx].current_price = (OrderType() == 0) ? MarketInfo(sym, MODE_BID) : MarketInfo(sym, MODE_ASK);
         items[idx].stop_loss = OrderStopLoss();
         items[idx].take_profit = OrderTakeProfit();
         items[idx].profit = OrderProfit();
         items[idx].swap = OrderSwap();
         items[idx].commission = OrderCommission();
         
         string comment = OrderComment();
         Pipvane_StringToUCharArray(comment, items[idx].comment, 32);
         idx++;
      }
   }

   int res = Pipvane_SendPositionsSync(login, items, openCount);
   return (res == 0);
#endif
}
