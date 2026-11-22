export type TabType = 'dashboard' | 'accounts' | 'channels' | 'rules' | 'settings';

export interface LicenseState {
  key: string;
  status: 'idle' | 'validating' | 'valid' | 'grace_period' | 'invalid';
  tier: 'Starter' | 'Pro Multi-Terminal' | 'Enterprise';
  daysRemaining: number;
  maxAccounts: number;
  errorMessage?: string;
}

export interface TelegramState {
  sessionState: 'idle' | 'qr_ready' | 'authenticating' | 'connected' | 'error';
  phone: string;
  safetyMode: boolean; // FR-4.1: Read-only Telegram session
  channelsDetected: number;
  channels: { id: string; name: string; subscribers: string; status: 'active' | 'pending' }[];
}

export interface BrokerTerminal {
  id: string;
  name: string;
  type: 'MT4' | 'MT5' | 'cTrader';
  version: string;
  status: 'detected' | 'connected' | 'disconnected';
  path: string;
  latencyMs: number;
  accountNumber?: string;
  server?: string;
}

export interface BrokerState {
  terminals: BrokerTerminal[];
  selectedTerminalId: string;
  autoDetectActive: boolean;
  bridgeInstalled: boolean;
}

export interface RiskProfileState {
  sizingMode: 'fixed_lot' | 'equity_percent' | 'multiplier';
  fixedLot: number;
  equityPercent: number;
  multiplier: number;
  maxDrawdownGuard: number; // e.g. 4.5%
  slippageTolerancePips: number; // e.g. 1.5 pips
  // Advanced Progressive Disclosure
  advancedRiskMath: boolean;
  maxOpenTrades: number;
  marginSafetyThreshold: number; // e.g. 200%
  dailyLossLimitUsd: number;
}

export interface DemoTestLog {
  timestamp: string;
  level: 'info' | 'success' | 'warning' | 'error';
  message: string;
}

export interface DemoTestState {
  step: 'idle' | 'ipc_ping' | 'signal_injection' | 'simulated_fill' | 'completed' | 'failed';
  logs: DemoTestLog[];
  latencyUs: number; // microseconds
  ticketId?: number;
  executionSpeedMs: number;
}

export interface SetupDoctorState {
  currentStep: number;
  completedSteps: number[];
  license: LicenseState;
  telegram: TelegramState;
  broker: BrokerState;
  risk: RiskProfileState;
  demoTest: DemoTestState;
}

export interface TradeMetric {
  id: string;
  time: string;
  symbol: string;
  type: 'BUY' | 'SELL';
  lots: number;
  channel: string;
  broker: string;
  latency: string;
  status: 'FILLED' | 'PENDING' | 'REJECTED';
  profit: number;
}
