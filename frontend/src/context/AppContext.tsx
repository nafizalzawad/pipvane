'use client';

import React, { createContext, useContext, useState, useEffect, useCallback } from 'react';
import {
  TabType,
  SetupDoctorState,
  LicenseState,
  TelegramState,
  BrokerState,
  RiskProfileState,
  DemoTestState,
  TradeMetric,
} from '../types';
import { apiClient, LicenseValidationResponse } from '../lib/apiClient';
import { engineWsService, TelemetryPayload } from '../services/websocketService';

interface AppContextType {
  // Navigation & View
  currentTab: TabType;
  setCurrentTab: (tab: TabType) => void;
  isSidebarCollapsed: boolean;
  setIsSidebarCollapsed: (collapsed: boolean) => void;
  isSetupDoctorOpen: boolean;
  setIsSetupDoctorOpen: (open: boolean) => void;

  // Active Context
  activeAccountId: string;
  setActiveAccountId: (id: string) => void;
  coordinatorStatus: 'active' | 'grace_period' | 'offline';
  cloudLeaseRemainingDays: number;
  ipcLatencyMs: number;
  engineOnline: boolean;
  sessionToken: string;
  setSessionToken: (token: string) => void;

  // Setup Doctor State & Setters
  wizardState: SetupDoctorState;
  setWizardStep: (step: number) => boolean;
  validateLicenseKey: (key: string) => Promise<LicenseValidationResponse>;
  updateLicense: (license: Partial<LicenseState>) => void;
  updateTelegram: (telegram: Partial<TelegramState>) => void;
  updateBroker: (broker: Partial<BrokerState>) => void;
  updateRisk: (risk: Partial<RiskProfileState>) => void;
  updateDemoTest: (demo: Partial<DemoTestState>) => void;
  runMockTradeTest: () => Promise<void>;
  resetWizard: () => void;
  isStepAllowed: (step: number) => boolean;

  // Live Metrics
  recentTrades: TradeMetric[];
}

const defaultLicense: LicenseState = {
  key: '', // Initially empty so user explicitly validates in Setup Doctor
  status: 'idle',
  tier: 'Pro Multi-Terminal',
  daysRemaining: 30,
  maxAccounts: 10,
  errorMessage: undefined,
};

const defaultTelegram: TelegramState = {
  sessionState: 'connected',
  phone: '+1 (555) 019-2834',
  safetyMode: true, // FR-4.1: Strictly Read-Only Mode
  channelsDetected: 3,
  channels: [
    { id: 'ch-1', name: 'Gold Scalpers VIP [Alpha]', subscribers: '14,200', status: 'active' },
    { id: 'ch-2', name: 'Forex Institutional Flow', subscribers: '8,450', status: 'active' },
    { id: 'ch-3', name: 'Indices London & NY Breakout', subscribers: '6,120', status: 'active' },
  ],
};

const defaultBroker: BrokerState = {
  terminals: [
    {
      id: 'term-1',
      name: 'IC Markets MT5 Terminal (Demo)',
      type: 'MT5',
      version: 'Build 4450 x64',
      status: 'connected',
      path: 'C:\\Program Files\\ICMarkets MT5\\terminal64.exe',
      latencyMs: 0.84,
      accountNumber: '8492019',
      server: 'ICMarketsSC-Demo',
    },
    {
      id: 'term-2',
      name: 'FTMO Challenge MT4 (Live)',
      type: 'MT4',
      version: 'Build 1420',
      status: 'connected',
      path: 'C:\\Program Files\\FTMO MT4\\terminal.exe',
      latencyMs: 1.12,
      accountNumber: '3920184',
      server: 'FTMO-Server2',
    },
    {
      id: 'term-3',
      name: 'Spotware cTrader (Desktop)',
      type: 'cTrader',
      version: 'v4.8.12 Open API',
      status: 'detected',
      path: 'C:\\Users\\AppData\\Local\\Spotware\\cTrader.exe',
      latencyMs: 1.45,
      accountNumber: '5019283',
      server: 'Spotware-cTrader-Live',
    },
  ],
  selectedTerminalId: 'term-1',
  autoDetectActive: true,
  bridgeInstalled: true,
};

const defaultRisk: RiskProfileState = {
  sizingMode: 'equity_percent',
  fixedLot: 0.1,
  equityPercent: 1.0,
  multiplier: 1.0,
  maxDrawdownGuard: 4.5,
  slippageTolerancePips: 1.5,
  advancedRiskMath: false,
  maxOpenTrades: 5,
  marginSafetyThreshold: 250,
  dailyLossLimitUsd: 500,
};

const defaultDemoTest: DemoTestState = {
  step: 'idle',
  logs: [
    { timestamp: '12:00:01', level: 'info', message: 'Ready to perform end-to-end IPC latency test.' }
  ],
  latencyUs: 820,
  executionSpeedMs: 1.14,
};

const defaultRecentTrades: TradeMetric[] = [
  {
    id: 'TRD-88219',
    time: 'Just now',
    symbol: 'XAUUSD',
    type: 'BUY',
    lots: 0.5,
    channel: 'Gold Scalpers VIP [Alpha]',
    broker: 'IC Markets MT5',
    latency: '0.82ms',
    status: 'FILLED',
    profit: 142.5,
  },
  {
    id: 'TRD-88218',
    time: '2m ago',
    symbol: 'EURUSD',
    type: 'SELL',
    lots: 1.0,
    channel: 'Forex Institutional Flow',
    broker: 'FTMO MT4',
    latency: '1.05ms',
    status: 'FILLED',
    profit: 85.0,
  },
  {
    id: 'TRD-88217',
    time: '8m ago',
    symbol: 'NAS100',
    type: 'BUY',
    lots: 0.25,
    channel: 'Indices London & NY',
    broker: 'IC Markets MT5',
    latency: '0.78ms',
    status: 'FILLED',
    profit: -45.0,
  },
  {
    id: 'TRD-88216',
    time: '24m ago',
    symbol: 'GBPUSD',
    type: 'BUY',
    lots: 0.8,
    channel: 'Forex Institutional Flow',
    broker: 'FTMO MT4',
    latency: '1.20ms',
    status: 'FILLED',
    profit: 210.8,
  },
];

const AppContext = createContext<AppContextType | undefined>(undefined);

export const AppProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [currentTab, setCurrentTab] = useState<TabType>('dashboard');
  const [isSidebarCollapsed, setIsSidebarCollapsed] = useState(false);
  const [isSetupDoctorOpen, setIsSetupDoctorOpen] = useState(false);
  const [activeAccountId, setActiveAccountId] = useState('term-1');

  // Live WebSocket State
  const [coordinatorStatus, setCoordinatorStatus] = useState<'active' | 'grace_period' | 'offline'>('active');
  const [cloudLeaseRemainingDays, setCloudLeaseRemainingDays] = useState(30);
  const [ipcLatencyMs, setIpcLatencyMs] = useState(0.84);
  const [engineOnline, setEngineOnline] = useState(true);
  const [sessionToken, setSessionTokenState] = useState(apiClient.getSessionToken());

  const [wizardState, setWizardState] = useState<SetupDoctorState>({
    currentStep: 1,
    completedSteps: [],
    license: defaultLicense,
    telegram: defaultTelegram,
    broker: defaultBroker,
    risk: defaultRisk,
    demoTest: defaultDemoTest,
  });

  const [recentTrades, setRecentTrades] = useState<TradeMetric[]>(defaultRecentTrades);

  // Bind to Live WebSocket Telemetry Feed
  useEffect(() => {
    const unsubscribe = engineWsService.subscribe((telemetry: TelemetryPayload) => {
      setIpcLatencyMs(telemetry.ipcLatencyMs);
      setCoordinatorStatus(telemetry.cloudSyncStatus);
      setCloudLeaseRemainingDays(telemetry.cloudLeaseRemainingDays);
      setEngineOnline(telemetry.engineOnline);
    });

    return () => {
      unsubscribe();
    };
  }, []);

  const setSessionToken = (token: string) => {
    apiClient.setSessionToken(token);
    setSessionTokenState(token);
  };

  /**
   * Enforces Step 1 verification guard:
   * Users cannot proceed past Step 1 unless the license has been successfully validated.
   */
  const isStepAllowed = useCallback(
    (step: number): boolean => {
      if (step <= 1) return true;
      return wizardState.license.status === 'valid';
    },
    [wizardState.license.status]
  );

  const setWizardStep = (step: number): boolean => {
    if (step > 1 && !isStepAllowed(step)) {
      setWizardState((prev) => ({
        ...prev,
        license: {
          ...prev.license,
          status: 'invalid',
          errorMessage: 'Step 1 validation required: You must enter and verify a valid license key before proceeding.',
        },
      }));
      return false;
    }

    setWizardState((prev) => ({
      ...prev,
      currentStep: step,
      completedSteps:
        step > prev.currentStep && !prev.completedSteps.includes(prev.currentStep)
          ? [...prev.completedSteps, prev.currentStep]
          : prev.completedSteps,
    }));
    return true;
  };

  const validateLicenseKey = async (key: string): Promise<LicenseValidationResponse> => {
    setWizardState((prev) => ({
      ...prev,
      license: {
        ...prev.license,
        key,
        status: 'validating',
        errorMessage: undefined,
      },
    }));

    const result = await apiClient.validateLicense(key);

    if (result.success && result.valid) {
      setWizardState((prev) => ({
        ...prev,
        license: {
          key,
          status: 'valid',
          tier: result.tier || 'Pro Multi-Terminal',
          daysRemaining: result.daysRemaining || 30,
          maxAccounts: result.maxAccounts || 10,
          errorMessage: undefined,
        },
        completedSteps: prev.completedSteps.includes(1) ? prev.completedSteps : [...prev.completedSteps, 1],
      }));
    } else {
      setWizardState((prev) => ({
        ...prev,
        license: {
          ...prev.license,
          key,
          status: 'invalid',
          errorMessage: result.message || 'License validation failed. Please check your product key.',
        },
      }));
    }

    return result;
  };

  const updateLicense = (license: Partial<LicenseState>) => {
    setWizardState((prev) => ({
      ...prev,
      license: { ...prev.license, ...license },
    }));
  };

  const updateTelegram = (telegram: Partial<TelegramState>) => {
    setWizardState((prev) => ({
      ...prev,
      telegram: { ...prev.telegram, ...telegram },
    }));
  };

  const updateBroker = (broker: Partial<BrokerState>) => {
    setWizardState((prev) => ({
      ...prev,
      broker: { ...prev.broker, ...broker },
    }));
  };

  const updateRisk = (risk: Partial<RiskProfileState>) => {
    setWizardState((prev) => ({
      ...prev,
      risk: { ...prev.risk, ...risk },
    }));
  };

  const updateDemoTest = (demo: Partial<DemoTestState>) => {
    setWizardState((prev) => ({
      ...prev,
      demoTest: { ...prev.demoTest, ...demo },
    }));
  };

  const runMockTradeTest = async () => {
    updateDemoTest({
      step: 'ipc_ping',
      logs: [
        { timestamp: new Date().toLocaleTimeString(), level: 'info', message: 'Sending Shared-Memory Ring Buffer IPC ping...' },
      ],
    });

    await new Promise((r) => setTimeout(r, 600));

    updateDemoTest({
      step: 'signal_injection',
      latencyUs: 640,
      logs: [
        { timestamp: new Date().toLocaleTimeString(), level: 'success', message: 'IPC Ring Buffer round-trip confirmed in 640 µs (0.64 ms).' },
        { timestamp: new Date().toLocaleTimeString(), level: 'info', message: 'Injecting synthetic mock signal: BUY XAUUSD 0.01 lot [Magic: 881920]...' },
      ],
    });

    await new Promise((r) => setTimeout(r, 700));

    updateDemoTest({
      step: 'simulated_fill',
      logs: [
        { timestamp: new Date().toLocaleTimeString(), level: 'info', message: 'Passing signal to MT5 Bridge Connector...' },
        { timestamp: new Date().toLocaleTimeString(), level: 'info', message: 'Order check verified: Margin OK, Spread 1.1 pips, Slippage within limit.' },
      ],
    });

    await new Promise((r) => setTimeout(r, 800));

    const mockId = Math.floor(100000 + Math.random() * 900000);
    updateDemoTest({
      step: 'completed',
      ticketId: mockId,
      executionSpeedMs: 0.92,
      logs: [
        { timestamp: new Date().toLocaleTimeString(), level: 'success', message: `Execution SUCCESS: Order #${mockId} filled at 2642.80!` },
        { timestamp: new Date().toLocaleTimeString(), level: 'success', message: 'End-to-end pipeline ready for real-time live trading.' },
      ],
    });

    setRecentTrades((prev) => [
      {
        id: `DEMO-${mockId}`,
        time: 'Just now',
        symbol: 'XAUUSD',
        type: 'BUY',
        lots: 0.01,
        channel: 'Setup Doctor Demo Test',
        broker: 'IC Markets MT5',
        latency: '0.92ms',
        status: 'FILLED',
        profit: 0.0,
      },
      ...prev,
    ]);
  };

  const resetWizard = () => {
    setWizardState({
      currentStep: 1,
      completedSteps: [],
      license: defaultLicense,
      telegram: defaultTelegram,
      broker: defaultBroker,
      risk: defaultRisk,
      demoTest: defaultDemoTest,
    });
  };

  return (
    <AppContext.Provider
      value={{
        currentTab,
        setCurrentTab,
        isSidebarCollapsed,
        setIsSidebarCollapsed,
        isSetupDoctorOpen,
        setIsSetupDoctorOpen,
        activeAccountId,
        setActiveAccountId,
        coordinatorStatus,
        cloudLeaseRemainingDays,
        ipcLatencyMs,
        engineOnline,
        sessionToken,
        setSessionToken,
        wizardState,
        setWizardStep,
        validateLicenseKey,
        updateLicense,
        updateTelegram,
        updateBroker,
        updateRisk,
        updateDemoTest,
        runMockTradeTest,
        resetWizard,
        isStepAllowed,
        recentTrades,
      }}
    >
      {children}
    </AppContext.Provider>
  );
};

export const useApp = () => {
  const context = useContext(AppContext);
  if (!context) {
    throw new Error('useApp must be used within an AppProvider');
  }
  return context;
};
