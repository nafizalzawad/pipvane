import React from 'react';
import { useApp } from '@/context/AppContext';
import { useTranslation } from '@/context/I18nContext';
import { Radio, Server, Zap, ShieldCheck } from 'lucide-react';

export const MetricCards: React.FC = () => {
  const { wizardState, ipcLatencyMs } = useApp();
  const { t } = useTranslation();
  const { broker, telegram, risk } = wizardState;

  const connectedTerminals = broker.terminals.filter((t) => t.status === 'connected').length;
  const activeChannels = telegram.channels.filter((c) => c.status === 'active').length;

  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
      {/* IPC Latency Metric */}
      <div className="p-4 rounded-xl bg-[#111827] border border-slate-800 hover:border-emerald-500/40 transition-all">
        <div className="flex items-center justify-between text-xs text-slate-400 mb-2">
          <span className="font-medium">{t('dashboard.metricSubMsLatency')}</span>
          <div className="p-1.5 rounded-lg bg-emerald-500/10 text-emerald-400">
            <Zap className="w-4 h-4" />
          </div>
        </div>
        <div className="text-2xl font-bold font-mono text-emerald-400 tabular-nums">
          {ipcLatencyMs.toFixed(2)} <span className="text-sm font-normal text-slate-400">ms</span>
        </div>
        <div className="flex items-center gap-1.5 mt-2 text-[11px] text-emerald-400">
          <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse-dot" />
          <span>{t('dashboard.metricShmRingOk')}</span>
        </div>
      </div>

      {/* Connected Accounts */}
      <div className="p-4 rounded-xl bg-[#111827] border border-slate-800 hover:border-cyan-500/40 transition-all">
        <div className="flex items-center justify-between text-xs text-slate-400 mb-2">
          <span className="font-medium">{t('dashboard.metricConnectedTerminals')}</span>
          <div className="p-1.5 rounded-lg bg-cyan-500/10 text-cyan-400">
            <Server className="w-4 h-4" />
          </div>
        </div>
        <div className="text-2xl font-bold font-mono text-white">
          {connectedTerminals} <span className="text-sm font-normal text-slate-400">/ {broker.terminals.length}</span>
        </div>
        <div className="mt-2 text-[11px] text-slate-400">
          MT5 Demo (0.8ms) • MT4 Live (1.1ms)
        </div>
      </div>

      {/* Active Telegram Channels */}
      <div className="p-4 rounded-xl bg-[#111827] border border-slate-800 hover:border-blue-500/40 transition-all">
        <div className="flex items-center justify-between text-xs text-slate-400 mb-2">
          <span className="font-medium">{t('dashboard.metricSignalChannels')}</span>
          <div className="p-1.5 rounded-lg bg-blue-500/10 text-blue-400">
            <Radio className="w-4 h-4" />
          </div>
        </div>
        <div className="text-2xl font-bold font-mono text-white">
          {activeChannels} <span className="text-sm font-normal text-slate-400">{t('dashboard.metricLiveChannels')}</span>
        </div>
        <div className="mt-2 text-[11px] text-blue-400 flex items-center gap-1">
          <ShieldCheck className="w-3.5 h-3.5" />
          <span>{t('dashboard.metricSafetyModeDesc')}</span>
        </div>
      </div>

      {/* Equity Guard Protection */}
      <div className="p-4 rounded-xl bg-[#111827] border border-slate-800 hover:border-amber-500/40 transition-all">
        <div className="flex items-center justify-between text-xs text-slate-400 mb-2">
          <span className="font-medium">{t('dashboard.metricEquityGuard')}</span>
          <div className="p-1.5 rounded-lg bg-amber-500/10 text-amber-400">
            <ShieldCheck className="w-4 h-4" />
          </div>
        </div>
        <div className="text-2xl font-bold font-mono text-amber-400">
          {risk.maxDrawdownGuard}% <span className="text-sm font-normal text-slate-400">{t('dashboard.metricCeiling')}</span>
        </div>
        <div className="mt-2 text-[11px] text-slate-400">
          {t('dashboard.metricSizing')} <strong className="text-slate-200 capitalize">{risk.sizingMode.replace('_', ' ')}</strong>
        </div>
      </div>
    </div>
  );
};
