import React from 'react';
import { useApp } from '@/context/AppContext';
import { useTranslation } from '@/context/I18nContext';
import { Badge } from '@/components/ui/Badge';
import { Button } from '@/components/ui/Button';
import { LanguageSwitcher } from '@/components/ui/LanguageSwitcher';
import {
  Zap,
  ChevronDown,
  Stethoscope,
  Activity,
  Layers,
} from 'lucide-react';

export const TopBar: React.FC = () => {
  const {
    ipcLatencyMs,
    coordinatorStatus,
    cloudLeaseRemainingDays,
    engineOnline,
    activeAccountId,
    setActiveAccountId,
    setIsSetupDoctorOpen,
    wizardState,
  } = useApp();
  const { t, isRtl } = useTranslation();

  const { broker } = wizardState;

  return (
    <header className="h-16 bg-[#111827]/90 backdrop-blur border-b border-slate-800 px-4 sm:px-6 flex items-center justify-between sticky top-0 z-30">
      {/* Telemetry & Status Badges */}
      <div className="flex items-center gap-3 sm:gap-6 overflow-x-auto py-1">
        {/* Engine Status */}
        <div className="flex items-center gap-2">
          <Badge
            data-testid="engine-status-badge"
            variant={engineOnline ? 'emerald' : 'red'}
            dot
            pulse={engineOnline}
          >
            <span className="font-mono font-semibold">
              {engineOnline ? t('common.engineActive') : t('common.engineOffline')}
            </span>
          </Badge>
          <span className="text-[11px] text-slate-500 font-mono hidden md:inline">
            127.0.0.1:8765
          </span>
        </div>

        {/* Local IPC Latency */}
        <div className="flex items-center gap-2 ltr:border-l rtl:border-r border-slate-800 ltr:pl-3 rtl:pr-3 sm:ltr:pl-6 sm:rtl:pr-6">
          <div className="flex items-center gap-1.5 text-xs text-slate-400">
            <Zap className="w-3.5 h-3.5 text-emerald-400" />
            <span className="hidden lg:inline">{t('common.localIpcLatency')}</span>
          </div>
          <span
            data-testid="ipc-latency-metric"
            className="font-mono text-xs font-bold text-emerald-400 bg-emerald-950/60 border border-emerald-800/80 px-2 py-0.5 rounded tabular-nums"
          >
            {ipcLatencyMs.toFixed(2)} ms
          </span>
        </div>

        {/* Cloud Coordinator Sync Status */}
        <div className="flex items-center gap-2 ltr:border-l rtl:border-r border-slate-800 ltr:pl-3 rtl:pr-3 sm:ltr:pl-6 sm:rtl:pr-6">
          <div className="flex items-center gap-1.5 text-xs text-slate-400">
            <Activity className="w-3.5 h-3.5 text-cyan-400" />
            <span className="hidden xl:inline">{t('common.cloudCoordinatorSync')}</span>
          </div>
          <Badge
            data-testid="cloud-sync-badge"
            variant={coordinatorStatus === 'active' ? 'cyan' : coordinatorStatus === 'grace_period' ? 'amber' : 'red'}
            dot
          >
            <span>
              {coordinatorStatus === 'active'
                ? t('common.leaseValidOffline', { days: cloudLeaseRemainingDays })
                : t('common.offlineGraceRemaining', { days: cloudLeaseRemainingDays })}
            </span>
          </Badge>
        </div>
      </div>

      {/* Right side: Language Switcher, Account Switcher & Setup Doctor CTA */}
      <div className="flex items-center gap-2.5 sm:gap-3 shrink-0">
        {/* Language Switcher with RTL trigger */}
        <LanguageSwitcher />

        {/* Active Account Quick Switcher */}
        <div className="relative hidden md:block">
          <select
            data-testid="active-account-select"
            value={activeAccountId}
            onChange={(e) => setActiveAccountId(e.target.value)}
            className="appearance-none bg-[#1E293B] border border-slate-700 hover:border-slate-600 text-slate-200 text-xs font-mono font-medium rounded-lg ltr:pl-8 rtl:pr-8 ltr:pr-8 rtl:pl-8 py-2 cursor-pointer focus:outline-none focus:ring-2 focus:ring-emerald-500"
          >
            {broker.terminals.map((term) => (
              <option key={term.id} value={term.id}>
                [{term.type}] #{term.accountNumber} ({term.server})
              </option>
            ))}
          </select>
          <div className="absolute ltr:left-2.5 rtl:right-2.5 top-1/2 -translate-y-1/2 pointer-events-none text-emerald-400">
            <Layers className="w-3.5 h-3.5" />
          </div>
          <div className="absolute ltr:right-2.5 rtl:left-2.5 top-1/2 -translate-y-1/2 pointer-events-none text-slate-400">
            <ChevronDown className="w-3.5 h-3.5" />
          </div>
        </div>

        {/* Launch Setup Doctor Button */}
        <Button
          data-testid="launch-setup-doctor-btn"
          size="sm"
          variant="secondary"
          onClick={() => setIsSetupDoctorOpen(true)}
          icon={<Stethoscope className="w-4 h-4 text-emerald-400" />}
          className="border-emerald-500/30 hover:border-emerald-500/60"
        >
          <span className="font-medium">{t('common.setupDoctorBtn')}</span>
        </Button>
      </div>
    </header>
  );
};
