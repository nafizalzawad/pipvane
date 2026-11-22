import React from 'react';
import { useApp } from '@/context/AppContext';
import { useTranslation } from '@/context/I18nContext';
import { Badge } from '@/components/ui/Badge';
import { Button } from '@/components/ui/Button';
import { Wallet, ArrowUpRight } from 'lucide-react';

export const AccountSummaryCard: React.FC = () => {
  const { activeAccountId, wizardState, setIsSetupDoctorOpen } = useApp();
  const { t } = useTranslation();
  const { broker } = wizardState;

  const currentTerm =
    broker.terminals.find((t) => t.id === activeAccountId) || broker.terminals[0];

  return (
    <div className="bg-[#111827] border border-slate-800 rounded-xl p-5 shadow-sm">
      <div className="flex flex-col sm:flex-row sm:items-center justify-between pb-4 mb-4 border-b border-slate-800 gap-3">
        <div className="flex items-center gap-3">
          <div className="p-2.5 rounded-lg bg-emerald-500/10 text-emerald-400">
            <Wallet className="w-5 h-5" />
          </div>
          <div>
            <div className="flex items-center gap-2">
              <h3 className="text-sm font-semibold text-white">
                {t('common.activeAccount')} {currentTerm?.name}
              </h3>
              <Badge variant="emerald" dot>
                Live Sync
              </Badge>
            </div>
            <p className="text-xs text-slate-400 font-mono mt-0.5">
              Account #{currentTerm?.accountNumber} • {currentTerm?.server}
            </p>
          </div>
        </div>

        <div className="flex items-center gap-2">
          <Badge variant="cyan" dot>
            IPC Latency: {currentTerm?.latencyMs}ms
          </Badge>
          <Button
            size="sm"
            variant="outline"
            onClick={() => setIsSetupDoctorOpen(true)}
          >
            Terminal Config
          </Button>
        </div>
      </div>

      {/* Account Balances Grid */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
        <div className="p-3 rounded-lg bg-[#161F30]/70 border border-slate-800/80">
          <div className="text-[11px] text-slate-400 font-medium">{t('dashboard.accountBalance')}</div>
          <div className="text-lg font-bold font-mono text-white mt-1">$50,000.00</div>
          <div className="text-[10px] text-slate-500 mt-0.5">{t('dashboard.demoCapital')}</div>
        </div>

        <div className="p-3 rounded-lg bg-[#161F30]/70 border border-slate-800/80">
          <div className="text-[11px] text-slate-400 font-medium">{t('dashboard.floatingEquity')}</div>
          <div className="text-lg font-bold font-mono text-emerald-400 mt-1">$50,392.30</div>
          <div className="text-[10px] text-emerald-400/80 mt-0.5 flex items-center gap-0.5">
            <ArrowUpRight className="w-3 h-3" />
            +$392.30 (+0.78%)
          </div>
        </div>

        <div className="p-3 rounded-lg bg-[#161F30]/70 border border-slate-800/80">
          <div className="text-[11px] text-slate-400 font-medium">{t('dashboard.marginLevel')}</div>
          <div className="text-lg font-bold font-mono text-cyan-400 mt-1">2,840%</div>
          <div className="text-[10px] text-slate-500 mt-0.5">{t('dashboard.marginSafetyThreshold')}</div>
        </div>

        <div className="p-3 rounded-lg bg-[#161F30]/70 border border-slate-800/80">
          <div className="text-[11px] text-slate-400 font-medium">{t('dashboard.openPositions')}</div>
          <div className="text-lg font-bold font-mono text-white mt-1">
            {t('common.tradesCount', { count: 3 })}
          </div>
          <div className="text-[10px] text-slate-500 mt-0.5">XAUUSD, EURUSD, NAS100</div>
        </div>
      </div>
    </div>
  );
};
