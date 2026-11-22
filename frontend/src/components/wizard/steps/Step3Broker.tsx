import React from 'react';
import { useApp } from '@/context/AppContext';
import { useTranslation } from '@/context/I18nContext';
import { Badge } from '@/components/ui/Badge';
import { Toggle } from '@/components/ui/Toggle';
import { Button } from '@/components/ui/Button';
import { Monitor, RefreshCw, FolderSearch, Cpu } from 'lucide-react';

export const Step3Broker: React.FC = () => {
  const { wizardState, updateBroker } = useApp();
  const { t } = useTranslation();
  const { broker } = wizardState;

  const handleSelectTerminal = (id: string) => {
    updateBroker({ selectedTerminalId: id });
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between p-4 rounded-xl bg-[#161F30]/70 border border-slate-800">
        <div>
          <h4 className="text-sm font-semibold text-white">{t('step3.title')}</h4>
          <p className="text-xs text-slate-400 mt-0.5">
            {t('step3.desc')}
          </p>
        </div>
        <div className="flex items-center gap-3">
          <Toggle
            checked={broker.autoDetectActive}
            onChange={(val) => updateBroker({ autoDetectActive: val })}
            label={t('step3.autoDetect')}
          />
          <Button
            size="sm"
            variant="outline"
            icon={<RefreshCw className="w-3.5 h-3.5" />}
          >
            {t('step3.rescan')}
          </Button>
        </div>
      </div>

      {/* Terminal Cards */}
      <div className="space-y-3">
        <label className="block text-xs font-semibold uppercase tracking-wider text-slate-400">
          {t('step3.availableTerminals')}
        </label>

        {broker.terminals.map((term) => {
          const isSelected = broker.selectedTerminalId === term.id;
          return (
            <div
              key={term.id}
              data-testid={`terminal-card-${term.id}`}
              onClick={() => handleSelectTerminal(term.id)}
              className={`p-4 rounded-xl border transition-all cursor-pointer ${
                isSelected
                  ? 'bg-[#1E293B] border-emerald-500 shadow-glow-emerald'
                  : 'bg-[#161F30]/70 border-slate-800 hover:border-slate-700'
              }`}
            >
              <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
                <div className="flex items-start gap-3">
                  <div
                    className={`p-2.5 rounded-lg shrink-0 mt-0.5 ${
                      isSelected ? 'bg-emerald-500/20 text-emerald-400' : 'bg-slate-800 text-slate-400'
                    }`}
                  >
                    <Monitor className="w-5 h-5" />
                  </div>
                  <div>
                    <div className="flex items-center gap-2">
                      <span className="text-sm font-semibold text-white">{term.name}</span>
                      <Badge variant={term.type === 'MT5' ? 'emerald' : term.type === 'MT4' ? 'cyan' : 'amber'}>
                        {term.type}
                      </Badge>
                      <span className="text-xs text-slate-500 font-mono">{term.version}</span>
                    </div>

                    <div className="flex flex-wrap items-center gap-x-4 gap-y-1 mt-1.5 text-xs text-slate-400 font-mono">
                      <span>Acc: <strong className="text-slate-200">{term.accountNumber}</strong></span>
                      <span>Server: <strong className="text-slate-200">{term.server}</strong></span>
                      <span className="text-slate-500 truncate max-w-xs">{term.path}</span>
                    </div>
                  </div>
                </div>

                <div className="flex sm:flex-col items-center sm:items-end justify-between gap-2 border-t sm:border-t-0 pt-2 sm:pt-0 border-slate-800">
                  <Badge variant={term.status === 'connected' ? 'emerald' : 'slate'} dot>
                    {term.status === 'connected' ? t('step3.ipcPipeReady') : t('step3.detected')}
                  </Badge>
                  <div className="flex items-center gap-1.5 text-xs font-mono">
                    <span className="text-slate-500">{t('step3.latency')}</span>
                    <span className="text-emerald-400 font-bold">{term.latencyMs}ms</span>
                  </div>
                </div>
              </div>
            </div>
          );
        })}
      </div>

      {/* IPC Bridge Status Info */}
      <div className="p-4 rounded-xl bg-[#111827] border border-slate-800 text-xs text-slate-400 flex items-center justify-between">
        <div className="flex items-center gap-2.5">
          <Cpu className="w-4 h-4 text-cyan-400" />
          <span>{t('step3.bridgeInfo')}</span>
        </div>
        <Button size="sm" variant="ghost" icon={<FolderSearch className="w-3.5 h-3.5" />}>
          {t('step3.manualPath')}
        </Button>
      </div>
    </div>
  );
};
