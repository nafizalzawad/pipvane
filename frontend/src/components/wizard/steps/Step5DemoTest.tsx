import React, { useState } from 'react';
import { useApp } from '@/context/AppContext';
import { useTranslation } from '@/context/I18nContext';
import { Button } from '@/components/ui/Button';
import { Badge } from '@/components/ui/Badge';
import { Terminal, Play, ArrowRight, ArrowLeft, Activity, ShieldCheck, Zap } from 'lucide-react';

export const Step5DemoTest: React.FC = () => {
  const { wizardState, runMockTradeTest, setIsSetupDoctorOpen, setCurrentTab } = useApp();
  const { t, isRtl } = useTranslation();
  const { demoTest, broker } = wizardState;
  const [isRunning, setIsRunning] = useState(false);

  const handleRunTest = async () => {
    setIsRunning(true);
    await runMockTradeTest();
    setIsRunning(false);
  };

  const selectedBroker = broker.terminals.find((t) => t.id === broker.selectedTerminalId) || broker.terminals[0];
  const ActionArrow = isRtl ? ArrowLeft : ArrowRight;

  return (
    <div className="space-y-6">
      <div className="p-4 rounded-xl bg-[#161F30]/70 border border-slate-800">
        <div className="flex items-start justify-between gap-4">
          <div>
            <h4 className="text-sm font-semibold text-white">{t('step5.title')}</h4>
            <p className="text-xs text-slate-400 mt-1 leading-relaxed">
              {t('step5.desc', { broker: selectedBroker?.name || '' })}
            </p>
          </div>
          <Badge variant="cyan" dot pulse>{t('step5.ipcRingBadge')}</Badge>
        </div>
      </div>

      {/* Trigger Box */}
      <div className="p-5 rounded-xl bg-[#1E293B]/70 border border-slate-700/80 flex flex-col sm:flex-row items-center justify-between gap-4">
        <div className="flex items-center gap-3">
          <div className="p-3 rounded-xl bg-emerald-500/10 text-emerald-400">
            <Zap className="w-6 h-6" />
          </div>
          <div>
            <div className="text-sm font-semibold text-white">{t('step5.testSignalTitle')}</div>
            <div className="text-xs text-slate-400">
              {t('step5.testSignalDesc', { type: selectedBroker?.type || '', account: selectedBroker?.accountNumber || '' })}
            </div>
          </div>
        </div>

        <Button
          type="button"
          variant="primary"
          onClick={handleRunTest}
          disabled={isRunning}
          icon={<Play className={`w-4 h-4 ${isRunning ? 'animate-spin' : ''}`} />}
        >
          {isRunning ? t('step5.injectingSignal') : demoTest.step === 'completed' ? t('step5.reRunTestTrade') : t('step5.executeTestTrade')}
        </Button>
      </div>

      {/* Diagnostic Console Log */}
      <div className="space-y-2">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-wider text-slate-400">
            <Terminal className="w-4 h-4 text-emerald-400" />
            <span>{t('step5.logTitle')}</span>
          </div>
          {demoTest.step === 'completed' && (
            <div className="flex items-center gap-2 font-mono text-xs text-emerald-400">
              <span>{t('step5.roundTripLatency')}</span>
              <strong className="bg-emerald-950/70 border border-emerald-800 px-2 py-0.5 rounded">
                {demoTest.latencyUs} µs ({demoTest.executionSpeedMs} ms)
              </strong>
            </div>
          )}
        </div>

        <div className="bg-[#0B0F19] border border-slate-800 rounded-xl p-4 font-mono text-xs space-y-1.5 min-h-[140px] max-h-[180px] overflow-y-auto">
          {demoTest.logs.map((log, idx) => (
            <div key={idx} className="flex items-start gap-2.5">
              <span className="text-slate-500 shrink-0 font-tabular">[{log.timestamp}]</span>
              <span
                className={`${
                  log.level === 'success'
                    ? 'text-emerald-400 font-semibold'
                    : log.level === 'warning'
                    ? 'text-amber-400'
                    : log.level === 'error'
                    ? 'text-rose-400 font-semibold'
                    : 'text-slate-300'
                }`}
              >
                {log.level === 'success' && '✓ '}
                {log.message}
              </span>
            </div>
          ))}
          {isRunning && (
            <div className="flex items-center gap-2 text-cyan-400 animate-pulse pt-1">
              <Activity className="w-3.5 h-3.5" />
              <span>Streaming shared memory event frames...</span>
            </div>
          )}
        </div>
      </div>

      {/* Completion Banner */}
      {demoTest.step === 'completed' && (
        <div className="p-4 rounded-xl bg-emerald-950/40 border border-emerald-700/60 flex flex-col sm:flex-row items-center justify-between gap-4 animate-in fade-in">
          <div className="flex items-center gap-3">
            <div className="p-2 rounded-lg bg-emerald-500/20 text-emerald-400">
              <ShieldCheck className="w-6 h-6" />
            </div>
            <div>
              <h5 className="text-sm font-semibold text-white">{t('step5.successTitle')}</h5>
              <p className="text-xs text-emerald-300/80 mt-0.5">
                {t('step5.successDesc')}
              </p>
            </div>
          </div>
          <Button
            type="button"
            variant="primary"
            onClick={() => {
              setIsSetupDoctorOpen(false);
              setCurrentTab('dashboard');
            }}
            icon={<ActionArrow className="w-4 h-4" />}
          >
            {t('step5.launchCopier')}
          </Button>
        </div>
      )}
    </div>
  );
};
