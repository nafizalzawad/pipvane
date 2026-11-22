import React, { useState } from 'react';
import { useApp } from '@/context/AppContext';
import { useTranslation } from '@/context/I18nContext';
import { Input } from '@/components/ui/Input';
import { Button } from '@/components/ui/Button';
import { Badge } from '@/components/ui/Badge';
import { KeyRound, ShieldCheck, CheckCircle2, Clock, Zap, ShieldAlert } from 'lucide-react';

export const Step1License: React.FC = () => {
  const { wizardState, validateLicenseKey } = useApp();
  const { t } = useTranslation();
  const { license } = wizardState;
  const [inputVal, setInputVal] = useState(license.key);
  const [isValidating, setIsValidating] = useState(false);

  const handleValidate = async () => {
    if (!inputVal.trim()) return;
    setIsValidating(true);
    try {
      await validateLicenseKey(inputVal.trim());
    } finally {
      setIsValidating(false);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      handleValidate();
    }
  };

  return (
    <div className="space-y-6">
      <div className="bg-[#161F30]/60 p-4 rounded-xl border border-slate-800">
        <div className="flex items-start gap-3">
          <div className="p-2 rounded-lg bg-emerald-500/10 text-emerald-400 shrink-0 mt-0.5">
            <ShieldCheck className="w-5 h-5" />
          </div>
          <div>
            <h4 className="text-sm font-semibold text-white">{t('step1.title')}</h4>
            <p className="text-xs text-slate-400 mt-1 leading-relaxed">
              {t('step1.desc')}
            </p>
          </div>
        </div>
      </div>

      <div className="space-y-4">
        <div>
          <label className="block text-xs font-semibold uppercase tracking-wider text-slate-400 mb-2">
            {t('step1.productKeyLabel')}
          </label>
          <div className="flex flex-col sm:flex-row gap-3">
            <div className="relative flex-1">
              <Input
                data-testid="license-input"
                value={inputVal}
                onChange={(e) => {
                  setInputVal(e.target.value);
                }}
                onKeyDown={handleKeyDown}
                placeholder={t('step1.inputPlaceholder')}
                prefixIcon={<KeyRound className="w-4 h-4" />}
                isMono
                error={license.status === 'invalid' ? license.errorMessage : undefined}
              />
            </div>
            <Button
              data-testid="verify-license-btn"
              type="button"
              variant="primary"
              onClick={handleValidate}
              disabled={isValidating || !inputVal.trim()}
              className="whitespace-nowrap"
            >
              {isValidating ? t('step1.validatingBtn') : t('step1.verifyBtn')}
            </Button>
          </div>
        </div>

        {/* Validation Failure State Banner */}
        {license.status === 'invalid' && license.errorMessage && (
          <div
            data-testid="license-error-banner"
            className="p-4 rounded-xl bg-rose-950/40 border border-rose-800/80 flex items-start gap-3 text-xs text-rose-300 animate-in fade-in"
          >
            <div className="p-1 rounded bg-rose-500/20 text-rose-400 shrink-0 mt-0.5">
              <ShieldAlert className="w-4 h-4" />
            </div>
            <div className="flex-1">
              <div className="flex items-center justify-between">
                <span className="font-bold text-rose-200">{t('step1.failureTitle')}</span>
                <Badge variant="red" dot>{t('common.rejected')}</Badge>
              </div>
              <p className="mt-1 text-rose-300/90 leading-relaxed">
                {license.errorMessage}
              </p>
              <div className="mt-2 text-[11px] text-rose-400/80">
                {t('step1.failureNotice')}
              </div>
            </div>
          </div>
        )}

        {/* Validation Success State Banner */}
        {license.status === 'valid' && (
          <div
            data-testid="license-success-banner"
            className="p-4 rounded-xl bg-emerald-950/40 border border-emerald-800/80 flex items-start justify-between gap-3 text-xs text-emerald-300 animate-in fade-in"
          >
            <div className="flex items-start gap-3">
              <div className="p-1 rounded bg-emerald-500/20 text-emerald-400 shrink-0 mt-0.5">
                <CheckCircle2 className="w-4 h-4" />
              </div>
              <div>
                <div className="font-bold text-white">
                  {t('step1.successTitle', { tier: license.tier })}
                </div>
                <div className="text-emerald-300/90 mt-0.5">
                  {t('step1.successDesc')}
                </div>
              </div>
            </div>
            <Badge variant="emerald" dot pulse>{t('common.activeLease')}</Badge>
          </div>
        )}
      </div>

      {/* Quick Test Presets Helper */}
      <div className="p-3 rounded-lg bg-[#111827] border border-slate-800/80 flex flex-wrap items-center justify-between gap-2 text-xs text-slate-400">
        <span className="font-mono text-[11px]">{t('step1.quickTokensLabel')}</span>
        <div className="flex items-center gap-2">
          <button
            type="button"
            data-testid="preset-invalid-key"
            onClick={() => {
              setInputVal('INVALID-EXP-TOKEN-000');
            }}
            className="px-2 py-1 rounded bg-rose-950/60 border border-rose-800/70 text-rose-300 hover:bg-rose-900/60 transition-colors font-mono text-[11px]"
          >
            {t('step1.simulateInvalidBtn')}
          </button>
          <button
            type="button"
            data-testid="preset-valid-key"
            onClick={() => {
              setInputVal('PV3-PRO-8921-X992-FL40');
            }}
            className="px-2 py-1 rounded bg-emerald-950/60 border border-emerald-800/70 text-emerald-300 hover:bg-emerald-900/60 transition-colors font-mono text-[11px]"
          >
            {t('step1.loadValidBtn')}
          </button>
        </div>
      </div>

      {/* License Specs Card */}
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-3 pt-2">
        <div className="p-3.5 rounded-lg bg-[#1E293B]/70 border border-slate-800">
          <div className="flex items-center gap-2 text-slate-400 text-xs mb-1">
            <Clock className="w-3.5 h-3.5 text-amber-400" />
            <span>{t('step1.offlineGraceCard')}</span>
          </div>
          <div className="text-base font-bold text-white font-mono">
            {t('common.daysRemaining', { days: license.daysRemaining })}
          </div>
          <div className="text-[10px] text-slate-500 mt-0.5">{t('step1.leaseValidOfflineSub')}</div>
        </div>

        <div className="p-3.5 rounded-lg bg-[#1E293B]/70 border border-slate-800">
          <div className="flex items-center gap-2 text-slate-400 text-xs mb-1">
            <Zap className="w-3.5 h-3.5 text-cyan-400" />
            <span>{t('step1.maxTerminalsCard')}</span>
          </div>
          <div className="text-base font-bold text-white font-mono">
            {t('common.accountsCount', { count: license.maxAccounts })}
          </div>
          <div className="text-[10px] text-slate-500 mt-0.5">{t('step1.terminalsSub')}</div>
        </div>

        <div className="p-3.5 rounded-lg bg-[#1E293B]/70 border border-slate-800">
          <div className="flex items-center gap-2 text-slate-400 text-xs mb-1">
            <ShieldCheck className="w-3.5 h-3.5 text-emerald-400" />
            <span>{t('step1.executionProtocolCard')}</span>
          </div>
          <div className="text-base font-bold text-emerald-400 font-mono">
            {t('step1.subMsRingIpc')}
          </div>
          <div className="text-[10px] text-slate-500 mt-0.5">{t('step1.shmSub')}</div>
        </div>
      </div>
    </div>
  );
};
