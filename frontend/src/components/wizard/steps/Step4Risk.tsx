import React from 'react';
import { useApp } from '@/context/AppContext';
import { useTranslation } from '@/context/I18nContext';
import { Input } from '@/components/ui/Input';
import { Badge } from '@/components/ui/Badge';
import { ShieldAlert, Sliders, ChevronDown, ChevronUp } from 'lucide-react';

export const Step4Risk: React.FC = () => {
  const { wizardState, updateRisk } = useApp();
  const { t } = useTranslation();
  const { risk } = wizardState;

  const sizingOptions = [
    {
      id: 'equity_percent',
      title: t('step4.optEquityTitle'),
      desc: t('step4.optEquityDesc'),
      badge: t('step4.recommendedBadge'),
    },
    {
      id: 'fixed_lot',
      title: t('step4.optFixedTitle'),
      desc: t('step4.optFixedDesc'),
      badge: t('step4.staticBadge'),
    },
    {
      id: 'multiplier',
      title: t('step4.optMultiplierTitle'),
      desc: t('step4.optMultiplierDesc'),
      badge: t('step4.proportionalBadge'),
    },
  ] as const;

  return (
    <div className="space-y-6">
      {/* Sizing Mode Selection */}
      <div className="space-y-3">
        <label className="block text-xs font-semibold uppercase tracking-wider text-slate-400">
          {t('step4.sizingTitle')}
        </label>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
          {sizingOptions.map((opt) => {
            const isSelected = risk.sizingMode === opt.id;
            return (
              <div
                key={opt.id}
                onClick={() => updateRisk({ sizingMode: opt.id })}
                className={`p-4 rounded-xl border cursor-pointer transition-all ${
                  isSelected
                    ? 'bg-[#1E293B] border-emerald-500 shadow-glow-emerald ring-1 ring-emerald-500/50'
                    : 'bg-[#161F30]/70 border-slate-800 hover:border-slate-700'
                }`}
              >
                <div className="flex items-center justify-between mb-2">
                  <Badge variant={isSelected ? 'emerald' : 'slate'}>{opt.badge}</Badge>
                  <div
                    className={`w-3.5 h-3.5 rounded-full border-2 ${
                      isSelected
                        ? 'border-emerald-400 bg-emerald-400'
                        : 'border-slate-600 bg-transparent'
                    }`}
                  />
                </div>
                <h5 className="text-sm font-semibold text-white">{opt.title}</h5>
                <p className="text-xs text-slate-400 mt-1 leading-relaxed">{opt.desc}</p>
              </div>
            );
          })}
        </div>
      </div>

      {/* Primary Parameters */}
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 p-4 rounded-xl bg-[#161F30]/70 border border-slate-800">
        {risk.sizingMode === 'equity_percent' && (
          <Input
            label={t('step4.riskPerTrade')}
            type="number"
            step="0.1"
            min="0.1"
            max="10"
            value={risk.equityPercent}
            onChange={(e) => updateRisk({ equityPercent: parseFloat(e.target.value) || 0 })}
            suffix="%"
            isMono
            helperText={t('step4.riskPerTradeHelper')}
          />
        )}

        {risk.sizingMode === 'fixed_lot' && (
          <Input
            label={t('step4.fixedLotVolume')}
            type="number"
            step="0.01"
            min="0.01"
            max="100"
            value={risk.fixedLot}
            onChange={(e) => updateRisk({ fixedLot: parseFloat(e.target.value) || 0.01 })}
            suffix="Lots"
            isMono
            helperText={t('step4.fixedLotHelper')}
          />
        )}

        {risk.sizingMode === 'multiplier' && (
          <Input
            label={t('step4.volumeMultiplier')}
            type="number"
            step="0.1"
            min="0.1"
            max="10"
            value={risk.multiplier}
            onChange={(e) => updateRisk({ multiplier: parseFloat(e.target.value) || 1 })}
            suffix="x"
            isMono
            helperText={t('step4.volumeMultiplierHelper')}
          />
        )}

        <Input
          label={t('step4.maxSlippage')}
          type="number"
          step="0.1"
          min="0.5"
          max="20"
          value={risk.slippageTolerancePips}
          onChange={(e) => updateRisk({ slippageTolerancePips: parseFloat(e.target.value) || 1.5 })}
          suffix="Pips"
          isMono
          helperText={t('step4.maxSlippageHelper')}
        />
      </div>

      {/* Max Drawdown Equity Guard */}
      <div className="p-4 rounded-xl bg-amber-950/20 border border-amber-900/40">
        <div className="flex items-start gap-3">
          <div className="p-2 rounded-lg bg-amber-500/10 text-amber-400 shrink-0 mt-0.5">
            <ShieldAlert className="w-5 h-5" />
          </div>
          <div className="flex-1">
            <div className="flex items-center justify-between">
              <h4 className="text-sm font-semibold text-amber-200">
                {t('step4.hardStopTitle')}
              </h4>
              <Badge variant="amber">{t('step4.propFirmSafe')}</Badge>
            </div>
            <p className="text-xs text-amber-300/80 mt-1 leading-relaxed">
              {t('step4.hardStopDesc')}
            </p>
            <div className="mt-3 max-w-xs">
              <Input
                type="number"
                step="0.5"
                min="1"
                max="20"
                value={risk.maxDrawdownGuard}
                onChange={(e) => updateRisk({ maxDrawdownGuard: parseFloat(e.target.value) || 4.5 })}
                suffix="%"
                isMono
              />
            </div>
          </div>
        </div>
      </div>

      {/* Progressive Disclosure: Advanced Settings */}
      <div className="border border-slate-800 rounded-xl overflow-hidden bg-[#111827]">
        <button
          type="button"
          onClick={() => updateRisk({ advancedRiskMath: !risk.advancedRiskMath })}
          className="w-full px-4 py-3 flex items-center justify-between ltr:text-left rtl:text-right text-xs font-semibold uppercase tracking-wider text-slate-300 hover:bg-slate-800/50 transition-colors"
        >
          <div className="flex items-center gap-2">
            <Sliders className="w-4 h-4 text-cyan-400" />
            <span>{t('step4.advancedTitle')}</span>
          </div>
          <div className="flex items-center gap-1.5 text-slate-400">
            <span>{risk.advancedRiskMath ? t('step4.hideAdvanced') : t('step4.showAdvanced')}</span>
            {risk.advancedRiskMath ? (
              <ChevronUp className="w-4 h-4" />
            ) : (
              <ChevronDown className="w-4 h-4" />
            )}
          </div>
        </button>

        {risk.advancedRiskMath && (
          <div className="p-4 border-t border-slate-800/80 bg-[#161F30]/50 space-y-4 animate-in fade-in">
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
              <Input
                label={t('step4.maxOpenPositions')}
                type="number"
                min="1"
                max="50"
                value={risk.maxOpenTrades}
                onChange={(e) => updateRisk({ maxOpenTrades: parseInt(e.target.value) || 5 })}
                suffix="Trades"
                isMono
                helperText={t('step4.maxOpenPositionsHelper')}
              />

              <Input
                label={t('step4.marginSafetyCutoff')}
                type="number"
                min="100"
                max="1000"
                step="50"
                value={risk.marginSafetyThreshold}
                onChange={(e) => updateRisk({ marginSafetyThreshold: parseInt(e.target.value) || 200 })}
                suffix="%"
                isMono
                helperText={t('step4.marginSafetyHelper')}
              />

              <Input
                label={t('step4.dailyLossLimit')}
                type="number"
                min="50"
                max="50000"
                step="50"
                value={risk.dailyLossLimitUsd}
                onChange={(e) => updateRisk({ dailyLossLimitUsd: parseInt(e.target.value) || 500 })}
                suffix="USD"
                isMono
                helperText={t('step4.dailyLossLimitHelper')}
              />
            </div>
          </div>
        )}
      </div>
    </div>
  );
};
