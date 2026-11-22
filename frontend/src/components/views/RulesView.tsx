import React from 'react';
import { useApp } from '@/context/AppContext';
import { Badge } from '@/components/ui/Badge';
import { Button } from '@/components/ui/Button';
import { Input } from '@/components/ui/Input';
import { Toggle } from '@/components/ui/Toggle';
import { SlidersHorizontal, ShieldAlert, Save } from 'lucide-react';

export const RulesView: React.FC = () => {
  const { wizardState, updateRisk } = useApp();
  const { risk } = wizardState;

  return (
    <div className="space-y-6">
      <div className="flex flex-col sm:flex-row sm:items-center justify-between pb-4 border-b border-slate-800 gap-4">
        <div>
          <h2 className="text-lg font-bold text-white">Risk & Copier Rules</h2>
          <p className="text-xs text-slate-400 mt-0.5">
            Configure lot sizing algorithms, prop firm drawdown guards, and slippage thresholds.
          </p>
        </div>
        <Button variant="primary" icon={<Save className="w-4 h-4" />}>
          Save Rule Changes
        </Button>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Sizing Rules */}
        <div className="p-5 rounded-xl bg-[#111827] border border-slate-800 space-y-4">
          <h4 className="text-sm font-semibold text-white flex items-center gap-2">
            <SlidersHorizontal className="w-4 h-4 text-emerald-400" />
            Position Sizing Engine
          </h4>

          <div className="space-y-4 text-xs">
            <Input
              label="Default Equity Risk %"
              type="number"
              step="0.1"
              value={risk.equityPercent}
              onChange={(e) => updateRisk({ equityPercent: parseFloat(e.target.value) || 0 })}
              suffix="%"
              isMono
            />
            <Input
              label="Slippage Filter (Pips)"
              type="number"
              step="0.1"
              value={risk.slippageTolerancePips}
              onChange={(e) => updateRisk({ slippageTolerancePips: parseFloat(e.target.value) || 1.5 })}
              suffix="Pips"
              isMono
            />
          </div>
        </div>

        {/* Prop Firm Safeguards */}
        <div className="p-5 rounded-xl bg-[#111827] border border-slate-800 space-y-4">
          <h4 className="text-sm font-semibold text-white flex items-center gap-2">
            <ShieldAlert className="w-4 h-4 text-amber-400" />
            Prop Firm Equity Safeguards
          </h4>

          <div className="space-y-4 text-xs">
            <Input
              label="Max Daily Drawdown Guard"
              type="number"
              step="0.5"
              value={risk.maxDrawdownGuard}
              onChange={(e) => updateRisk({ maxDrawdownGuard: parseFloat(e.target.value) || 4.5 })}
              suffix="%"
              isMono
              helperText="Hard stop: terminates open orders if daily drawdown touches this percent."
            />

            <Input
              label="Margin Level Safety Floor"
              type="number"
              step="50"
              value={risk.marginSafetyThreshold}
              onChange={(e) => updateRisk({ marginSafetyThreshold: parseInt(e.target.value) || 200 })}
              suffix="%"
              isMono
            />
          </div>
        </div>
      </div>
    </div>
  );
};
