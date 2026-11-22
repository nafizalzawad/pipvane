import React from 'react';
import { useApp } from '@/context/AppContext';
import { Badge } from '@/components/ui/Badge';
import { Button } from '@/components/ui/Button';
import { Server, Monitor, Plus, ShieldCheck, CheckCircle2 } from 'lucide-react';

export const AccountsView: React.FC = () => {
  const { wizardState, activeAccountId, setActiveAccountId, setIsSetupDoctorOpen } = useApp();
  const { broker } = wizardState;

  return (
    <div className="space-y-6">
      <div className="flex flex-col sm:flex-row sm:items-center justify-between pb-4 border-b border-slate-800 gap-4">
        <div>
          <h2 className="text-lg font-bold text-white">Configured Terminals & Broker Accounts</h2>
          <p className="text-xs text-slate-400 mt-0.5">
            Manage your MetaTrader 4, MetaTrader 5, and cTrader instances linked via the local IPC pipe.
          </p>
        </div>
        <Button
          variant="primary"
          icon={<Plus className="w-4 h-4" />}
          onClick={() => setIsSetupDoctorOpen(true)}
        >
          Add New Terminal
        </Button>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {broker.terminals.map((term) => {
          const isActive = activeAccountId === term.id;
          return (
            <div
              key={term.id}
              className={`p-5 rounded-xl border transition-all ${
                isActive
                  ? 'bg-[#1E293B] border-emerald-500 shadow-glow-emerald'
                  : 'bg-[#111827] border-slate-800 hover:border-slate-700'
              }`}
            >
              <div className="flex items-start justify-between">
                <div className="flex items-center gap-2.5">
                  <div className="p-2 rounded-lg bg-slate-800 text-emerald-400">
                    <Monitor className="w-5 h-5" />
                  </div>
                  <div>
                    <h4 className="text-sm font-semibold text-white">{term.name}</h4>
                    <span className="text-xs font-mono text-slate-400">{term.version}</span>
                  </div>
                </div>
                <Badge variant={term.status === 'connected' ? 'emerald' : 'slate'} dot>
                  {term.status}
                </Badge>
              </div>

              <div className="mt-4 pt-3 border-t border-slate-800/80 space-y-2 text-xs font-mono">
                <div className="flex justify-between text-slate-400">
                  <span>Account Number:</span>
                  <span className="text-slate-100 font-bold">{term.accountNumber}</span>
                </div>
                <div className="flex justify-between text-slate-400">
                  <span>Broker Server:</span>
                  <span className="text-slate-200">{term.server}</span>
                </div>
                <div className="flex justify-between text-slate-400">
                  <span>IPC Latency:</span>
                  <span className="text-emerald-400 font-bold">{term.latencyMs} ms</span>
                </div>
              </div>

              <div className="mt-4 pt-3 border-t border-slate-800/80 flex items-center justify-between">
                <Button
                  size="sm"
                  variant={isActive ? 'secondary' : 'outline'}
                  onClick={() => setActiveAccountId(term.id)}
                  className="w-full"
                >
                  {isActive ? 'Active Primary Account' : 'Switch to This Account'}
                </Button>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
};
