import React from 'react';
import { useApp } from '@/context/AppContext';
import { Badge } from '@/components/ui/Badge';
import { Button } from '@/components/ui/Button';
import { Radio, Shield, CheckCircle2, RefreshCw } from 'lucide-react';

export const ChannelsView: React.FC = () => {
  const { wizardState } = useApp();
  const { telegram } = wizardState;

  return (
    <div className="space-y-6">
      <div className="flex flex-col sm:flex-row sm:items-center justify-between pb-4 border-b border-slate-800 gap-4">
        <div>
          <h2 className="text-lg font-bold text-white">Monitored Signal Channels</h2>
          <p className="text-xs text-slate-400 mt-0.5">
            Real-time Telegram feeds monitored by the isolated TDLib listener with Safety Mode enabled.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Badge variant="blue" dot>
            Read-Only MTProto Session
          </Badge>
          <Button variant="outline" size="sm" icon={<RefreshCw className="w-3.5 h-3.5" />}>
            Sync Channels
          </Button>
        </div>
      </div>

      <div className="space-y-3">
        {telegram.channels.map((ch) => (
          <div
            key={ch.id}
            className="p-5 rounded-xl bg-[#111827] border border-slate-800 hover:border-slate-700 transition-all flex flex-col sm:flex-row sm:items-center justify-between gap-4"
          >
            <div className="flex items-start gap-3.5">
              <div className="p-2.5 rounded-lg bg-blue-500/10 text-blue-400 shrink-0 mt-0.5">
                <Radio className="w-5 h-5 text-emerald-400" />
              </div>
              <div>
                <div className="flex items-center gap-2">
                  <h4 className="text-sm font-semibold text-white">{ch.name}</h4>
                  <Badge variant="emerald" dot>
                    Active Feed
                  </Badge>
                </div>
                <p className="text-xs text-slate-400 mt-1">
                  {ch.subscribers} members • Natural Language Signal Parsing • Zero latency bridge
                </p>
              </div>
            </div>

            <div className="flex items-center gap-3 shrink-0">
              <Button size="sm" variant="outline">
                Signal Rules
              </Button>
              <Button size="sm" variant="secondary">
                View Logs
              </Button>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};
