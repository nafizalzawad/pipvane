import React from 'react';
import { useApp } from '@/context/AppContext';
import { useTranslation } from '@/context/I18nContext';
import { Badge } from '@/components/ui/Badge';
import { ArrowUpRight, ArrowDownRight, Clock } from 'lucide-react';

export const RecentActivityTable: React.FC = () => {
  const { recentTrades } = useApp();
  const { t } = useTranslation();

  return (
    <div className="bg-[#111827] border border-slate-800 rounded-xl overflow-hidden shadow-sm">
      <div className="px-5 py-4 border-b border-slate-800/80 flex items-center justify-between">
        <div className="flex items-center gap-2.5">
          <Clock className="w-4 h-4 text-emerald-400" />
          <h3 className="text-sm font-semibold text-white">{t('dashboard.liveExecutionFeed')}</h3>
        </div>
        <Badge variant="emerald" dot pulse>
          {t('dashboard.subMsDispatchActive')}
        </Badge>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full ltr:text-left rtl:text-right border-collapse">
          <thead>
            <tr className="border-b border-slate-800/80 text-[11px] font-semibold uppercase tracking-wider text-slate-400 bg-[#161F30]/40">
              <th className="py-3 px-4">{t('dashboard.thTicket')}</th>
              <th className="py-3 px-4">{t('dashboard.thTime')}</th>
              <th className="py-3 px-4">{t('dashboard.thSymbol')}</th>
              <th className="py-3 px-4">{t('dashboard.thType')}</th>
              <th className="py-3 px-4">{t('dashboard.thLots')}</th>
              <th className="py-3 px-4">{t('dashboard.thChannel')}</th>
              <th className="py-3 px-4">{t('dashboard.thBroker')}</th>
              <th className="py-3 px-4">{t('dashboard.thLatency')}</th>
              <th className="py-3 px-4">{t('dashboard.thStatus')}</th>
              <th className="py-3 px-4 ltr:text-right rtl:text-left">{t('dashboard.thPnl')}</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800/50 text-xs">
            {recentTrades.map((trade) => {
              const isBuy = trade.type === 'BUY';
              return (
                <tr
                  key={trade.id}
                  className="hover:bg-slate-800/30 transition-colors font-medium"
                >
                  <td className="py-3 px-4 font-mono text-slate-300">
                    {trade.id}
                  </td>
                  <td className="py-3 px-4 text-slate-400 text-[11px] whitespace-nowrap">
                    {trade.time}
                  </td>
                  <td className="py-3 px-4 font-mono font-bold text-white">
                    {trade.symbol}
                  </td>
                  <td className="py-3 px-4">
                    <span
                      className={`inline-flex items-center gap-1 font-mono font-bold text-xs px-2 py-0.5 rounded ${
                        isBuy
                          ? 'bg-emerald-950/70 text-emerald-400 border border-emerald-800/80'
                          : 'bg-rose-950/70 text-rose-400 border border-rose-800/80'
                      }`}
                    >
                      {isBuy ? (
                        <ArrowUpRight className="w-3.5 h-3.5" />
                      ) : (
                        <ArrowDownRight className="w-3.5 h-3.5" />
                      )}
                      {trade.type}
                    </span>
                  </td>
                  <td className="py-3 px-4 font-mono text-slate-200">
                    {trade.lots.toFixed(2)}
                  </td>
                  <td className="py-3 px-4 text-slate-300 max-w-xs truncate">
                    {trade.channel}
                  </td>
                  <td className="py-3 px-4 text-slate-400 text-[11px]">
                    {trade.broker}
                  </td>
                  <td className="py-3 px-4 font-mono text-emerald-400 font-semibold">
                    {trade.latency}
                  </td>
                  <td className="py-3 px-4">
                    <Badge variant={trade.status === 'FILLED' ? 'emerald' : 'amber'} dot>
                      {trade.status}
                    </Badge>
                  </td>
                  <td
                    className={`py-3 px-4 ltr:text-right rtl:text-left font-mono font-bold ${
                      trade.profit > 0
                        ? 'text-emerald-400'
                        : trade.profit < 0
                        ? 'text-rose-400'
                        : 'text-slate-400'
                    }`}
                  >
                    {trade.profit > 0
                      ? `+$${trade.profit.toFixed(2)}`
                      : trade.profit < 0
                      ? `-$${Math.abs(trade.profit).toFixed(2)}`
                      : '$0.00'}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
};
