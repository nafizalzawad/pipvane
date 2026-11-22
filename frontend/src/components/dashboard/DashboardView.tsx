import React from 'react';
import { useApp } from '@/context/AppContext';
import { useTranslation } from '@/context/I18nContext';
import { MetricCards } from './MetricCards';
import { AccountSummaryCard } from './AccountSummaryCard';
import { RecentActivityTable } from './RecentActivityTable';
import { Button } from '@/components/ui/Button';
import { Badge } from '@/components/ui/Badge';
import { Stethoscope, Sparkles, ArrowRight, ArrowLeft } from 'lucide-react';

export const DashboardView: React.FC = () => {
  const { setIsSetupDoctorOpen } = useApp();
  const { t, isRtl } = useTranslation();

  const ActionArrow = isRtl ? ArrowLeft : ArrowRight;

  return (
    <div className="space-y-6">
      {/* Setup Doctor Welcome / Banner */}
      <div className="p-5 rounded-2xl bg-gradient-to-r from-[#161F30] via-[#111827] to-[#1E293B] border border-slate-800 shadow-md flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div className="flex items-start gap-3.5">
          <div className="p-2.5 rounded-xl bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 shrink-0 mt-0.5">
            <Stethoscope className="w-6 h-6" />
          </div>
          <div>
            <div className="flex items-center gap-2">
              <h2 className="text-base font-bold text-white tracking-tight">
                {t('dashboard.bannerTitle')}
              </h2>
              <Badge variant="emerald">{t('dashboard.bannerSubtitle')}</Badge>
            </div>
            <p className="text-xs text-slate-400 mt-1 max-w-2xl leading-relaxed">
              {t('dashboard.bannerDesc')}
            </p>
          </div>
        </div>

        <Button
          data-testid="banner-launch-doctor-btn"
          variant="primary"
          onClick={() => setIsSetupDoctorOpen(true)}
          icon={<Sparkles className="w-4 h-4" />}
          className="whitespace-nowrap shrink-0"
        >
          <span>{t('dashboard.launchDoctor')}</span>
          <ActionArrow className="w-4 h-4" />
        </Button>
      </div>

      {/* Top 4 Metrics */}
      <MetricCards />

      {/* Active Account Overview */}
      <AccountSummaryCard />

      {/* Real-time Copier Trade Log */}
      <RecentActivityTable />
    </div>
  );
};
