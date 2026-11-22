import React from 'react';
import { useApp } from '@/context/AppContext';
import { useTranslation } from '@/context/I18nContext';
import { TabType } from '@/types';
import {
  LayoutDashboard,
  Server,
  Radio,
  SlidersHorizontal,
  Settings,
  ChevronLeft,
  ChevronRight,
  ShieldCheck,
  Cpu,
} from 'lucide-react';

export const Sidebar: React.FC = () => {
  const {
    currentTab,
    setCurrentTab,
    isSidebarCollapsed,
    setIsSidebarCollapsed,
  } = useApp();
  const { t, isRtl } = useTranslation();

  const navItems = [
    { id: 'dashboard' as TabType, label: t('nav.dashboard'), icon: LayoutDashboard },
    { id: 'accounts' as TabType, label: t('nav.accounts'), icon: Server, badge: t('nav.liveBadge') },
    { id: 'channels' as TabType, label: t('nav.channels'), icon: Radio, badge: t('nav.activeBadge') },
    { id: 'rules' as TabType, label: t('nav.rules'), icon: SlidersHorizontal },
    { id: 'settings' as TabType, label: t('nav.settings'), icon: Settings },
  ];

  // In RTL, ChevronLeft means "forward/expand" and ChevronRight means "back/collapse"
  const CollapseIcon = isRtl
    ? isSidebarCollapsed ? ChevronLeft : ChevronRight
    : isSidebarCollapsed ? ChevronRight : ChevronLeft;

  return (
    <aside
      className={`h-screen bg-[#0E1424] ltr:border-r rtl:border-l border-slate-800 flex flex-col justify-between transition-all duration-300 z-40 select-none ${
        isSidebarCollapsed ? 'w-20' : 'w-64'
      }`}
    >
      {/* Top Header Branding */}
      <div>
        <div className="h-16 px-4 flex items-center justify-between border-b border-slate-800/80">
          {!isSidebarCollapsed ? (
            <div className="flex items-center gap-2.5">
              <div className="w-8 h-8 rounded-lg bg-emerald-500/10 border border-emerald-500/40 flex items-center justify-center text-emerald-400 font-mono font-bold text-base shadow-glow-emerald">
                P
              </div>
              <div>
                <div className="flex items-center gap-1.5">
                  <span className="font-bold text-white tracking-tight text-sm">
                    {t('common.appName')}
                  </span>
                  <span className="text-[10px] font-mono font-semibold px-1.5 py-0.2 rounded bg-emerald-950 text-emerald-400 border border-emerald-800">
                    {t('common.version')}
                  </span>
                </div>
                <div className="text-[10px] text-slate-500 tracking-wider uppercase font-semibold">
                  {t('common.agentSubtitle')}
                </div>
              </div>
            </div>
          ) : (
            <div className="mx-auto w-8 h-8 rounded-lg bg-emerald-500/10 border border-emerald-500/40 flex items-center justify-center text-emerald-400 font-mono font-bold text-base">
              P
            </div>
          )}

          <button
            onClick={() => setIsSidebarCollapsed(!isSidebarCollapsed)}
            className={`p-1.5 rounded-lg text-slate-400 hover:text-white hover:bg-slate-800/80 transition-colors ${
              isSidebarCollapsed ? 'mx-auto mt-2 hidden' : ''
            }`}
            title={isSidebarCollapsed ? 'Expand Sidebar' : 'Collapse Sidebar'}
          >
            <CollapseIcon className="w-4 h-4" />
          </button>
        </div>

        {/* Collapsed Toggle Button when collapsed */}
        {isSidebarCollapsed && (
          <div className="py-2 flex justify-center border-b border-slate-800/40">
            <button
              onClick={() => setIsSidebarCollapsed(false)}
              className="p-1.5 rounded-lg text-slate-400 hover:text-white hover:bg-slate-800/80 transition-colors"
              title="Expand Sidebar"
            >
              <CollapseIcon className="w-4 h-4" />
            </button>
          </div>
        )}

        {/* Navigation Items */}
        <nav className="p-3 space-y-1">
          {navItems.map((item) => {
            const isActive = currentTab === item.id;
            const Icon = item.icon;

            return (
              <button
                key={item.id}
                onClick={() => setCurrentTab(item.id)}
                className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-xl font-medium text-xs transition-all group ${
                  isActive
                    ? 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/30 shadow-sm'
                    : 'text-slate-400 hover:text-slate-100 hover:bg-slate-800/60'
                } ${isSidebarCollapsed ? 'justify-center px-0' : ''}`}
                title={isSidebarCollapsed ? item.label : undefined}
              >
                <Icon
                  className={`w-4 h-4 shrink-0 transition-colors ${
                    isActive ? 'text-emerald-400' : 'text-slate-400 group-hover:text-slate-200'
                  }`}
                />

                {!isSidebarCollapsed && (
                  <div className="flex-1 flex items-center justify-between ltr:text-left rtl:text-right">
                    <span>{item.label}</span>
                    {item.badge && (
                      <span className="text-[10px] font-mono px-1.5 py-0.5 rounded-md bg-slate-800 text-slate-400 border border-slate-700/60">
                        {item.badge}
                      </span>
                    )}
                  </div>
                )}
              </button>
            );
          })}
        </nav>
      </div>

      {/* Footer System Diagnostics */}
      <div className="p-3 border-t border-slate-800/80">
        {!isSidebarCollapsed ? (
          <div className="p-3 rounded-xl bg-[#111827] border border-slate-800/80 space-y-2">
            <div className="flex items-center justify-between text-[11px] text-slate-400">
              <span className="flex items-center gap-1.5">
                <Cpu className="w-3.5 h-3.5 text-cyan-400" />
                <span>{t('nav.agentFootprint')}</span>
              </span>
              <span className="font-mono text-emerald-400 text-[10px]">{t('nav.footprintOptimal')}</span>
            </div>

            <div className="space-y-1 font-mono text-[10px] text-slate-400">
              <div className="flex justify-between">
                <span>{t('nav.ramUsage')}</span>
                <span className="text-slate-200">38.4 MB</span>
              </div>
              <div className="flex justify-between">
                <span>{t('nav.cpuLoad')}</span>
                <span className="text-slate-200">0.18%</span>
              </div>
            </div>

            <div className="pt-1.5 border-t border-slate-800/80 flex items-center gap-1.5 text-[10px] text-slate-500">
              <ShieldCheck className="w-3 h-3 text-emerald-400 shrink-0" />
              <span>{t('nav.localhostIsolated')}</span>
            </div>
          </div>
        ) : (
          <div className="flex justify-center text-emerald-400" title="Footprint: 38.4MB RAM">
            <Cpu className="w-4 h-4 text-cyan-400" />
          </div>
        )}
      </div>
    </aside>
  );
};
