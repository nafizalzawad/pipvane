import React from 'react';
import { useApp } from '@/context/AppContext';
import { useTranslation } from '@/context/I18nContext';
import { Sidebar } from './Sidebar';
import { TopBar } from './TopBar';
import { SetupDoctorModal } from '../wizard/SetupDoctorModal';
import { DashboardView } from '../dashboard/DashboardView';
import { AccountsView } from '../views/AccountsView';
import { ChannelsView } from '../views/ChannelsView';
import { RulesView } from '../views/RulesView';
import { SettingsView } from '../views/SettingsView';

export const DashboardLayout: React.FC = () => {
  const { currentTab } = useApp();
  const { dir } = useTranslation();

  const renderCurrentView = () => {
    switch (currentTab) {
      case 'dashboard':
        return <DashboardView />;
      case 'accounts':
        return <AccountsView />;
      case 'channels':
        return <ChannelsView />;
      case 'rules':
        return <RulesView />;
      case 'settings':
        return <SettingsView />;
      default:
        return <DashboardView />;
    }
  };

  return (
    <div dir={dir} className="flex h-screen w-screen overflow-hidden bg-[#0B0F19] text-slate-100">
      {/* Navigation Sidebar (flips automatically to right side in RTL) */}
      <Sidebar />

      {/* Main Area: Top Bar + Viewport */}
      <div className="flex-1 flex flex-col min-w-0 h-screen overflow-hidden">
        <TopBar />

        {/* Scrollable Main Content Viewport */}
        <main className="flex-1 overflow-y-auto p-4 sm:p-6 lg:p-8">
          <div className="max-w-7xl mx-auto">{renderCurrentView()}</div>
        </main>
      </div>

      {/* Setup Doctor Onboarding Wizard Modal */}
      <SetupDoctorModal />
    </div>
  );
};
