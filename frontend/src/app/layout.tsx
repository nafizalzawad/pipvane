import type { Metadata } from 'next';
import './globals.css';
import { I18nProvider } from '@/context/I18nContext';
import { AppProvider } from '@/context/AppContext';

export const metadata: Metadata = {
  title: 'Pipvane v3 - Ultra-Low Latency Trade Copier',
  description: 'Localhost Trade Copier Agent with Sub-Millisecond IPC Engine and Safety Mode Telegram Listener',
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className="dark">
      <body className="bg-[#0B0F19] text-slate-100 antialiased min-h-screen">
        <I18nProvider>
          <AppProvider>{children}</AppProvider>
        </I18nProvider>
      </body>
    </html>
  );
}
