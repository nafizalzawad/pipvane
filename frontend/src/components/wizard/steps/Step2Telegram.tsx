import React, { useState } from 'react';
import { useApp } from '@/context/AppContext';
import { useTranslation } from '@/context/I18nContext';
import { Badge } from '@/components/ui/Badge';
import { Button } from '@/components/ui/Button';
import { Input } from '@/components/ui/Input';
import { Toggle } from '@/components/ui/Toggle';
import { Shield, QrCode, Phone, CheckCircle2, Radio } from 'lucide-react';

export const Step2Telegram: React.FC = () => {
  const { wizardState, updateTelegram } = useApp();
  const { t } = useTranslation();
  const { telegram } = wizardState;
  const [connectMethod, setConnectMethod] = useState<'qr' | 'phone'>('qr');
  const [phoneVal, setPhoneVal] = useState(telegram.phone);

  return (
    <div className="space-y-6">
      {/* Safety Mode Banner */}
      <div className="p-4 rounded-xl bg-blue-950/30 border border-blue-900/60 flex items-start gap-3">
        <div className="p-2 rounded-lg bg-blue-500/20 text-blue-400 shrink-0 mt-0.5">
          <Shield className="w-5 h-5" />
        </div>
        <div className="flex-1">
          <div className="flex items-center justify-between">
            <h4 className="text-sm font-semibold text-blue-200">
              {t('step2.title')}
            </h4>
            <Badge variant="blue" dot pulse>{t('step2.verifiedBadge')}</Badge>
          </div>
          <p className="text-xs text-blue-300/80 mt-1 leading-relaxed">
            {t('step2.desc')}
          </p>
          <div className="mt-3 pt-3 border-t border-blue-900/40">
            <Toggle
              checked={telegram.safetyMode}
              onChange={(val) => updateTelegram({ safetyMode: val })}
              label={t('step2.toggleLabel')}
              description={t('step2.toggleDesc')}
            />
          </div>
        </div>
      </div>

      {/* Connection Mode Selection */}
      <div className="space-y-4">
        <div className="flex items-center justify-between">
          <span className="text-xs font-semibold uppercase tracking-wider text-slate-400">
            {t('step2.setupHeader')}
          </span>
          <div className="flex bg-[#1E293B] p-1 rounded-lg border border-slate-700">
            <button
              onClick={() => setConnectMethod('qr')}
              className={`px-3 py-1 text-xs rounded-md transition-colors flex items-center gap-1.5 ${
                connectMethod === 'qr'
                  ? 'bg-slate-700 text-white font-medium shadow-sm'
                  : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              <QrCode className="w-3.5 h-3.5" />
              {t('step2.qrTab')}
            </button>
            <button
              onClick={() => setConnectMethod('phone')}
              className={`px-3 py-1 text-xs rounded-md transition-colors flex items-center gap-1.5 ${
                connectMethod === 'phone'
                  ? 'bg-slate-700 text-white font-medium shadow-sm'
                  : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              <Phone className="w-3.5 h-3.5" />
              {t('step2.phoneTab')}
            </button>
          </div>
        </div>

        {connectMethod === 'qr' ? (
          <div className="flex flex-col sm:flex-row items-center gap-6 p-5 rounded-xl bg-[#161F30]/70 border border-slate-800">
            {/* Visual QR Code Placeholder with high-tech trader frame */}
            <div className="relative p-3 bg-white rounded-xl shadow-lg shrink-0">
              <div className="w-36 h-36 flex flex-col items-center justify-center bg-white border-2 border-slate-900 p-2 text-center">
                <div className="grid grid-cols-6 gap-1 w-full h-full p-1 bg-slate-950 rounded">
                  <div className="bg-white col-span-2 row-span-2 rounded-sm" />
                  <div className="bg-slate-700 col-span-2 row-span-1" />
                  <div className="bg-white col-span-2 row-span-2 rounded-sm" />
                  <div className="bg-slate-800 col-span-2" />
                  <div className="bg-emerald-400 col-span-2 row-span-2" />
                  <div className="bg-white col-span-2" />
                  <div className="bg-slate-600 col-span-2" />
                  <div className="bg-white col-span-2 row-span-2 rounded-sm" />
                  <div className="bg-slate-800 col-span-2" />
                  <div className="bg-white col-span-2" />
                </div>
              </div>
              <div className="absolute inset-0 flex items-center justify-center pointer-events-none">
                <span className="p-1 rounded bg-[#0B0F19] text-emerald-400 text-[10px] font-mono border border-emerald-500/50">
                  TDLib
                </span>
              </div>
            </div>

            <div className="space-y-2 text-center ltr:sm:text-left rtl:sm:text-right">
              <h5 className="text-sm font-semibold text-white">
                {t('step2.scanTitle')}
              </h5>
              <ol className="text-xs text-slate-400 space-y-1 list-decimal list-inside leading-relaxed">
                <li>{t('step2.scanStep1')}</li>
                <li>{t('step2.scanStep2')}</li>
                <li>{t('step2.scanStep3')}</li>
              </ol>
              <div className="pt-2 flex items-center gap-2 text-xs text-emerald-400">
                <CheckCircle2 className="w-4 h-4" />
                <span>{t('step2.sessionConnected', { phone: telegram.phone })}</span>
              </div>
            </div>
          </div>
        ) : (
          <div className="p-5 rounded-xl bg-[#161F30]/70 border border-slate-800 space-y-4">
            <Input
              label={t('step2.phoneLabel')}
              value={phoneVal}
              onChange={(e) => {
                setPhoneVal(e.target.value);
                updateTelegram({ phone: e.target.value });
              }}
              placeholder="+1 (555) 000-0000"
              prefixIcon={<Phone className="w-4 h-4" />}
              helperText={t('step2.phoneHelper')}
            />
            <Button size="sm" variant="secondary">
              {t('step2.requestCodeBtn')}
            </Button>
          </div>
        )}
      </div>

      {/* Auto-detected Channels Preview */}
      <div className="space-y-2">
        <div className="flex items-center justify-between">
          <span className="text-xs font-semibold uppercase tracking-wider text-slate-400">
            {t('step2.detectedChannels', { count: telegram.channels.length })}
          </span>
          <Badge variant="cyan" dot>{t('step2.autoMonitored')}</Badge>
        </div>

        <div className="space-y-2">
          {telegram.channels.map((ch) => (
            <div
              key={ch.id}
              className="flex items-center justify-between p-3 rounded-lg bg-[#1E293B]/60 border border-slate-800 hover:border-slate-700 transition-colors"
            >
              <div className="flex items-center gap-3">
                <div className="p-2 rounded-lg bg-slate-800 text-slate-300">
                  <Radio className="w-4 h-4 text-emerald-400" />
                </div>
                <div>
                  <div className="text-sm font-medium text-slate-100">{ch.name}</div>
                  <div className="text-xs text-slate-500">
                    {t('step2.formatInfo', { subscribers: ch.subscribers })}
                  </div>
                </div>
              </div>
              <Badge variant="emerald">{t('step2.liveFeedReady')}</Badge>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};
