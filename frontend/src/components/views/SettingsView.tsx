import React from 'react';
import { useApp } from '@/context/AppContext';
import { useTranslation, SUPPORTED_LANGUAGES, SupportedLocale } from '@/context/I18nContext';
import { Badge } from '@/components/ui/Badge';
import { Input } from '@/components/ui/Input';
import { Shield, Clock, Lock, Globe } from 'lucide-react';

export const SettingsView: React.FC = () => {
  const { wizardState } = useApp();
  const { t, locale, setLocale, isRtl } = useTranslation();
  const { license } = wizardState;

  return (
    <div className="space-y-6">
      <div className="pb-4 border-b border-slate-800">
        <h2 className="text-lg font-bold text-white">Agent Settings & Local Configuration</h2>
        <p className="text-xs text-slate-400 mt-0.5">
          Localhost security parameters, offline lease status, localization, and engine configuration.
        </p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Security & Access */}
        <div className="p-5 rounded-xl bg-[#111827] border border-slate-800 space-y-4">
          <div className="flex items-center justify-between">
            <h4 className="text-sm font-semibold text-white flex items-center gap-2">
              <Shield className="w-4 h-4 text-emerald-400" />
              Localhost Binding & Security
            </h4>
            <Badge variant="emerald" dot>Strict 127.0.0.1</Badge>
          </div>

          <p className="text-xs text-slate-400 leading-relaxed">
            Pipvane web UI binds exclusively to <code className="text-emerald-400 font-mono">127.0.0.1:8765</code>. No external network traffic is permitted. Access is guarded by a session security token.
          </p>

          <Input
            label="Local Access Token (Auto-Generated)"
            value="pv_sec_99a8f2701b2a"
            readOnly
            isMono
            prefixIcon={<Lock className="w-4 h-4" />}
          />
        </div>

        {/* Offline Grace Period */}
        <div className="p-5 rounded-xl bg-[#111827] border border-slate-800 space-y-4">
          <div className="flex items-center justify-between">
            <h4 className="text-sm font-semibold text-white flex items-center gap-2">
              <Clock className="w-4 h-4 text-amber-400" />
              Offline Grace Period (FR-8.4)
            </h4>
            <Badge variant="amber">30-Day Lease</Badge>
          </div>

          <p className="text-xs text-slate-400 leading-relaxed">
            The local engine operates completely self-contained. Even without internet connectivity to the Cloud Coordinator, your trade copier runs without disruption for up to 30 days.
          </p>

          <div className="p-3 rounded-lg bg-[#161F30] border border-slate-800 font-mono text-xs space-y-1">
            <div className="flex justify-between text-slate-400">
              <span>Remaining Offline Time:</span>
              <span className="text-amber-400 font-bold">{license.daysRemaining} Days</span>
            </div>
            <div className="flex justify-between text-slate-400">
              <span>License Tier:</span>
              <span className="text-slate-200">{license.tier}</span>
            </div>
            <div className="flex justify-between text-slate-400">
              <span>Lease Token Hash:</span>
              <span className="text-slate-400 truncate max-w-xs">{license.key}</span>
            </div>
          </div>
        </div>

        {/* Language & RTL Localization (Sprint 24) */}
        <div className="p-5 rounded-xl bg-[#111827] border border-slate-800 space-y-4 lg:col-span-2">
          <div className="flex items-center justify-between">
            <h4 className="text-sm font-semibold text-white flex items-center gap-2">
              <Globe className="w-4 h-4 text-emerald-400" />
              Language & RTL Layout Localization (PRD Mandated)
            </h4>
            <Badge variant={isRtl ? 'cyan' : 'slate'} dot>
              {isRtl ? 'RTL Layout Active' : 'LTR Layout'}
            </Badge>
          </div>

          <p className="text-xs text-slate-400 leading-relaxed">
            Pipvane supports full internationalization across English, Arabic (العربية with dynamic RTL flipping), Spanish (Español), Portuguese (Português), and Bengali (বাংলা).
          </p>

          <div className="grid grid-cols-2 sm:grid-cols-5 gap-3 pt-1">
            {SUPPORTED_LANGUAGES.map((lang) => {
              const isSelected = locale === lang.code;
              return (
                <button
                  key={lang.code}
                  data-testid={`lang-btn-${lang.code}`}
                  onClick={() => setLocale(lang.code)}
                  className={`p-3 rounded-xl border text-center transition-all ${
                    isSelected
                      ? 'bg-[#1E293B] border-emerald-500 shadow-glow-emerald ring-1 ring-emerald-500/40 text-white font-bold'
                      : 'bg-[#161F30]/70 border-slate-800 hover:border-slate-700 text-slate-300'
                  }`}
                >
                  <div className="text-sm">{lang.nativeName}</div>
                  <div className="text-[10px] text-slate-400 uppercase font-mono mt-0.5">
                    {lang.name} ({lang.dir.toUpperCase()})
                  </div>
                </button>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );
};
