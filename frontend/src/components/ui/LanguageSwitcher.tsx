import React from 'react';
import { useTranslation, SUPPORTED_LANGUAGES, SupportedLocale } from '@/context/I18nContext';
import { Globe } from 'lucide-react';

export const LanguageSwitcher: React.FC<{ className?: string }> = ({ className = '' }) => {
  const { locale, setLocale } = useTranslation();

  return (
    <div className={`relative inline-flex items-center ${className}`}>
      <div className="flex items-center gap-1.5 bg-[#1E293B] border border-slate-700 hover:border-slate-600 rounded-lg px-2.5 py-1.5 text-xs text-slate-200">
        <Globe className="w-3.5 h-3.5 text-emerald-400 shrink-0" />
        <select
          data-testid="language-select"
          aria-label="Select Interface Language"
          value={locale}
          onChange={(e) => setLocale(e.target.value as SupportedLocale)}
          className="bg-transparent border-none text-slate-200 text-xs font-medium cursor-pointer focus:outline-none pr-1"
        >
          {SUPPORTED_LANGUAGES.map((lang) => (
            <option
              key={lang.code}
              value={lang.code}
              className="bg-[#111827] text-slate-100"
            >
              {lang.nativeName} ({lang.code.toUpperCase()})
            </option>
          ))}
        </select>
      </div>
    </div>
  );
};
