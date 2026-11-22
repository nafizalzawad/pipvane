'use client';

import React, { createContext, useContext, useState, useEffect, useCallback } from 'react';
import enDict from '../locales/en.json';
import arDict from '../locales/ar.json';
import esDict from '../locales/es.json';
import ptDict from '../locales/pt.json';
import bnDict from '../locales/bn.json';

export type SupportedLocale = 'en' | 'ar' | 'es' | 'pt' | 'bn';

export interface LanguageInfo {
  code: SupportedLocale;
  name: string;
  nativeName: string;
  dir: 'ltr' | 'rtl';
}

export const SUPPORTED_LANGUAGES: LanguageInfo[] = [
  { code: 'en', name: 'English', nativeName: 'English', dir: 'ltr' },
  { code: 'ar', name: 'Arabic', nativeName: 'العربية', dir: 'rtl' },
  { code: 'es', name: 'Spanish', nativeName: 'Español', dir: 'ltr' },
  { code: 'pt', name: 'Portuguese', nativeName: 'Português', dir: 'ltr' },
  { code: 'bn', name: 'Bengali', nativeName: 'বাংলা', dir: 'ltr' },
];

const dictionaries: Record<SupportedLocale, Record<string, any>> = {
  en: enDict,
  ar: arDict,
  es: esDict,
  pt: ptDict,
  bn: bnDict,
};

interface I18nContextType {
  locale: SupportedLocale;
  setLocale: (locale: SupportedLocale) => void;
  isRtl: boolean;
  dir: 'ltr' | 'rtl';
  t: (path: string, params?: Record<string, string | number>) => string;
}

const STORAGE_KEY_LOCALE = 'pipvane_locale';

const I18nContext = createContext<I18nContextType | undefined>(undefined);

export const I18nProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [locale, setLocaleState] = useState<SupportedLocale>('en');

  // Load persisted locale preference
  useEffect(() => {
    if (typeof window !== 'undefined') {
      try {
        const stored = window.localStorage.getItem(STORAGE_KEY_LOCALE) as SupportedLocale;
        if (stored && ['en', 'ar', 'es', 'pt', 'bn'].includes(stored)) {
          setLocaleState(stored);
        }
      } catch (err) {
        console.warn('Could not read locale from localStorage:', err);
      }
    }
  }, []);

  const isRtl = locale === 'ar';
  const dir = isRtl ? 'rtl' : 'ltr';

  // Synchronize document direction and lang attributes
  useEffect(() => {
    if (typeof document !== 'undefined') {
      document.documentElement.dir = dir;
      document.documentElement.lang = locale;
      if (isRtl) {
        document.documentElement.classList.add('rtl');
      } else {
        document.documentElement.classList.remove('rtl');
      }
    }
  }, [dir, locale, isRtl]);

  const setLocale = (newLocale: SupportedLocale) => {
    setLocaleState(newLocale);
    if (typeof window !== 'undefined') {
      try {
        window.localStorage.setItem(STORAGE_KEY_LOCALE, newLocale);
      } catch (err) {
        console.warn('Could not save locale to localStorage:', err);
      }
    }
  };

  /**
   * Nested translation lookup with interpolation.
   * e.g., t('common.daysRemaining', { days: 30 })
   */
  const t = useCallback(
    (path: string, params?: Record<string, string | number>): string => {
      const keys = path.split('.');
      const currentDict = dictionaries[locale] || dictionaries.en;

      let value: any = currentDict;
      for (const k of keys) {
        if (value && typeof value === 'object' && k in value) {
          value = value[k];
        } else {
          value = undefined;
          break;
        }
      }

      // Fallback to English if key is missing in active language
      if (value === undefined && locale !== 'en') {
        let fallbackValue: any = dictionaries.en;
        for (const k of keys) {
          if (fallbackValue && typeof fallbackValue === 'object' && k in fallbackValue) {
            fallbackValue = fallbackValue[k];
          } else {
            fallbackValue = undefined;
            break;
          }
        }
        value = fallbackValue;
      }

      if (typeof value !== 'string') {
        return path;
      }

      if (params) {
        return Object.entries(params).reduce((str, [paramKey, paramVal]) => {
          return str.replace(new RegExp(`{{${paramKey}}}`, 'g'), String(paramVal));
        }, value);
      }

      return value;
    },
    [locale]
  );

  return (
    <I18nContext.Provider value={{ locale, setLocale, isRtl, dir, t }}>
      {children}
    </I18nContext.Provider>
  );
};

export const useTranslation = () => {
  const context = useContext(I18nContext);
  if (!context) {
    throw new Error('useTranslation must be used within an I18nProvider');
  }
  return context;
};
