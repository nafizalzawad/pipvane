/**
 * Pipvane v3: Internationalization Configuration (Sprint 24)
 * Mandated PRD Languages: English (en), Arabic (ar), Spanish (es), Portuguese (pt), Bengali (bn)
 */

module.exports = {
  i18n: {
    defaultLocale: 'en',
    locales: ['en', 'ar', 'es', 'pt', 'bn'],
  },
  fallbackLng: {
    default: ['en'],
  },
  localePath: typeof window === 'undefined' ? './public/locales' : '/locales',
  reloadOnPrerender: process.env.NODE_ENV === 'development',
};
