/** @type {import('tailwindcss').Config} */
module.exports = {
  darkMode: 'class',
  content: [
    './src/pages/**/*.{js,ts,jsx,tsx,mdx}',
    './src/components/**/*.{js,ts,jsx,tsx,mdx}',
    './src/app/**/*.{js,ts,jsx,tsx,mdx}',
  ],
  theme: {
    extend: {
      colors: {
        pip: {
          dark: '#0B0F19',
          card: '#111827',
          surface: '#1E293B',
          'surface-hover': '#283548',
          border: '#374151',
          'border-subtle': '#1F2937',
          emerald: '#10B981',
          'emerald-light': '#34D399',
          'emerald-muted': 'rgba(16, 185, 129, 0.15)',
          amber: '#F59E0B',
          'amber-light': '#FCD34D',
          'amber-muted': 'rgba(245, 158, 11, 0.15)',
          red: '#EF4444',
          'red-light': '#F87171',
          'red-muted': 'rgba(239, 68, 68, 0.15)',
          cyan: '#06B6D4',
          'cyan-light': '#38BDF8',
          'cyan-muted': 'rgba(6, 182, 212, 0.15)',
          blue: '#3B82F6',
          'blue-muted': 'rgba(59, 130, 246, 0.15)',
        },
      },
      fontFamily: {
        mono: ['ui-monospace', 'SFMono-Regular', 'Menlo', 'Monaco', 'Consolas', 'Liberation Mono', 'Courier New', 'monospace'],
        sans: ['Inter', 'system-ui', '-apple-system', 'BlinkMacSystemFont', 'Segoe UI', 'Roboto', 'sans-serif'],
      },
      boxShadow: {
        'glow-emerald': '0 0 20px -5px rgba(16, 185, 129, 0.3)',
        'glow-cyan': '0 0 20px -5px rgba(6, 182, 212, 0.3)',
        'glow-amber': '0 0 20px -5px rgba(245, 158, 11, 0.3)',
      },
      animation: {
        'pulse-slow': 'pulse 3s cubic-bezier(0.4, 0, 0.6, 1) infinite',
      },
    },
  },
  plugins: [],
};
