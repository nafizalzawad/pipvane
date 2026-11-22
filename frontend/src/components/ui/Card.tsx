import React from 'react';

interface CardProps {
  children: React.ReactNode;
  className?: string;
  headerAction?: React.ReactNode;
  title?: string;
  subtitle?: string;
  glow?: 'emerald' | 'cyan' | 'amber' | 'none';
}

export const Card: React.FC<CardProps> = ({
  children,
  className = '',
  headerAction,
  title,
  subtitle,
  glow = 'none',
}) => {
  const glowStyles = {
    none: '',
    emerald: 'hover:border-emerald-500/40 hover:shadow-glow-emerald transition-all',
    cyan: 'hover:border-cyan-500/40 hover:shadow-glow-cyan transition-all',
    amber: 'hover:border-amber-500/40 hover:shadow-glow-amber transition-all',
  };

  return (
    <div
      className={`bg-[#111827] border border-slate-800 rounded-xl p-5 shadow-sm ${glowStyles[glow]} ${className}`}
    >
      {(title || headerAction) && (
        <div className="flex items-center justify-between pb-4 mb-4 border-b border-slate-800/80">
          <div>
            {title && <h3 className="text-sm font-semibold text-slate-100 tracking-tight">{title}</h3>}
            {subtitle && <p className="text-xs text-slate-400 mt-0.5">{subtitle}</p>}
          </div>
          {headerAction && <div>{headerAction}</div>}
        </div>
      )}
      {children}
    </div>
  );
};
