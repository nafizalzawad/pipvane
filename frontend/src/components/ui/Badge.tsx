import React from 'react';

interface BadgeProps extends React.HTMLAttributes<HTMLSpanElement> {
  children: React.ReactNode;
  variant?: 'emerald' | 'amber' | 'red' | 'cyan' | 'slate' | 'blue';
  dot?: boolean;
  pulse?: boolean;
  className?: string;
}

export const Badge: React.FC<BadgeProps> = ({
  children,
  variant = 'slate',
  dot = false,
  pulse = false,
  className = '',
  ...props
}) => {
  const variantStyles = {
    emerald: 'bg-emerald-950/60 text-emerald-400 border-emerald-800/60',
    amber: 'bg-amber-950/60 text-amber-400 border-amber-800/60',
    red: 'bg-rose-950/60 text-rose-400 border-rose-800/60',
    cyan: 'bg-cyan-950/60 text-cyan-400 border-cyan-800/60',
    blue: 'bg-blue-950/60 text-blue-400 border-blue-800/60',
    slate: 'bg-slate-800/70 text-slate-300 border-slate-700',
  };

  const dotColors = {
    emerald: 'bg-emerald-400',
    amber: 'bg-amber-400',
    red: 'bg-rose-400',
    cyan: 'bg-cyan-400',
    blue: 'bg-blue-400',
    slate: 'bg-slate-400',
  };

  return (
    <span
      className={`inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-xs font-medium border ${variantStyles[variant]} ${className}`}
      {...props}
    >
      {dot && (
        <span
          className={`w-1.5 h-1.5 rounded-full ${dotColors[variant]} ${
            pulse ? 'animate-pulse-dot' : ''
          }`}
        />
      )}
      {children}
    </span>
  );
};
