import React from 'react';

interface InputProps extends React.InputHTMLAttributes<HTMLInputElement> {
  label?: string;
  helperText?: string;
  error?: string;
  prefixIcon?: React.ReactNode;
  suffix?: React.ReactNode;
  isMono?: boolean;
}

export const Input: React.FC<InputProps> = ({
  label,
  helperText,
  error,
  prefixIcon,
  suffix,
  isMono = false,
  className = '',
  disabled,
  ...props
}) => {
  return (
    <div className="w-full">
      {label && (
        <label className="block text-xs font-medium text-slate-300 mb-1.5">
          {label}
        </label>
      )}
      <div className="relative flex items-center">
        {prefixIcon && (
          <div className="absolute left-3 flex items-center pointer-events-none text-slate-400">
            {prefixIcon}
          </div>
        )}
        <input
          className={`w-full bg-[#1E293B] border ${
            error ? 'border-rose-500 focus:border-rose-400 focus:ring-rose-500/20' : 'border-slate-700 focus:border-emerald-500 focus:ring-emerald-500/20'
          } rounded-lg py-2 text-sm text-slate-100 placeholder-slate-500 focus:outline-none focus:ring-2 transition-colors disabled:opacity-50 disabled:bg-slate-900 ${
            prefixIcon ? 'pl-9' : 'pl-3'
          } ${suffix ? 'pr-12' : 'pr-3'} ${isMono ? 'font-mono tracking-wide' : ''} ${className}`}
          disabled={disabled}
          {...props}
        />
        {suffix && (
          <div className="absolute right-3 flex items-center text-xs text-slate-400">
            {suffix}
          </div>
        )}
      </div>
      {error ? (
        <p className="text-xs text-rose-400 mt-1">{error}</p>
      ) : helperText ? (
        <p className="text-xs text-slate-500 mt-1">{helperText}</p>
      ) : null}
    </div>
  );
};
