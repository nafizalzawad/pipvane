import React from 'react';
import { useApp } from '@/context/AppContext';
import { useTranslation } from '@/context/I18nContext';
import { KeyRound, MessageSquare, Monitor, ShieldAlert, Zap, Check, Lock } from 'lucide-react';

export const StepNavigation: React.FC = () => {
  const { wizardState, setWizardStep, isStepAllowed } = useApp();
  const { t } = useTranslation();
  const { currentStep } = wizardState;

  const steps = [
    { id: 1, label: t('step1.tabLabel'), icon: KeyRound },
    { id: 2, label: t('step2.tabLabel'), icon: MessageSquare },
    { id: 3, label: t('step3.tabLabel'), icon: Monitor },
    { id: 4, label: t('step4.tabLabel'), icon: ShieldAlert },
    { id: 5, label: t('step5.tabLabel'), icon: Zap },
  ];

  return (
    <div className="w-full pb-4">
      <div className="grid grid-cols-5 gap-2">
        {steps.map((step) => {
          const isCompleted = step.id < currentStep && isStepAllowed(step.id);
          const isCurrent = step.id === currentStep;
          const isLocked = !isStepAllowed(step.id);
          const IconComponent = step.icon;

          return (
            <button
              key={step.id}
              data-testid={`step-nav-btn-${step.id}`}
              onClick={() => {
                if (!isLocked) {
                  setWizardStep(step.id);
                }
              }}
              disabled={isLocked && !isCurrent}
              className={`flex flex-col items-center sm:items-start p-2 sm:p-2.5 rounded-lg border ltr:text-left rtl:text-right transition-all relative ${
                isCurrent
                  ? 'bg-[#1E293B] border-emerald-500 shadow-sm ring-1 ring-emerald-500/30'
                  : isCompleted
                  ? 'bg-[#161F30]/80 border-slate-700/80 hover:border-slate-600 cursor-pointer'
                  : isLocked
                  ? 'bg-[#111827]/40 border-slate-800/60 opacity-40 cursor-not-allowed'
                  : 'bg-[#111827]/60 border-slate-800 hover:border-slate-700 cursor-pointer'
              }`}
            >
              <div className="flex items-center gap-2 w-full">
                <div
                  className={`w-6 h-6 rounded-md flex items-center justify-center shrink-0 text-xs font-mono font-bold ${
                    isCurrent
                      ? 'bg-emerald-500 text-slate-950'
                      : isCompleted
                      ? 'bg-emerald-950 text-emerald-400 border border-emerald-800'
                      : isLocked
                      ? 'bg-slate-900 text-slate-600 border border-slate-800'
                      : 'bg-slate-800 text-slate-400'
                  }`}
                >
                  {isCompleted ? (
                    <Check className="w-3.5 h-3.5" />
                  ) : isLocked ? (
                    <Lock className="w-3 h-3 text-slate-500" />
                  ) : (
                    step.id
                  )}
                </div>
                <div className="hidden sm:block">
                  <span
                    className={`block text-[10px] uppercase tracking-wider font-semibold ${
                      isCurrent
                        ? 'text-emerald-400'
                        : isCompleted
                        ? 'text-slate-400'
                        : 'text-slate-500'
                    }`}
                  >
                    Step {step.id}: {step.label}
                  </span>
                  <span
                    className={`block text-xs font-medium truncate ${
                      isCurrent ? 'text-white font-semibold' : 'text-slate-300'
                    }`}
                  >
                    {step.label}
                  </span>
                </div>
              </div>

              {/* Progress bar line under active step */}
              {isCurrent && (
                <div className="absolute bottom-0 left-0 right-0 h-0.5 bg-emerald-500 rounded-b-lg" />
              )}
            </button>
          );
        })}
      </div>
    </div>
  );
};
