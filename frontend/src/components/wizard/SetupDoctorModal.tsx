import React from 'react';
import { useApp } from '@/context/AppContext';
import { useTranslation } from '@/context/I18nContext';
import { Modal } from '@/components/ui/Modal';
import { Button } from '@/components/ui/Button';
import { StepNavigation } from './StepNavigation';
import { Step1License } from './steps/Step1License';
import { Step2Telegram } from './steps/Step2Telegram';
import { Step3Broker } from './steps/Step3Broker';
import { Step4Risk } from './steps/Step4Risk';
import { Step5DemoTest } from './steps/Step5DemoTest';
import { ArrowLeft, ArrowRight, CheckCircle2, RotateCcw, Lock } from 'lucide-react';

export const SetupDoctorModal: React.FC = () => {
  const {
    isSetupDoctorOpen,
    setIsSetupDoctorOpen,
    wizardState,
    setWizardStep,
    resetWizard,
  } = useApp();
  const { t, isRtl, dir } = useTranslation();

  const { currentStep, license } = wizardState;

  const handleNext = () => {
    if (currentStep === 1 && license.status !== 'valid') {
      setWizardStep(2);
      return;
    }

    if (currentStep < 5) {
      setWizardStep(currentStep + 1);
    } else {
      setIsSetupDoctorOpen(false);
    }
  };

  const handleBack = () => {
    if (currentStep > 1) {
      setWizardStep(currentStep - 1);
    }
  };

  const renderStepContent = () => {
    switch (currentStep) {
      case 1:
        return <Step1License />;
      case 2:
        return <Step2Telegram />;
      case 3:
        return <Step3Broker />;
      case 4:
        return <Step4Risk />;
      case 5:
        return <Step5DemoTest />;
      default:
        return <Step1License />;
    }
  };

  const isContinueDisabled = currentStep === 1 && license.status !== 'valid';

  // RTL arrow directions
  const ForwardArrow = isRtl ? ArrowLeft : ArrowRight;
  const BackwardArrow = isRtl ? ArrowRight : ArrowLeft;

  return (
    <Modal
      isOpen={isSetupDoctorOpen}
      onClose={() => setIsSetupDoctorOpen(false)}
      title={t('wizard.modalTitle')}
      subtitle={t('wizard.modalSubtitle')}
      maxWidth="3xl"
    >
      <div dir={dir} className="space-y-6">
        {/* Step Navigation Bar */}
        <StepNavigation />

        {/* Dynamic Step Content */}
        <div className="min-h-[380px]">{renderStepContent()}</div>

        {/* Modal Footer Controls */}
        <div className="flex items-center justify-between pt-4 border-t border-slate-800">
          <Button
            type="button"
            data-testid="reset-wizard-btn"
            variant="ghost"
            size="sm"
            onClick={resetWizard}
            icon={<RotateCcw className="w-3.5 h-3.5" />}
          >
            {t('wizard.resetFlow')}
          </Button>

          <div className="flex items-center gap-2">
            {currentStep > 1 && (
              <Button
                type="button"
                data-testid="back-step-btn"
                variant="secondary"
                onClick={handleBack}
                icon={<BackwardArrow className="w-4 h-4" />}
              >
                {t('wizard.back')}
              </Button>
            )}

            {currentStep < 5 ? (
              <div className="flex flex-col items-end">
                <Button
                  type="button"
                  data-testid="continue-step-btn"
                  variant={isContinueDisabled ? 'secondary' : 'primary'}
                  onClick={handleNext}
                  disabled={isContinueDisabled}
                  className="gap-2"
                  icon={isContinueDisabled ? <Lock className="w-3.5 h-3.5" /> : undefined}
                >
                  <span>
                    {isContinueDisabled
                      ? t('wizard.verifyLicenseToContinue')
                      : t('wizard.continueToStep', { step: currentStep + 1 })}
                  </span>
                  {!isContinueDisabled && <ForwardArrow className="w-4 h-4" />}
                </Button>
                {isContinueDisabled && (
                  <span
                    data-testid="step1-lock-notice"
                    className="text-[10px] text-slate-500 mt-1 font-mono"
                  >
                    {t('wizard.engineValidationRequired')}
                  </span>
                )}
              </div>
            ) : (
              <Button
                type="button"
                data-testid="finish-wizard-btn"
                variant="primary"
                onClick={() => setIsSetupDoctorOpen(false)}
                icon={<CheckCircle2 className="w-4 h-4" />}
              >
                {t('wizard.done')}
              </Button>
            )}
          </div>
        </div>
      </div>
    </Modal>
  );
};
