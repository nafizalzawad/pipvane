import { test, expect } from '@playwright/test';
import * as path from 'path';

/**
 * Sprint 24 E2E Verification:
 * Validates PRD-mandated internationalization (en, ar, es, pt, bn)
 * and dynamic RTL layout flipping when Arabic (ar) is selected.
 */

const ARTIFACT_DIR = 'C:/Users/USER/.gemini/antigravity/brain/68d461a1-9e43-46fe-8fed-986499336de7/screenshots';
const FRONTEND_DIR = path.resolve(__dirname, '../../../frontend/screenshots');

test.describe('Sprint 24: Localization & Arabic RTL Verification', () => {
  test('should switch interface language to Arabic, flip layout to RTL, and render Setup Doctor in full Arabic', async ({ page }) => {
    // 1. Navigate to Dashboard
    await page.goto('/');

    // 2. Select Arabic (ar) from the Language Switcher
    const langSelect = page.locator('[data-testid="language-select"]');
    await expect(langSelect).toBeVisible();
    await langSelect.selectOption('ar');

    // 3. Verify Document Direction and Language Attributes
    await expect(page.locator('html')).toHaveAttribute('dir', 'rtl');
    await expect(page.locator('html')).toHaveAttribute('lang', 'ar');

    // Verify Arabic navigation items are rendered
    await expect(page.getByText('لوحة التحكم')).toBeVisible();
    await expect(page.getByText('الحسابات')).toBeVisible();
    await expect(page.getByText('طبيب إعداد الوكيل المحلي')).toBeVisible();

    // Verify RTL sidebar alignment
    const sidebar = page.locator('aside');
    await expect(sidebar).toBeVisible();

    // Take screenshot of Dashboard in Arabic (RTL)
    await page.screenshot({ path: `${ARTIFACT_DIR}/dashboard_arabic_rtl.png`, fullPage: true });
    await page.screenshot({ path: `${FRONTEND_DIR}/dashboard_arabic_rtl.png`, fullPage: true });

    // 4. Open Setup Doctor Onboarding Wizard in Arabic
    const launchDoctorBtn = page.locator('[data-testid="launch-setup-doctor-btn"]').first();
    await expect(launchDoctorBtn).toBeVisible();
    await launchDoctorBtn.click();

    // Verify Modal Dialog is opened and has RTL direction
    const dialog = page.getByRole('dialog');
    await expect(dialog).toBeVisible();
    await expect(dialog).toHaveAttribute('dir', 'rtl');

    // Verify Arabic Modal Header
    await expect(dialog.getByText('طبيب إعداد Pipvane: إعداد سلس خلال 5 دقائق')).toBeVisible();
    await expect(dialog.getByText('مصادقة الترخيص وفترة السماح دون اتصال')).toBeVisible();

    // 5. STEP 1 IN ARABIC: Load valid key and authenticate
    const loadValidBtn = dialog.locator('[data-testid="preset-valid-key"]');
    await expect(loadValidBtn).toBeVisible();
    await loadValidBtn.click();

    const verifyBtn = dialog.locator('[data-testid="verify-license-btn"]');
    await expect(verifyBtn).toBeVisible();
    await verifyBtn.click();

    // Verify Arabic success alert
    const successBanner = dialog.locator('[data-testid="license-success-banner"]');
    await expect(successBanner).toBeVisible({ timeout: 5000 });
    await expect(successBanner.getByText('تمت مصادقة الترخيص التشفيري: Pro Multi-Terminal')).toBeVisible();
    await expect(dialog.getByText('عقد نشط')).toBeVisible();

    // Verify Continue button is unlocked in Arabic
    const continueBtn = dialog.locator('[data-testid="continue-step-btn"]');
    await expect(continueBtn).toBeEnabled();
    await expect(continueBtn.getByText('المتابعة إلى الخطوة 2')).toBeVisible();

    // Capture Step 1 Arabic RTL Screenshot
    await page.screenshot({ path: `${ARTIFACT_DIR}/setup_doctor_step1_arabic_rtl.png` });
    await page.screenshot({ path: `${FRONTEND_DIR}/setup_doctor_step1_arabic_rtl.png` });

    // 6. STEP 2 IN ARABIC: Telegram Connect (Safety Mode)
    await continueBtn.click();
    await expect(dialog.getByText('وضع الأمان لتلغرام (مستمع للقراءة فقط)')).toBeVisible();
    await expect(dialog.getByText('معتمد بموجب FR-4.1')).toBeVisible();
    await expect(dialog.getByText('امسح الرمز باستخدام تطبيق تلغرام على هاتفك')).toBeVisible();

    // Capture Step 2 Arabic RTL Screenshot
    await page.screenshot({ path: `${ARTIFACT_DIR}/setup_doctor_step2_arabic_rtl.png` });
    await page.screenshot({ path: `${FRONTEND_DIR}/setup_doctor_step2_arabic_rtl.png` });

    // 7. STEP 3 IN ARABIC: Broker Terminal Detection
    const continueStep2Btn = dialog.locator('[data-testid="continue-step-btn"]');
    await continueStep2Btn.click();
    await expect(dialog.getByText('اكتشاف منصات التداول')).toBeVisible();
    await expect(dialog.getByText('منصات التنفيذ المتاحة')).toBeVisible();

    // Capture Step 3 Arabic RTL Screenshot
    await page.screenshot({ path: `${ARTIFACT_DIR}/setup_doctor_step3_broker_arabic_rtl.png` });
    await page.screenshot({ path: `${FRONTEND_DIR}/setup_doctor_step3_broker_arabic_rtl.png` });

    // 8. STEP 4 IN ARABIC: Risk Profile & Equity Hard Stop
    const continueStep3Btn = dialog.locator('[data-testid="continue-step-btn"]');
    await continueStep3Btn.click();
    await expect(dialog.getByText('محرك تحديد حجم الصفقات')).toBeVisible();
    await expect(dialog.getByText('نسبة المخاطرة من رأس المال (ديناميكي)')).toBeVisible();
    await expect(dialog.getByText('حماية أقصى تراجع لرأس المال (إيقاف صارم)')).toBeVisible();

    // Capture Step 4 Arabic RTL Screenshot
    await page.screenshot({ path: `${ARTIFACT_DIR}/setup_doctor_step4_risk_arabic_rtl.png` });
    await page.screenshot({ path: `${FRONTEND_DIR}/setup_doctor_step4_risk_arabic_rtl.png` });

    // 9. STEP 5 IN ARABIC: Demo Trade Test & Sub-ms Execution
    const continueStep4Btn = dialog.locator('[data-testid="continue-step-btn"]');
    await continueStep4Btn.click();
    await expect(dialog.getByText('التحقق الشامل من مسار التنفيذ')).toBeVisible();
    await expect(dialog.getByText('إشارة اختبار اصطناعية: شراء XAUUSD 0.01')).toBeVisible();

    // Execute Test Trade in Arabic
    const executeTestBtn = dialog.getByRole('button', { name: /تنفيذ صفقة الاختبار/i });
    await expect(executeTestBtn).toBeVisible();
    await executeTestBtn.click();

    // Wait for completion banner in Arabic
    await expect(dialog.getByText('اكتمل إعداد طبيب الإعداد بنجاح!')).toBeVisible({ timeout: 8000 });
    await expect(dialog.getByText('فتح لوحة تحكم الناسخ')).toBeVisible();

    // Capture Step 5 Arabic RTL Screenshot
    await page.screenshot({ path: `${ARTIFACT_DIR}/setup_doctor_step5_demo_arabic_rtl.png` });
    await page.screenshot({ path: `${FRONTEND_DIR}/setup_doctor_step5_demo_arabic_rtl.png` });
  });

  test('should verify Spanish, Portuguese, and Bengali language switches', async ({ page }) => {
    await page.goto('/');

    const langSelect = page.locator('[data-testid="language-select"]');

    // Switch to Spanish
    await langSelect.selectOption('es');
    await expect(page.locator('html')).toHaveAttribute('dir', 'ltr');
    await expect(page.locator('html')).toHaveAttribute('lang', 'es');
    await expect(page.getByText('Panel de Control')).toBeVisible();
    await expect(page.getByText('Doctor de Configuración del Agente Local')).toBeVisible();

    // Switch to Portuguese
    await langSelect.selectOption('pt');
    await expect(page.locator('html')).toHaveAttribute('dir', 'ltr');
    await expect(page.locator('html')).toHaveAttribute('lang', 'pt');
    await expect(page.getByText('Painel de Controlo')).toBeVisible();
    await expect(page.getByText('Doutor de Configuração do Agente Local')).toBeVisible();

    // Switch to Bengali
    await langSelect.selectOption('bn');
    await expect(page.locator('html')).toHaveAttribute('dir', 'ltr');
    await expect(page.locator('html')).toHaveAttribute('lang', 'bn');
    await expect(page.getByText('ড্যাশবোর্ড')).toBeVisible();
    await expect(page.getByText('লোকাল এজেন্ট সেটআপ ডক্টর')).toBeVisible();
  });
});
