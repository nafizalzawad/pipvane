import { test, expect } from '@playwright/test';
import * as path from 'path';

/**
 * Sprint 9 E2E Verification:
 * Validates UI Data Integration, WebSocket status bar binding,
 * and Setup Doctor state transitions with mock Engine API response validation.
 */

const ARTIFACT_DIR = 'C:/Users/USER/.gemini/antigravity/brain/68d461a1-9e43-46fe-8fed-986499336de7/screenshots';
const FRONTEND_DIR = path.resolve(__dirname, '../../../frontend/screenshots');

test.describe('Sprint 9: UI Data Integration & Setup Doctor Verification', () => {
  test('should verify Dashboard WebSocket status bar and Step 1 License failure/success transitions', async ({ page }) => {
    // 1. Navigate to Dashboard
    await page.goto('/');

    // Verify Brand Identity
    await expect(page.getByText('PIPVANE', { exact: true })).toBeVisible();

    // 2. Verify WebSocket Status Bar Data Binding
    const engineBadge = page.locator('[data-testid="engine-status-badge"]');
    await expect(engineBadge).toBeVisible();
    await expect(engineBadge).toContainText('ENGINE: ACTIVE');

    const ipcLatency = page.locator('[data-testid="ipc-latency-metric"]');
    await expect(ipcLatency).toBeVisible();

    const cloudSync = page.locator('[data-testid="cloud-sync-badge"]');
    await expect(cloudSync).toBeVisible();
    await expect(cloudSync).toContainText('Lease Valid');

    // Take screenshot of live dashboard
    await page.screenshot({ path: `${ARTIFACT_DIR}/dashboard_telemetry.png`, fullPage: true });
    await page.screenshot({ path: `${FRONTEND_DIR}/dashboard_telemetry.png`, fullPage: true });

    // 3. Open Setup Doctor Onboarding Wizard
    const launchDoctorBtn = page.locator('[data-testid="launch-setup-doctor-btn"]').first();
    await expect(launchDoctorBtn).toBeVisible();
    await launchDoctorBtn.click();

    // Verify Modal is opened
    const dialog = page.getByRole('dialog');
    await expect(dialog).toBeVisible();
    await expect(dialog.getByText('Setup Doctor: Frictionless 5-Minute Onboarding')).toBeVisible();
    await expect(dialog.getByText('License Authentication & Offline Grace')).toBeVisible();

    // Verify Step 1 is active and "Continue to Step 2" is locked
    const continueBtn = dialog.locator('[data-testid="continue-step-btn"]');
    await expect(continueBtn).toBeVisible();
    await expect(continueBtn).toBeDisabled();
    await expect(continueBtn).toContainText('Verify License to Continue');

    // Verify Step 2 navigation tab is disabled / locked
    const step2Nav = dialog.locator('[data-testid="step-nav-btn-2"]');
    await expect(step2Nav).toBeDisabled();

    // 4. TEST FAILURE STATE: Enter an invalid license key and verify API rejection
    const licenseInput = dialog.locator('[data-testid="license-input"]');
    await licenseInput.fill('INVALID-EXPIRED-TOKEN-000');

    const verifyBtn = dialog.locator('[data-testid="verify-license-btn"]');
    await expect(verifyBtn).toBeEnabled();
    await verifyBtn.click();

    // Wait for API response and failure banner
    const errorBanner = dialog.locator('[data-testid="license-error-banner"]');
    await expect(errorBanner).toBeVisible({ timeout: 5000 });
    await expect(errorBanner).toContainText('License Verification Failed');
    await expect(errorBanner).toContainText('Cryptographic signature verification failed');

    // Ensure Continue to Step 2 remains strictly disabled
    await expect(continueBtn).toBeDisabled();
    await expect(step2Nav).toBeDisabled();

    // Capture FAILURE State Screenshot
    await page.screenshot({ path: `${ARTIFACT_DIR}/step1_license_failure.png` });
    await page.screenshot({ path: `${FRONTEND_DIR}/step1_license_failure.png` });

    // 5. TEST SUCCESS STATE: Enter a valid license key and verify API authorization
    await licenseInput.fill('PV3-PRO-8921-X992-FL40');
    await verifyBtn.click();

    // Wait for API response and success banner
    const successBanner = dialog.locator('[data-testid="license-success-banner"]');
    await expect(successBanner).toBeVisible({ timeout: 5000 });
    await expect(successBanner).toContainText('Cryptographic License Authenticated: Pro Multi-Terminal');
    await expect(successBanner).toContainText('Active Lease');

    // Verify that "Continue to Step 2" is now unlocked and enabled!
    await expect(continueBtn).toBeEnabled();
    await expect(continueBtn).toContainText('Continue to Step 2');

    // Capture SUCCESS State Screenshot
    await page.screenshot({ path: `${ARTIFACT_DIR}/step1_license_success.png` });
    await page.screenshot({ path: `${FRONTEND_DIR}/step1_license_success.png` });

    // 6. Transition to Step 2: Telegram Connect
    await continueBtn.click();
    await expect(dialog.getByText('Telegram Safety Mode (Read-Only Listener)')).toBeVisible();
    await expect(dialog.getByText('FR-4.1 Verified')).toBeVisible();

    // Capture Step 2 Screenshot
    await page.screenshot({ path: `${ARTIFACT_DIR}/step2_telegram.png` });
    await page.screenshot({ path: `${FRONTEND_DIR}/step2_telegram.png` });

    // 7. Transition to Step 3: Broker Selection
    const continueStep2Btn = dialog.locator('[data-testid="continue-step-btn"]');
    await continueStep2Btn.click();
    await expect(dialog.getByText('Broker Terminal Detection')).toBeVisible();
    await expect(dialog.getByText('IC Markets MT5 Terminal (Demo)', { exact: true })).toBeVisible();

    // Capture Step 3 Screenshot
    await page.screenshot({ path: `${ARTIFACT_DIR}/step3_broker.png` });
    await page.screenshot({ path: `${FRONTEND_DIR}/step3_broker.png` });

    // 8. Transition to Step 4: Risk Profile
    const continueStep3Btn = dialog.locator('[data-testid="continue-step-btn"]');
    await continueStep3Btn.click();
    await expect(dialog.getByText('Position Sizing Engine')).toBeVisible();
    await expect(dialog.getByText('Max Drawdown Equity Guard (Hard Stop)')).toBeVisible();

    // Capture Step 4 Screenshot
    await page.screenshot({ path: `${ARTIFACT_DIR}/step4_risk.png` });
    await page.screenshot({ path: `${FRONTEND_DIR}/step4_risk.png` });

    // 9. Transition to Step 5: Demo Trade Test
    const continueStep4Btn = dialog.locator('[data-testid="continue-step-btn"]');
    await continueStep4Btn.click();
    await expect(dialog.getByText('End-to-End Pipeline Verification')).toBeVisible();
    await expect(dialog.getByText('Synthetic Test Signal: BUY XAUUSD 0.01')).toBeVisible();

    // Trigger Mock Trade Execution
    const executeTestBtn = dialog.getByRole('button', { name: /execute test trade/i });
    await expect(executeTestBtn).toBeVisible();
    await executeTestBtn.click();

    // Wait for demo test completion
    await expect(dialog.getByText('Setup Doctor Completed Successfully!')).toBeVisible({ timeout: 8000 });
    await expect(dialog.getByText('Execution SUCCESS')).toBeVisible();

    // Capture Step 5 Screenshot
    await page.screenshot({ path: `${ARTIFACT_DIR}/step5_demo_test.png` });
    await page.screenshot({ path: `${FRONTEND_DIR}/step5_demo_test.png` });

    // 10. Complete setup and return to dashboard
    const launchCopierBtn = dialog.getByRole('button', { name: /launch copier dashboard/i });
    await launchCopierBtn.click();

    // Verify modal closed and we are on dashboard with new trade logged
    await expect(dialog).not.toBeVisible();
    await expect(page.locator('text=DEMO-').first()).toBeVisible();
  });
});
