const { test, expect } = require('@playwright/test');
const path = require('path');

/**
 * Pipvane v3: Sprint 9 - E2E Onboarding Wizard Verification Suite
 *
 * Requirements:
 * 1. Playwright Setup: Spanning both Mock Engine API (port 8765) and static UI (port 3000).
 * 2. Happy Path Flow: Valid License -> Dummy Telegram -> Demo MT5 account -> Finish -> Main Dashboard view.
 * 3. Error Handling Flow: Invalid license -> User-friendly error -> Next locked -> No raw HTTP status codes.
 */

test.describe('Pipvane v3 User Onboarding Wizard (Setup Doctor)', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to root dashboard
    await page.goto('/');

    // Wait for initial render of dashboard shell
    await expect(page.locator('text=PIPVANE').first()).toBeVisible({ timeout: 10000 });
  });

  test('Happy Path: Complete 5-step onboarding flow and route to Dashboard', async ({ page }) => {
    // 1. Open Setup Doctor Wizard modal
    const openWizardBtn = page
      .locator('[data-testid="banner-launch-doctor-btn"]')
      .or(page.locator('[data-testid="launch-setup-doctor-btn"]'))
      .first();

    await expect(openWizardBtn).toBeVisible();
    await openWizardBtn.click();

    // Verify Setup Doctor modal dialog opens
    const modalDialog = page.getByRole('dialog');
    await expect(modalDialog).toBeVisible();
    await expect(modalDialog.locator('text=Setup Doctor: Frictionless 5-Minute Onboarding')).toBeVisible();
    await expect(modalDialog.locator('text=License Authentication & Offline Grace')).toBeVisible();

    // Verify Continue button is initially locked
    const continueBtn = modalDialog.locator('[data-testid="continue-step-btn"]');
    await expect(continueBtn).toBeVisible();
    await expect(continueBtn).toBeDisabled();
    await expect(continueBtn).toContainText(/Verify License to Continue/i);

    // 2. Step 1: Input valid Pipvane license key
    const licenseInput = modalDialog.locator('[data-testid="license-input"]');
    await expect(licenseInput).toBeVisible();
    await licenseInput.fill('PV3-PRO-8921-X992-FL40');

    // Click "Verify License"
    const verifyBtn = modalDialog.locator('[data-testid="verify-license-btn"]');
    await expect(verifyBtn).toBeEnabled();
    await verifyBtn.click();

    // Assert active cryptographic lease is verified
    const successBanner = modalDialog.locator('[data-testid="license-success-banner"]');
    await expect(successBanner).toBeVisible({ timeout: 6000 });
    await expect(successBanner).toContainText(/Cryptographic License Authenticated/i);
    await expect(successBanner).toContainText(/Active Lease/i);

    // Assert Continue button is now unlocked
    await expect(continueBtn).toBeEnabled();
    await expect(continueBtn).toContainText(/Continue to Step 2/i);

    // Advance to Step 2: Telegram
    await continueBtn.click();

    // 3. Step 2: Connect dummy Telegram credentials
    await expect(modalDialog.locator('text=Telegram Safety Mode (Read-Only Listener)')).toBeVisible();
    await expect(modalDialog.locator('text=FR-4.1 Verified')).toBeVisible();

    // Test connecting via Phone mode with dummy credentials
    const phoneModeBtn = modalDialog.locator('[data-testid="connect-method-phone"]');
    if (await phoneModeBtn.isVisible()) {
      await phoneModeBtn.click();
      const phoneInput = modalDialog.locator('[data-testid="telegram-phone-input"]');
      await phoneInput.fill('+1 (555) 0199-MOCK');
      const requestCodeBtn = modalDialog.locator('[data-testid="request-telegram-code-btn"]');
      await requestCodeBtn.click();
    }

    // Switch back to QR / verify active session
    const qrModeBtn = modalDialog.locator('[data-testid="connect-method-qr"]');
    if (await qrModeBtn.isVisible()) {
      await qrModeBtn.click();
      await expect(modalDialog.locator('text=Session connected:')).toBeVisible();
    }

    // Advance to Step 3: Broker Selection
    await continueBtn.click();

    // 4. Step 3: Add / Select demo MT5 account
    await expect(modalDialog.locator('text=Broker Terminal Detection')).toBeVisible();
    await expect(modalDialog.locator('text=IC Markets MT5 Terminal (Demo)').first()).toBeVisible();

    // Click to select the IC Markets MT5 demo terminal card
    const mt5Card = modalDialog.locator('[data-testid="terminal-card-term-1"]');
    await expect(mt5Card).toBeVisible();
    await mt5Card.click();

    // Verify IPC pipe status badge is ready
    await expect(modalDialog.locator('text=IPC Pipe Ready').first()).toBeVisible();

    // Advance to Step 4: Risk Profile
    await continueBtn.click();

    // 5. Step 4: Configure Risk Settings
    await expect(modalDialog.locator('text=Position Sizing Engine')).toBeVisible();
    await expect(modalDialog.locator('text=Max Drawdown Equity Guard (Hard Stop)')).toBeVisible();

    // Advance to Step 5: Synthetic Test & Finalization
    await continueBtn.click();

    // 6. Step 5: Complete onboarding and click "Finish"
    await expect(modalDialog.locator('text=End-to-End Pipeline Verification')).toBeVisible();
    await expect(modalDialog.locator('text=Synthetic Test Signal: BUY XAUUSD 0.01')).toBeVisible();

    // Locate the Finish button
    const finishBtn = modalDialog
      .getByRole('button', { name: /^Finish$/i })
      .or(modalDialog.locator('[data-testid="finish-wizard-btn"]'));

    await expect(finishBtn).toBeVisible();
    await finishBtn.click();

    // 7. Verification: Assert final view routes to main Dashboard
    await expect(modalDialog).not.toBeVisible();

    // Assert main Dashboard components are visible and operational
    await expect(page.locator('text=Live Execution Feed')).toBeVisible();
    await expect(page.locator('text=Sub-ms Dispatch Active')).toBeVisible();
    await expect(page.locator('text=IC Markets MT5 Terminal (Demo)').first()).toBeVisible();
    await expect(page.locator('[data-testid="engine-status-badge"]')).toBeVisible();
  });

  test('Error Handling: Validate invalid license key rejection and UI error safety', async ({ page }) => {
    // 1. Open Setup Doctor Wizard modal
    const openWizardBtn = page
      .locator('[data-testid="banner-launch-doctor-btn"]')
      .or(page.locator('[data-testid="launch-setup-doctor-btn"]'))
      .first();

    await expect(openWizardBtn).toBeVisible();
    await openWizardBtn.click();

    const modalDialog = page.getByRole('dialog');
    await expect(modalDialog).toBeVisible();

    const continueBtn = modalDialog.locator('[data-testid="continue-step-btn"]');
    await expect(continueBtn).toBeDisabled();

    // 2. Simulate user typing an invalid license key
    const licenseInput = modalDialog.locator('[data-testid="license-input"]');
    const invalidKey = 'INVALID-UNTRUSTED-KEY-999';
    await licenseInput.fill(invalidKey);

    // Click "Verify License"
    const verifyBtn = modalDialog.locator('[data-testid="verify-license-btn"]');
    await verifyBtn.click();

    // 3. Assert user-friendly error message is displayed
    const errorBanner = modalDialog.locator('[data-testid="license-error-banner"]');
    await expect(errorBanner).toBeVisible({ timeout: 6000 });
    await expect(errorBanner).toContainText('License Verification Failed');
    await expect(errorBanner).toContainText(/Token is invalid, untrusted, or has expired/i);

    // 4. Assert UI does NOT expose raw HTTP status codes
    const errorBannerText = await errorBanner.innerText();
    const rawStatusCodePattern = /\b(HTTP\s+status|status\s+code\s+\d{3}|400|401|403|404|500|502|503)\b/i;
    expect(errorBannerText).not.toMatch(rawStatusCodePattern);

    // 5. Assert the "Next" / "Continue" button strictly remains locked
    await expect(continueBtn).toBeDisabled();
    await expect(continueBtn).toContainText(/Verify License to Continue/i);

    // Assert step 2 navigation tab is also locked
    const step2Nav = modalDialog.locator('[data-testid="step-nav-btn-2"]');
    if (await step2Nav.isVisible()) {
      await expect(step2Nav).toBeDisabled();
    }

    // 6. Recovery: User enters a valid license key after failure
    await licenseInput.fill('PV3-PRO-8921-X992-FL40');
    await verifyBtn.click();

    // Assert error banner is dismissed and success banner appears
    await expect(errorBanner).not.toBeVisible({ timeout: 6000 });
    const successBanner = modalDialog.locator('[data-testid="license-success-banner"]');
    await expect(successBanner).toBeVisible();

    // Assert Continue button is now unlocked
    await expect(continueBtn).toBeEnabled();
    await expect(continueBtn).toContainText(/Continue to Step 2/i);
  });
});
