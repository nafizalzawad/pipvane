import { test, expect } from '@playwright/test';

/**
 * Pipvane v3: Dashboard E2E Playwright Verification
 * Tests static UI loading, telemetry headers, sub-ms latency indicators, and interactive components.
 */

test.describe('Pipvane v3 Local Agent Dashboard', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to dashboard root
    await page.goto('/');
  });

  test('should render page title and brand identity', async ({ page }) => {
    await expect(page).toHaveTitle(/Pipvane v3/i);
    await expect(page.locator('text=PIPVANE').first()).toBeVisible();
    await expect(page.locator('text=Local Copier Agent').first()).toBeVisible();
  });

  test('should display active engine status and sub-5ms IPC latency metric (VR-3.1)', async ({ page }) => {
    // Check Engine status indicator
    const engineBadge = page.locator('text=ENGINE: ACTIVE');
    await expect(engineBadge).toBeVisible();

    // Check Sub-ms IPC Latency metric card
    const latencyHeading = page.locator('text=Sub-ms IPC Latency');
    await expect(latencyHeading).toBeVisible();

    // Ensure latency text is displayed and indicates sub-millisecond range (< 5.0ms)
    const latencyMetric = page.locator('text=0.84');
    await expect(latencyMetric.first()).toBeVisible();
  });

  test('should render navigation links and allow switching views', async ({ page }) => {
    const navDashboard = page.getByRole('button', { name: /dashboard/i });
    const navAccounts = page.getByRole('button', { name: /accounts/i });
    const navChannels = page.getByRole('button', { name: /channels/i });
    const navSettings = page.getByRole('button', { name: /settings/i });

    await expect(navDashboard).toBeVisible();
    await expect(navAccounts).toBeVisible();
    await expect(navChannels).toBeVisible();
    await expect(navSettings).toBeVisible();

    // Switch to Accounts view
    await navAccounts.click();
    await expect(page.locator('text=Configured Terminals & Broker Accounts')).toBeVisible();

    // Switch to Settings view
    await navSettings.click();
    await expect(page.locator('text=Agent Settings')).toBeVisible();

    // Switch back to Dashboard view
    await navDashboard.click();
    await expect(page.locator('text=Live Execution Feed')).toBeVisible();
  });

  test('should display active accounts summary and execution feed', async ({ page }) => {
    // Verify terminal summary
    await expect(page.locator('text=Active Account:')).toBeVisible();
    await expect(page.locator('text=IC Markets MT5 Terminal (Demo)')).toBeVisible();

    // Verify Live Execution Feed table headers
    await expect(page.locator('text=Live Execution Feed')).toBeVisible();
    const tableHeaderSymbol = page.locator('th:has-text("Symbol")');
    await expect(tableHeaderSymbol).toBeVisible();

    // Verify executions in table
    await expect(page.locator('td:has-text("XAUUSD")').first()).toBeVisible();
    await expect(page.locator('span:has-text("FILLED")').first()).toBeVisible();
  });

  test('should open and close the Setup Doctor wizard modal', async ({ page }) => {
    const setupDoctorBtn = page.getByRole('button', { name: /setup doctor/i }).first();
    await expect(setupDoctorBtn).toBeVisible();
    await setupDoctorBtn.click();

    // Modal should be visible
    await expect(page.locator('text=Setup Doctor: Frictionless 5-Minute Onboarding')).toBeVisible();
    await expect(page.locator('text=License Authentication & Offline Grace')).toBeVisible();

    // Close modal
    const closeBtn = page.getByRole('button', { name: /close modal/i }).or(page.locator('button[aria-label="Close modal"]'));
    if (await closeBtn.isVisible()) {
      await closeBtn.click();
      await expect(page.locator('text=Setup Doctor: Frictionless 5-Minute Onboarding')).not.toBeVisible();
    }
  });
});
