import { defineConfig, devices } from '@playwright/test';
import * as path from 'path';

/**
 * Pipvane v3: Playwright End-to-End & UI Verification Configuration
 * Targets the local static dashboard build (frontend/out) on port 3000
 * and the Mock Engine API on port 8765.
 */

const PORT = process.env.PORT || 3000;
const BASE_URL = process.env.PLAYWRIGHT_BASE_URL || `http://localhost:${PORT}`;

export default defineConfig({
  testDir: './tests',
  testMatch: /.*\.spec\.(ts|js)/,
  timeout: 30 * 1000,
  expect: {
    timeout: 5000,
  },
  fullyParallel: false,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: 1,
  reporter: [
    ['list'],
    ['html', { outputFolder: 'playwright-report', open: 'never' }],
    ['json', { outputFile: 'playwright-report/test-results.json' }],
  ],
  use: {
    baseURL: BASE_URL,
    headless: true,
    trace: 'on-first-retry',
    screenshot: 'only-on-failure',
    video: 'retain-on-failure',
    viewport: { width: 1440, height: 900 },
    ignoreHTTPSErrors: true,
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
    {
      name: 'firefox',
      use: { ...devices['Desktop Firefox'] },
    },
    {
      name: 'webkit',
      use: { ...devices['Desktop Safari'] },
    },
  ],
  /* Run local web servers before starting tests: Mock Engine API + Static Dashboard */
  webServer: [
    {
      command: 'node mock_engine_server.js',
      port: 8765,
      reuseExistingServer: !process.env.CI,
      timeout: 30 * 1000,
    },
    {
      command: process.platform === 'win32'
        ? `python -m http.server ${PORT} --directory ../frontend/out`
        : `npx serve -s ../frontend/out -p ${PORT}`,
      url: BASE_URL,
      reuseExistingServer: !process.env.CI,
      timeout: 60 * 1000,
    },
  ],
});
