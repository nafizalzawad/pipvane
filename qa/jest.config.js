/** @type {import('ts-jest').JestConfigWithTsJest} */
module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  testMatch: [
    '<rootDir>/tests/unit/**/*.test.ts',
    '<rootDir>/tests/unit/**/*.spec.ts',
    '<rootDir>/tests/integration/**/*.test.ts'
  ],
  testPathIgnorePatterns: [
    '/node_modules/',
    '/tests/e2e/'
  ],
  moduleFileExtensions: ['ts', 'tsx', 'js', 'jsx', 'json', 'node'],
  transform: {
    '^.+\\.tsx?$': [
      'ts-jest',
      {
        tsconfig: {
          target: 'es2022',
          module: 'commonjs',
          strict: true,
          esModuleInterop: true,
          skipLibCheck: true
        }
      }
    ]
  },
  verbose: true,
  collectCoverageFrom: [
    'src/**/*.{ts,tsx}',
    '!**/node_modules/**'
  ],
  coverageDirectory: './coverage',
  coverageReporters: ['text', 'lcov', 'json']
};
