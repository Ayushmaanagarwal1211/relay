/** @type {import('jest').Config} */
const tsJest = { '^.+\\.ts$': ['ts-jest', { tsconfig: 'tsconfig.json' }] };

module.exports = {
  projects: [
    {
      displayName: 'unit',
      testEnvironment: 'node',
      transform: tsJest,
      testMatch: ['<rootDir>/tests/unit/**/*.test.ts'],
    },
    {
      // Needs Postgres running (npm run infra:up). Uses the separate relay_test database.
      displayName: 'integration',
      testEnvironment: 'node',
      transform: tsJest,
      testMatch: ['<rootDir>/tests/integration/**/*.test.ts'],
      globalSetup: '<rootDir>/tests/integration/global-setup.ts',
    },
  ],
};
