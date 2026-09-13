import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    projects: [
      {
        test: {
          name: 'unit',
          include: ['tests/unit/**/*.test.ts'],
          environment: 'node',
          testTimeout: 20_000,
        },
      },
      {
        test: {
          name: 'property',
          include: ['tests/property/**/*.test.ts'],
          environment: 'node',
          testTimeout: 120_000,
        },
      },
      {
        test: {
          name: 'integration',
          include: ['tests/integration/**/*.test.ts'],
          environment: 'node',
          testTimeout: 240_000,
          hookTimeout: 120_000,
          // Each file spawns its own disposable local Technocore server; keep CPU contention sane
          // so timing assertions stay meaningful.
          maxWorkers: 4,
          pool: 'forks',
        },
      },
    ],
  },
});
