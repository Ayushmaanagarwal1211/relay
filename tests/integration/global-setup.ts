import { createPool } from '../../src/shared/db';
import { runMigrations } from '../../src/shared/migrations';
import { testDatabaseUrl } from './test-db';

// Runs once before all integration tests: bring the test database's schema up to date.
export default async function globalSetup() {
  const pool = createPool(testDatabaseUrl());
  try {
    await runMigrations(pool);
  } finally {
    await pool.end();
  }
}
