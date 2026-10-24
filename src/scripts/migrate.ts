import { config } from '../shared/config';
import { createPool } from '../shared/db';
import { runMigrations } from '../shared/migrations';

async function main() {
  const pool = createPool(config.DATABASE_URL);
  try {
    const applied = await runMigrations(pool);
    console.log(applied.length ? `Applied: ${applied.join(', ')}` : 'Database is up to date.');
  } finally {
    await pool.end();
  }
}

main().catch((err) => {
  console.error('Migration failed:', err);
  process.exit(1);
});
