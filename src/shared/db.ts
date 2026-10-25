import { Pool, PoolClient } from 'pg';

export function createPool(connectionString: string, max = 10): Pool {
  return new Pool({ connectionString, max });
}

// Runs `fn` inside BEGIN ... COMMIT. Any error rolls everything back,
// so either all of fn's writes are saved or none are.
export async function withTransaction<T>(pool: Pool, fn: (client: PoolClient) => Promise<T>): Promise<T> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}
