// Integration tests use their own database (relay_test) so they never wipe your dev data.
export function testDatabaseUrl(): string {
  try {
    process.loadEnvFile();
  } catch {
    // No .env: fall back to the docker-compose default below.
  }
  return process.env.TEST_DATABASE_URL ?? 'postgres://relay:relay@localhost:5432/relay_test';
}
