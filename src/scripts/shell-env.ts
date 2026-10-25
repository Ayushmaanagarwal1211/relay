/**
 * A copy of the environment variables set in the shell, taken BEFORE src/shared/config.ts
 * loads .env into process.env.
 *
 * The stress test needs it: .env is made for `npm run dev` (e.g. WORKER_PREFETCH=10), and
 * without this copy those values would silently replace the stress test's own settings
 * (e.g. WORKER_PREFETCH=100). Only what you type in the shell should override them.
 *
 * Must be imported before anything that imports config.
 */
export const shellEnv: NodeJS.ProcessEnv = { ...process.env };
