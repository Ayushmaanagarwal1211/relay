// Minimal levelled logger. During load tests set LOG_LEVEL=warn (or error) so that
// thousands of "sent" lines don't flood the terminal and slow the processes down.
const LEVELS = { info: 0, warn: 1, error: 2 } as const;
type Level = keyof typeof LEVELS;

const configured = (process.env.LOG_LEVEL ?? 'info') as Level;
const threshold = LEVELS[configured] ?? LEVELS.info;

export const log = {
  info: (...args: unknown[]) => threshold <= LEVELS.info && console.log(...args),
  warn: (...args: unknown[]) => threshold <= LEVELS.warn && console.warn(...args),
  error: (...args: unknown[]) => console.error(...args),
};
