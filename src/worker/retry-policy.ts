export type FailureDecision =
  | { action: 'retry'; delayMs: number }
  | { action: 'dead-letter'; reason: 'permanent-error' | 'max-attempts' };

/**
 * After a failed send, decide: try again later, or give up?
 *
 * `attempts` is how many times we've tried so far, including the one that just failed.
 * With retryDelaysMs = [10s, 1m, 5m]:
 *   attempt 1 fails → retry in 10s
 *   attempt 2 fails → retry in 1m
 *   attempt 3 fails → retry in 5m
 *   attempt 4 fails → dead-letter queue
 *
 * The delays grow (exponential backoff) so a struggling provider gets time to recover,
 * instead of being hammered by retries.
 */
export function decideOnFailure(attempts: number, retryDelaysMs: number[], permanent: boolean): FailureDecision {
  if (permanent) return { action: 'dead-letter', reason: 'permanent-error' };
  if (attempts > retryDelaysMs.length) return { action: 'dead-letter', reason: 'max-attempts' };
  return { action: 'retry', delayMs: retryDelaysMs[attempts - 1] };
}
