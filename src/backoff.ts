export interface BackoffOptions {
  baseDelayMs: number;
  maxDelayMs: number;
}

/** Exponential backoff: base * 2^(attempt - 1), capped. `attempt` starts at 1. */
export function retryDelay(attempt: number, { baseDelayMs, maxDelayMs }: BackoffOptions): number {
  return Math.min(maxDelayMs, baseDelayMs * 2 ** Math.max(0, attempt - 1));
}
