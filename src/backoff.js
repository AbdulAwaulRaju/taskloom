/**
 * Exponential backoff with "equal jitter":
 *   ceiling = min(maxMs, baseMs * factor^(attempt-1))
 *   delay   = ceiling/2 + random(0, ceiling/2)
 *
 * Jitter spreads retries out so a burst of failures does not come back as a
 * synchronized thundering herd; keeping half the ceiling guarantees a minimum wait.
 *
 * @param {number} attempt  number of attempts already made (>= 1)
 */
export function backoffDelay(attempt, { baseMs = 500, maxMs = 60_000, factor = 2, rng = Math.random } = {}) {
  const exponent = Math.max(0, attempt - 1);
  const ceiling = Math.min(maxMs, baseMs * factor ** exponent);
  return Math.floor(ceiling / 2 + rng() * (ceiling / 2));
}
