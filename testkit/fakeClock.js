/** A controllable clock for deterministic time-based tests. */
export function fakeClock(start = 1_000_000) {
  let now = start;
  const clock = () => now;
  clock.advance = (ms) => {
    now += ms;
  };
  return clock;
}
