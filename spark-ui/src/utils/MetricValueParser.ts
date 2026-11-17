// Parses the human formatted values Spark (SQL metrics) and spark-rapids (task accumulators)
// put into the REST API back into numbers.
//
// SQL metrics (SQLMetrics.stringValue):
//   "total (min, med, max (stageId: taskId))\n6.0 s (201 ms, 249 ms, 309 ms (stage 8.0: task 21))"
//   "37 ms", "4.0 m", "1.1 h"            -> milliseconds
//   "14.7 MiB", "158.0 B"                -> bytes
//   "730,806,711"                        -> count
// RAPIDS task accumulators (GpuTaskMetrics):
//   "00:04:41.581"                       -> milliseconds
//   "49.72GB (53389098783 bytes)"        -> bytes (the exact number in brackets)

const DURATION_UNITS_MS: Record<string, number> = {
  ns: 1e-6,
  us: 1e-3,
  ms: 1,
  s: 1000,
  m: 60 * 1000,
  min: 60 * 1000,
  h: 3600 * 1000,
};

const SIZE_UNITS: Record<string, number> = {
  b: 1,
  kib: 1024,
  mib: 1024 ** 2,
  gib: 1024 ** 3,
  tib: 1024 ** 4,
  pib: 1024 ** 5,
  kb: 1024,
  mb: 1024 ** 2,
  gb: 1024 ** 3,
  tb: 1024 ** 4,
};

/** Returns the "total" part of a SQL metric value (the first number of the second line). */
export function metricTotalText(value: string): string {
  const lines = value.split("\n");
  const text = lines.length >= 2 ? lines[1] : lines[0];
  const bracket = text.indexOf("(");
  return (bracket >= 0 ? text.slice(0, bracket) : text).trim();
}

/** "6.0 s" -> 6000, "37 ms" -> 37, "00:04:41.581" -> 281581; undefined if not a duration. */
export function parseDurationMs(text: string): number | undefined {
  const t = text.trim();
  const clock = t.match(/^(?:(\d+)\s*days?,?\s*)?(\d+):(\d{2}):(\d{2}(?:\.\d+)?)$/);
  if (clock) {
    const days = clock[1] ? parseInt(clock[1], 10) : 0;
    return (
      ((days * 24 + parseInt(clock[2], 10)) * 60 + parseInt(clock[3], 10)) *
        60000 +
      parseFloat(clock[4]) * 1000
    );
  }
  const m = t.replace(/,/g, "").match(/^(-?\d+(?:\.\d+)?)\s*(ns|us|ms|s|min|m|h)$/);
  if (!m) return undefined;
  return parseFloat(m[1]) * DURATION_UNITS_MS[m[2]];
}

/** "14.7 MiB" -> bytes, "49.72GB (53389098783 bytes)" -> 53389098783. */
export function parseSizeBytes(text: string): number | undefined {
  const t = text.trim();
  const exact = t.match(/\((\d+)\s*bytes\)/);
  if (exact) return parseInt(exact[1], 10);
  const m = t.replace(/,/g, "").match(/^(-?\d+(?:\.\d+)?)\s*([KMGTP]?i?B)$/i);
  if (!m) return undefined;
  const unit = SIZE_UNITS[m[2].toLowerCase()];
  return unit === undefined ? undefined : parseFloat(m[1]) * unit;
}

/** "730,806,711" -> 730806711 */
export function parseCount(text: string): number | undefined {
  const t = text.trim().replace(/,/g, "");
  if (!/^-?\d+(\.\d+)?$/.test(t)) return undefined;
  return parseFloat(t);
}

/** Parses any of the formats above; durations are ms, sizes are bytes. */
export function parseMetricNumber(value: string | undefined | null): number | undefined {
  if (value === undefined || value === null) return undefined;
  // accumulator sizes carry the exact byte count: "49.72GB (53389098783 bytes)"
  const exact = String(value).trim().match(/^[\d.,]+\s*[KMGTP]?i?B\s*\((\d+)\s*bytes\)$/i);
  if (exact) return parseInt(exact[1], 10);
  const total = metricTotalText(String(value));
  return parseDurationMs(total) ?? parseSizeBytes(total) ?? parseCount(total);
}
