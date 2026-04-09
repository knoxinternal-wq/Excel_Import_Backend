/**
 * Minimal structured logging. Hot paths should use debug() — disabled unless LOG_LEVEL=debug.
 */
const LEVEL = (process.env.LOG_LEVEL || 'info').toLowerCase();
const LEVELS = { error: 0, warn: 1, info: 2, debug: 3 };

function enabled(level) {
  return LEVELS[level] <= (LEVELS[LEVEL] ?? 2);
}

export function logError(scope, message, meta) {
  if (!enabled('error')) return;
  const line = meta != null ? { scope, message, ...meta } : { scope, message };
  console.error(JSON.stringify(line));
}

export function logWarn(scope, message, meta) {
  if (!enabled('warn')) return;
  const line = meta != null ? { scope, message, ...meta } : { scope, message };
  console.warn(JSON.stringify(line));
}

export function logInfo(scope, message, meta) {
  if (!enabled('info')) return;
  const line = meta != null ? { scope, message, ...meta } : { scope, message };
  console.info(JSON.stringify(line));
}

/** Import / pivot diagnostics only when LOG_LEVEL=debug */
export function logDebug(scope, message, meta) {
  if (!enabled('debug')) return;
  const line = meta != null ? { scope, message, ...meta } : { scope, message };
  console.debug(JSON.stringify(line));
}
