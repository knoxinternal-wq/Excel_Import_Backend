import { useEffect, useState } from 'react';

function storageForKey(key) {
  return key === 'auth_session' ? sessionStorage : localStorage;
}

export default function usePersistentState(key, initialValue, isValidValue) {
  const [value, setValue] = useState(() => {
    try {
      const raw = storageForKey(key).getItem(key);
      if (raw == null) return initialValue;
      const parsed = JSON.parse(raw);
      if (typeof isValidValue === 'function' && !isValidValue(parsed)) return initialValue;
      return parsed;
    } catch {
      return initialValue;
    }
  });

  useEffect(() => {
    try {
      storageForKey(key).setItem(key, JSON.stringify(value));
    } catch {
      // Ignore storage write errors.
    }
  }, [key, value]);

  return [value, setValue];
}
