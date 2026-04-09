import { useEffect, useState } from 'react';

export default function usePersistentState(key, initialValue, isValidValue) {
  const [value, setValue] = useState(() => {
    try {
      const raw = localStorage.getItem(key);
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
      localStorage.setItem(key, JSON.stringify(value));
    } catch {
      // Ignore storage write errors.
    }
  }, [key, value]);

  return [value, setValue];
}
