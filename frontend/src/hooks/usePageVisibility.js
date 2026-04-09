import { useState, useEffect } from 'react';

/** True when document is visible (tab focused). */
export default function usePageVisibility() {
  const [visible, setVisible] = useState(
    () => typeof document !== 'undefined' && !document.hidden,
  );

  useEffect(() => {
    const onChange = () => setVisible(!document.hidden);
    document.addEventListener('visibilitychange', onChange);
    return () => document.removeEventListener('visibilitychange', onChange);
  }, []);

  return visible;
}
