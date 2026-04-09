import { useCallback, useEffect, useRef } from 'react';
import usePersistentState from './hooks/usePersistentState';
import Dashboard from './pages/Dashboard';
import Login from './pages/Login';

const STORAGE_KEY = 'auth_session';
const INACTIVITY_TIMEOUT_MS = 15 * 60 * 1000;
const ACTIVITY_EVENTS = ['mousemove', 'mousedown', 'keydown', 'scroll', 'touchstart', 'click'];
const BACK_GUARD_STATE = { appBackGuard: true };

function App() {
  const [session, setSession] = usePersistentState(
    STORAGE_KEY,
    null,
    (v) => !v || (typeof v === 'object' && v !== null),
  );
  const inactivityTimerRef = useRef(null);

  const handleLogout = useCallback(() => {
    if (inactivityTimerRef.current) {
      clearTimeout(inactivityTimerRef.current);
      inactivityTimerRef.current = null;
    }
    setSession(null);
  }, [setSession]);

  const resetInactivityTimer = useCallback(() => {
    if (!session?.token) return;
    if (inactivityTimerRef.current) clearTimeout(inactivityTimerRef.current);
    inactivityTimerRef.current = setTimeout(() => {
      setSession(null);
      inactivityTimerRef.current = null;
    }, INACTIVITY_TIMEOUT_MS);
  }, [session?.token, setSession]);

  const handleLoginSuccess = (authData) => {
    const next = {
      token: authData?.token,
      user: authData?.user || null,
    };
    setSession(next);
  };

  useEffect(() => {
    if (!session?.token) {
      if (inactivityTimerRef.current) {
        clearTimeout(inactivityTimerRef.current);
        inactivityTimerRef.current = null;
      }
      return undefined;
    }

    resetInactivityTimer();
    const handleActivity = () => resetInactivityTimer();
    for (const eventName of ACTIVITY_EVENTS) {
      window.addEventListener(eventName, handleActivity, { passive: true });
    }

    return () => {
      for (const eventName of ACTIVITY_EVENTS) {
        window.removeEventListener(eventName, handleActivity);
      }
      if (inactivityTimerRef.current) {
        clearTimeout(inactivityTimerRef.current);
        inactivityTimerRef.current = null;
      }
    };
  }, [session?.token, resetInactivityTimer]);

  useEffect(() => {
    if (!session?.token) return undefined;

    window.history.pushState(BACK_GUARD_STATE, '', window.location.href);
    const handlePopState = () => {
      window.history.pushState(BACK_GUARD_STATE, '', window.location.href);
    };

    window.addEventListener('popstate', handlePopState);
    return () => {
      window.removeEventListener('popstate', handlePopState);
    };
  }, [session?.token]);

  if (!session?.token) {
    return <Login onLoginSuccess={handleLoginSuccess} />;
  }

  return <Dashboard user={session.user} onLogout={handleLogout} />;
}

export default App;
