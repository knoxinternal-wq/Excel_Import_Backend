import { useState } from 'react';
import { Loader2, Mail, Lock, Eye, EyeOff, LogIn } from 'lucide-react';
import { authApi } from '../services/api';
import { formatRequestError } from '../utils/requestError';

export default function Login({ onLoginSuccess }) {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [showPassword, setShowPassword] = useState(false);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');
    setLoading(true);
    try {
      const { data } = await authApi.login({ email, password });
      onLoginSuccess?.(data);
    } catch (err) {
      setError(formatRequestError(err, 'Login failed'));
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="relative min-h-screen bg-gradient-to-b from-sky-300 via-sky-100 to-slate-100 flex items-center justify-center p-4 overflow-hidden">
      <div className="pointer-events-none absolute -bottom-20 left-1/2 -translate-x-1/2 w-[1200px] h-[420px] bg-white/50 blur-3xl rounded-full" aria-hidden />
      <div className="pointer-events-none absolute top-[16%] left-[8%] w-20 h-20 rounded-full bg-white/50 blur-2xl" aria-hidden />
      <div className="pointer-events-none absolute top-[26%] right-[10%] w-16 h-16 rounded-full bg-white/40 blur-2xl" aria-hidden />

      <div className="w-full max-w-md bg-white/60 backdrop-blur-md rounded-3xl border border-white/60 shadow-[0_20px_40px_rgba(15,23,42,0.12)]">
        <div className="px-8 pt-8 pb-6">
          <div className="mx-auto mb-5 w-12 h-12 rounded-2xl bg-white/90 border border-slate-200 flex items-center justify-center shadow-sm">
            <LogIn className="w-5 h-5 text-slate-700" />
          </div>
          <h1 className="text-4xl font-bold text-slate-900 text-center tracking-tight">Sign in with email</h1>
          <p className="text-center text-slate-600 mt-3 leading-6">
            Excel Import System
          </p>
        </div>

        <form onSubmit={handleSubmit} className="px-8 pb-8 space-y-3">
          <div>
            <label htmlFor="email" className="sr-only">Email</label>
            <div className="relative">
              <Mail className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" aria-hidden />
              <input
                id="email"
                type="email"
                required
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                className="w-full h-12 border border-slate-200/80 rounded-xl pl-10 pr-3 text-sm bg-slate-100/75 focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-400 transition"
                placeholder="Email"
                autoComplete="email"
              />
            </div>
          </div>
          <div>
            <label htmlFor="password" className="sr-only">Password</label>
            <div className="relative">
              <Lock className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" aria-hidden />
              <input
                id="password"
                type={showPassword ? 'text' : 'password'}
                required
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                className="w-full h-12 border border-slate-200/80 rounded-xl pl-10 pr-10 text-sm bg-slate-100/75 focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-400 transition"
                placeholder="Password"
                autoComplete="current-password"
              />
              <button
                type="button"
                onClick={() => setShowPassword((v) => !v)}
                className="absolute right-2 top-1/2 -translate-y-1/2 h-8 w-8 inline-flex items-center justify-center rounded-md text-slate-500 hover:text-slate-700 hover:bg-slate-200/70"
                aria-label={showPassword ? 'Hide password' : 'Show password'}
              >
                {showPassword ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
              </button>
            </div>
          </div>

          {error && (
            <div className="text-sm text-red-700 bg-red-50 border border-red-200 rounded-lg px-3 py-2.5 mt-1">
              {error}
            </div>
          )}

          <button
            type="submit"
            disabled={loading}
            className="w-full h-12 inline-flex items-center justify-center gap-2 rounded-xl bg-gradient-to-b from-slate-800 to-slate-900 text-white text-lg font-medium tracking-tight hover:from-slate-700 hover:to-slate-900 disabled:opacity-60"
          >
            {loading ? (
              <>
                <Loader2 className="w-4 h-4 animate-spin" />
                Signing in...
              </>
            ) : (
              'Sign in'
            )}
          </button>
        </form>
      </div>
    </div>
  );
}

