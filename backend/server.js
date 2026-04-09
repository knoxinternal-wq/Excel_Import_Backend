import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import compression from 'compression';
import path from 'path';
import { fileURLToPath } from 'url';

import importRoutes from './routes/import.js';
import dataRoutes from './routes/data.js';
import historyRoutes from './routes/history.js';
import authRoutes from './routes/auth.js';
import adminRoutes from './routes/admin.js';
import { endPgPool } from './config/database.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 5001;

app.use(cors());
app.use(compression({ threshold: 1024 }));
app.use(express.json({ limit: '2mb' }));

// Lightweight in-memory rate limit for API abuse protection.
const RATE_LIMIT_WINDOW_MS = Number(process.env.API_RATE_WINDOW_MS) > 0
  ? Number(process.env.API_RATE_WINDOW_MS)
  : 60_000;
const RATE_LIMIT_MAX = Number(process.env.API_RATE_MAX) > 0
  ? Number(process.env.API_RATE_MAX)
  : 300;
const rateBuckets = new Map();
app.use((req, res, next) => {
  const key = req.ip || req.socket?.remoteAddress || 'unknown';
  const now = Date.now();
  const bucket = rateBuckets.get(key);
  if (!bucket || now - bucket.ts > RATE_LIMIT_WINDOW_MS) {
    rateBuckets.set(key, { ts: now, count: 1 });
    return next();
  }
  bucket.count += 1;
  if (bucket.count > RATE_LIMIT_MAX) {
    return res.status(429).json({ error: 'Too many requests' });
  }
  return next();
});

app.use('/api/import', importRoutes);
app.use('/api/data', dataRoutes);
app.use('/api/history', historyRoutes);
app.use('/api/auth', authRoutes);
app.use('/api/admin', adminRoutes);

app.use((err, req, res, next) => {
  if (res.headersSent) return next(err);
  return res.status(500).json({ error: 'Internal server error' });
});

if (process.env.NODE_ENV === 'production') {
  app.use(express.static(path.join(__dirname, '../frontend/dist')));
  app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, '../frontend/dist/index.html'));
  });
}

const server = app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
  if (String(process.env.PIVOT_PRELOAD_FILTER_CACHE || '').trim() === '1') {
    import('./services/pivotService.js')
      .then((m) => m.preloadCommonPivotFilterCaches?.())
      .catch(() => {});
  }
});

function shutdown() {
  endPgPool()
    .catch(() => {})
    .finally(() => {
      server.close(() => process.exit(0));
    });
}
process.once('SIGTERM', shutdown);
process.once('SIGINT', shutdown);
