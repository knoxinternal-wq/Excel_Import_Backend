import axios from 'axios';
import {
  MAX_UI_DATA_LOAD_MS,
  PIVOT_FILTER_VALUES_TIMEOUT_MS,
  LONG_RUNNING_REQUEST_MS,
} from '../constants/timing';

const API_BASE = (() => {
  const backendUrl = String(import.meta.env?.VITE_BACKEND_URL || '').trim();
  if (!backendUrl) return '/api';
  return `${backendUrl.replace(/\/+$/, '')}/api`;
})();

axios.interceptors.request.use((config) => {
  try {
    const raw = sessionStorage.getItem('auth_session');
    if (raw) {
      const s = JSON.parse(raw);
      if (s?.token) {
        const headers = config.headers || {};
        headers.Authorization = `Bearer ${s.token}`;
        config.headers = headers;
      }
    }
  } catch {
    /* ignore */
  }
  return config;
});

const inflight = new Map();
async function dedup(key, fn) {
  if (inflight.has(key)) return inflight.get(key);
  const p = fn().finally(() => inflight.delete(key));
  inflight.set(key, p);
  return p;
}

const FILTER_VALUES_TTL_MS = 30 * 60 * 1000;
const filterValuesCache = new Map();
const filterValuesInflight = new Map();

const fastOpts = () => ({ timeout: MAX_UI_DATA_LOAD_MS });
const longOpts = () => ({ timeout: LONG_RUNNING_REQUEST_MS });

/**
 * Synchronous read of cached filter distinct values (same key as filterValues).
 * Lets the UI paint options immediately on repeat visits without waiting on the network.
 */
export function peekPivotFilterValuesCache(field, search = '', limit = 10_000) {
  const f = String(field || '').trim();
  if (!f) return null;
  const searchRaw = String(search || '').trim();
  const lim = limit ?? 10_000;
  const cacheKey = JSON.stringify({ field: f, search: searchRaw.toLowerCase(), limit: lim });
  const cached = filterValuesCache.get(cacheKey);
  if (cached && Date.now() - cached.ts < FILTER_VALUES_TTL_MS) {
    return cached.data;
  }
  return null;
}

export const importApi = {
  upload: (formData, onUploadProgress, signal) =>
    axios.post(`${API_BASE}/import`, formData, {
      signal,
      ...longOpts(),
      maxBodyLength: Infinity,
      maxContentLength: Infinity,
      onUploadProgress: onUploadProgress
        ? (e) => {
            const pct = e.total ? Math.round((e.loaded / e.total) * 100) : 0;
            onUploadProgress(pct);
          }
        : undefined,
    }),
  getStatus: (jobId, config) =>
    axios.get(`${API_BASE}/import/status/${jobId}`, {
      ...longOpts(),
      ...(config || {}),
    }),
  cancel: (jobId) =>
    axios.post(`${API_BASE}/import/cancel/${jobId}`, null, longOpts()),
};

export const dataApi = {
  fetch: (params, config) =>
    axios.get(`${API_BASE}/data`, {
      ...fastOpts(),
      params,
      ...(config || {}),
    }),
  getStates: () => axios.get(`${API_BASE}/data/states`, fastOpts()),
  getFilterOptions: (config) =>
    axios.get(`${API_BASE}/data/filter-options`, { ...fastOpts(), ...(config || {}) }),
  previewDeleteRange: (body, config) =>
    axios.post(`${API_BASE}/data/delete-range/preview`, body, { ...fastOpts(), ...(config || {}) }),
  deleteByDateRange: (body, config) =>
    axios.delete(`${API_BASE}/data/delete-range`, {
      ...longOpts(),
      data: body,
      ...(config || {}),
    }),
};

export const reportApi = {
  meta: () => axios.get(`${API_BASE}/data/report/meta`, fastOpts()),
};

export const pivotApi = {
  quick: (params = {}, requestConfig) =>
    axios.get(`${API_BASE}/data/pivot`, {
      params,
      ...longOpts(),
      ...(requestConfig || {}),
    }),
  fields: () => axios.get(`${API_BASE}/data/report/fields`, fastOpts()),
  filterValues: async (params = {}) => {
    const field = String(params?.field || '').trim();
    const searchRaw = String(params?.search || '').trim();
    const limit = params?.limit ?? 10000;
    const cacheKey = JSON.stringify({ field, search: searchRaw.toLowerCase(), limit });
    const now = Date.now();

    const cached = filterValuesCache.get(cacheKey);
    if (cached && now - cached.ts < FILTER_VALUES_TTL_MS) {
      return { data: cached.data };
    }

    if (filterValuesInflight.has(cacheKey)) {
      return filterValuesInflight.get(cacheKey);
    }

    const req = axios
      .get(`${API_BASE}/data/report/filter-values`, {
        params: { field, search: searchRaw, limit },
        timeout: PIVOT_FILTER_VALUES_TIMEOUT_MS,
      })
      .then((res) => {
        filterValuesCache.set(cacheKey, { ts: Date.now(), data: res.data });
        return res;
      })
      .finally(() => {
        filterValuesInflight.delete(cacheKey);
      });

    filterValuesInflight.set(cacheKey, req);
    return req;
  },
  /**
   * Loads distinct values for many pivot filter fields in one HTTP round-trip.
   * Fills the same per-field cache keys as `filterValues` (empty search).
   */
  filterValuesBatch: async ({ fields = [], limit = 500 } = {}) => {
    const list = [...new Set(fields.map((f) => String(f || '').trim()).filter(Boolean))].sort();
    const lim = Math.min(2000, Math.max(50, Number(limit) || 500));
    const cacheKey = JSON.stringify({ batch: true, fields: list, limit: lim });
    const now = Date.now();

    if (list.length === 0) {
      return { data: { fields: {} } };
    }

    const allFromCache = list.every((f) => {
      const k = JSON.stringify({ field: f, search: '', limit: lim });
      const c = filterValuesCache.get(k);
      return c && now - c.ts < FILTER_VALUES_TTL_MS && Array.isArray(c.data?.values);
    });
    if (allFromCache) {
      const fieldsOut = {};
      for (const f of list) {
        const k = JSON.stringify({ field: f, search: '', limit: lim });
        fieldsOut[f] = { values: filterValuesCache.get(k).data.values };
      }
      return { data: { fields: fieldsOut } };
    }

    if (filterValuesInflight.has(cacheKey)) {
      return filterValuesInflight.get(cacheKey);
    }

    const req = axios
      .post(
        `${API_BASE}/data/report/filter-values-batch`,
        { fields: list, limit: lim },
        { timeout: PIVOT_FILTER_VALUES_TIMEOUT_MS },
      )
      .then((res) => {
        const bag = res.data?.fields && typeof res.data.fields === 'object' ? res.data.fields : {};
        const ts = Date.now();
        for (const f of list) {
          const entry = bag[f];
          if (entry && !entry.error && Array.isArray(entry.values)) {
            const k = JSON.stringify({ field: f, search: '', limit: lim });
            filterValuesCache.set(k, { ts, data: { field: f, values: entry.values } });
          }
        }
        return res;
      })
      .finally(() => {
        filterValuesInflight.delete(cacheKey);
      });

    filterValuesInflight.set(cacheKey, req);
    return req;
  },
  run: (config, requestConfig) => {
    const key = `pivot:${JSON.stringify(config)}`;
    return dedup(key, () =>
      axios.post(`${API_BASE}/data/report/pivot`, config, {
        ...longOpts(),
        ...(requestConfig || {}),
      }),
    );
  },
  drilldown: (payload, requestConfig) =>
    axios.post(`${API_BASE}/data/report/drilldown`, payload, {
      ...longOpts(),
      ...(requestConfig || {}),
    }),
  export: (payload, requestConfig) =>
    axios.post(`${API_BASE}/data/report/export`, payload, {
      responseType: 'blob',
      ...longOpts(),
      ...(requestConfig || {}),
    }),
};

export const historyApi = {
  list: (limit = 20) =>
    axios.get(`${API_BASE}/history`, { params: { limit }, ...fastOpts() }),
  getFailedRows: (jobId) =>
    axios.get(`${API_BASE}/history/${jobId}/failed-rows`, {
      params: { format: 'json' },
      ...longOpts(),
    }),
  downloadFailed: (jobId) =>
    axios.get(`${API_BASE}/history/${jobId}/failed-rows`, {
      responseType: 'blob',
      ...longOpts(),
    }),
};

export const authApi = {
  login: (payload) => axios.post(`${API_BASE}/auth/login`, payload, fastOpts()),
};

export const adminApi = {
  importSoMaster: (formData) =>
    axios.post(`${API_BASE}/admin/import-so-master`, formData, {
      ...longOpts(),
      maxBodyLength: Infinity,
      maxContentLength: Infinity,
    }),
  previewSoMaster: (params = {}, config) =>
    axios.get(`${API_BASE}/admin/so-master-preview`, {
      ...fastOpts(),
      params: { ...params },
      ...(config || {}),
    }),
  soMasterHistory: (limit = 20) =>
    axios.get(`${API_BASE}/admin/so-master-history`, { params: { limit }, ...fastOpts() }),
  soMasterEditHistory: ({ brand, fy, limit = 20 } = {}) =>
    axios.get(`${API_BASE}/admin/so-master-edit-history`, {
      params: { brand, fy, limit },
      ...fastOpts(),
    }),
  editSoMasterRow: (payload) =>
    axios.post(`${API_BASE}/admin/edit-so-master-row`, payload, fastOpts()),
  masterTableOptions: () => axios.get(`${API_BASE}/admin/master-table-options`, fastOpts()),
  masterTablePreview: ({ table, q = '', limit = 200 } = {}) =>
    axios.get(`${API_BASE}/admin/master-table-preview`, {
      params: { table, q, limit },
      ...fastOpts(),
    }),
  masterTableEditHistory: ({ table, limit = 100 } = {}) =>
    axios.get(`${API_BASE}/admin/master-table-edit-history`, {
      params: { table, limit },
      ...fastOpts(),
    }),
  editMasterTableRow: (payload) =>
    axios.post(`${API_BASE}/admin/edit-master-table-row`, payload, fastOpts()),
};
