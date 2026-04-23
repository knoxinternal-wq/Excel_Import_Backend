import express from 'express';
import {
  getData,
  getStates,
  getFilterOptions,
  previewDeleteByDateRange,
  deleteByDateRange,
} from '../controllers/dataController.js';
import { getReportMeta } from '../controllers/reportController.js';
import {
  getPivotFieldsHandler,
  getPivotCapabilitiesHandler,
  getPivotFilterValuesHandler,
  getPivotFilterValuesBatchHandler,
  getPivotDataHandler,
  getPivotQuickHandler,
  getPivotDrilldownHandler,
  exportPivotHandler,
} from '../controllers/pivotController.js';
import { requireAuthSession } from '../middleware/authenticateSession.js';
import { requireAdmin } from '../middleware/requireAdmin.js';

const router = express.Router();
router.use(requireAuthSession);

function cacheControl(v) {
  return (req, res, next) => {
    res.set('Cache-Control', v);
    next();
  };
}

router.get('/', cacheControl('private,no-store'), getData);
router.get('/states', cacheControl('public,max-age=300'), getStates);
router.get('/filter-options', getFilterOptions);
router.post('/delete-range/preview', requireAdmin, previewDeleteByDateRange);
router.delete('/delete-range', requireAdmin, deleteByDateRange);
router.get('/report/meta', getReportMeta);
router.get('/report/fields', cacheControl('public,max-age=3600'), getPivotFieldsHandler);
router.get('/report/capabilities', cacheControl('public,max-age=120'), getPivotCapabilitiesHandler);
router.get('/report/filter-values', cacheControl('public,max-age=300,stale-while-revalidate=60'), getPivotFilterValuesHandler);
router.post('/report/filter-values-batch', getPivotFilterValuesBatchHandler);
router.post('/report/pivot', getPivotDataHandler);
router.get('/pivot', getPivotQuickHandler);
router.post('/report/drilldown', getPivotDrilldownHandler);
router.post('/report/export', exportPivotHandler);

export default router;
