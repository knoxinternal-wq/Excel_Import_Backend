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
  getPivotFilterValuesHandler,
  getPivotFilterValuesBatchHandler,
  getPivotDataHandler,
  getPivotQuickHandler,
  getPivotDrilldownHandler,
  exportPivotHandler,
} from '../controllers/pivotController.js';

const router = express.Router();

router.get('/', getData);
router.get('/states', getStates);
router.get('/filter-options', getFilterOptions);
router.post('/delete-range/preview', previewDeleteByDateRange);
router.delete('/delete-range', deleteByDateRange);
router.get('/report/meta', getReportMeta);
router.get('/report/fields', getPivotFieldsHandler);
router.get('/report/filter-values', getPivotFilterValuesHandler);
router.post('/report/filter-values-batch', getPivotFilterValuesBatchHandler);
router.post('/report/pivot', getPivotDataHandler);
router.get('/pivot', getPivotQuickHandler);
router.post('/report/drilldown', getPivotDrilldownHandler);
router.post('/report/export', exportPivotHandler);

export default router;
