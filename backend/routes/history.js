import express from 'express';
import { getImportHistory, downloadFailedRows } from '../controllers/historyController.js';

const router = express.Router();

router.get('/', getImportHistory);
router.get('/:jobId/failed-rows', downloadFailedRows);

export default router;
