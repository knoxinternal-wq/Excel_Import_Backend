import express from 'express';
import { getImportHistory, downloadFailedRows } from '../controllers/historyController.js';
import { requireAuthSession } from '../middleware/authenticateSession.js';

const router = express.Router();
router.use(requireAuthSession);

router.get('/', getImportHistory);
router.get('/:jobId/failed-rows', downloadFailedRows);

export default router;
