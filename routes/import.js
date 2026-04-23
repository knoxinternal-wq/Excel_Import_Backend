import express from 'express';
import {
  uploadMiddleware,
  uploadFile,
  getStatus,
  cancelImport,
  resumeImport,
  downloadTemplate,
} from '../controllers/importController.js';
import { requireAuthSession } from '../middleware/authenticateSession.js';
import { requireAdmin } from '../middleware/requireAdmin.js';

const router = express.Router();

router.use(requireAuthSession);

router.get('/template', requireAdmin, downloadTemplate);
router.post('/', requireAdmin, (req, res, next) => {
  uploadMiddleware(req, res, (err) => {
    if (err) {
      return res.status(400).json({ error: err.message || 'File upload failed' });
    }
    uploadFile(req, res).catch((e) => res.status(500).json({ error: e.message }));
  });
});
router.get('/status/:jobId', getStatus);
router.post('/cancel/:jobId', requireAdmin, cancelImport);
router.post('/resume/:jobId', requireAdmin, resumeImport);

export default router;
