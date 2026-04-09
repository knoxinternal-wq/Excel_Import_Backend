import express from 'express';
import {
  uploadMiddleware,
  uploadFile,
  getStatus,
  cancelImport,
  resumeImport,
  downloadTemplate,
} from '../controllers/importController.js';

const router = express.Router();

router.get('/template', downloadTemplate);
router.post('/', (req, res, next) => {
  uploadMiddleware(req, res, (err) => {
    if (err) {
      return res.status(400).json({ error: err.message || 'File upload failed' });
    }
    uploadFile(req, res).catch((e) => res.status(500).json({ error: e.message }));
  });
});
router.get('/status/:jobId', getStatus);
router.post('/cancel/:jobId', cancelImport);
router.post('/resume/:jobId', resumeImport);

export default router;
