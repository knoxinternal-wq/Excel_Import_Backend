import express from 'express';
import multer from 'multer';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';
import { requireAuthSession } from '../middleware/authenticateSession.js';
import { requireAdmin } from '../middleware/requireAdmin.js';
import {
  importSoMaster,
  previewSoMaster,
  soMasterHistory,
  editSoMasterRow,
  soMasterEditHistory,
  masterTableOptions,
  masterTablePreview,
  masterTableEditHistory,
  editMasterTableRow,
} from '../controllers/adminController.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const uploadsDir = path.join(__dirname, '../uploads');
if (!fs.existsSync(uploadsDir)) {
  fs.mkdirSync(uploadsDir, { recursive: true });
}

const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, uploadsDir);
  },
  filename: (req, file, cb) => {
    const unique = Date.now() + '-' + Math.round(Math.random() * 1e9);
    cb(null, `so-master-${unique}${path.extname(file.originalname)}`);
  },
});

const upload = multer({
  storage,
  limits: { fileSize: 50 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    const ext = path.extname(file.originalname).toLowerCase();
    if (ext !== '.xlsx' && ext !== '.xls') {
      return cb(new Error('Only Excel files (.xlsx, .xls) are allowed'));
    }
    cb(null, true);
  },
});

const router = express.Router();

router.use(requireAuthSession);
router.use(requireAdmin);

router.post('/import-so-master', (req, res, next) => {
  upload.single('file')(req, res, (err) => {
    if (err) {
      return res.status(400).json({ success: false, error: err.message || 'File upload failed' });
    }
    importSoMaster(req, res).catch((e) =>
      res.status(500).json({ success: false, error: e?.message || 'Import failed' }),
    );
  });
});

router.get('/so-master-preview', previewSoMaster);
router.get('/so-master-history', soMasterHistory);
router.get('/so-master-edit-history', soMasterEditHistory);
router.post('/edit-so-master-row', editSoMasterRow);
router.get('/master-table-options', masterTableOptions);
router.get('/master-table-preview', masterTablePreview);
router.get('/master-table-edit-history', masterTableEditHistory);
router.post('/edit-master-table-row', editMasterTableRow);

export default router;
