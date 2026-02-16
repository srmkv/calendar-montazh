// server.js
// smart-process + static index.html + mobile.html
// + reactive updates: SSE /api/stream
// + AUTH + RBAC: admin/operator/viewer + admin.html
// + manual_events.json, comments.json, done_status.json, geocode_cache.json, recl_seen.json, geocode_cache.json
// + 🚀 мгновенный /api/events: отдаём готовый snapshot из памяти (и с диска после рестарта)
// + 🛰️ обновление snapshot в фоне: /api/bitrix/hook + polling Bitrix (страховка)
// + ✅ перенос по drag&drop: PUT /api/assigned-date/:id (с сохранением в Bitrix + патч снапшота сразу)

const express = require('express');
const cors = require('cors');
const axios = require('axios');
const compression = require('compression');
const dotenv = require('dotenv');
const path = require('path');
const fs = require('fs/promises');
const http = require('http');
const https = require('https');
const session = require('express-session');
const crypto = require('crypto');
const { promisify } = require('util');

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json({ limit: '2mb' }));

// ✅ compression, но SSE (text/event-stream) не сжимаем
app.use(compression({
  filter: (req, res) => {
    const accept = String(req.headers.accept || '');
    if (accept.includes('text/event-stream')) return false;
    return compression.filter(req, res);
  }
}));

// ===================== AUTH CONFIG (bootstrap admin) =====================
const AUTH_USER = String(process.env.AUTH_USER || 'admin');
const AUTH_PASS = String(process.env.AUTH_PASS || '');
const AUTH_PASS_SHA256 = String(process.env.AUTH_PASS_SHA256 || '');
const SESSION_SECRET = String(process.env.SESSION_SECRET || 'change-me');

if (!SESSION_SECRET || SESSION_SECRET === 'change-me') {
  console.warn('WARN: SESSION_SECRET is not set (or is default). Set it in .env for real usage.');
}
if (!AUTH_PASS && !AUTH_PASS_SHA256) {
  console.warn('WARN: AUTH_PASS/AUTH_PASS_SHA256 not set. Defaulting AUTH_PASS=admin (set in .env!)');
}

// session middleware
app.use(session({
  name: 'cal_sess',
  secret: SESSION_SECRET,
  resave: false,
  saveUninitialized: false,
  cookie: {
    httpOnly: true,
    sameSite: 'lax',
    secure: false // поставь true если HTTPS/реверс-прокси
  }
}));

function sha256hex(s) {
  return crypto.createHash('sha256').update(String(s)).digest('hex');
}
function safeEqual(a, b) {
  const A = Buffer.from(String(a));
  const B = Buffer.from(String(b));
  if (A.length !== B.length) return false;
  return crypto.timingSafeEqual(A, B);
}

function wantsHtml(req) {
  const accept = String(req.headers.accept || '');
  return accept.includes('text/html');
}

function requireAuth(req, res, next) {
  if (req.session && req.session.user) return next();
  if (wantsHtml(req)) return res.redirect('/login');
  return res.status(401).json({ ok: false, error: 'unauthorized' });
}

// ===================== RBAC =====================
const ROLE_ADMIN = 'admin';
const ROLE_OPERATOR = 'operator';
const ROLE_VIEWER = 'viewer';
const ROLE_SET = new Set([ROLE_ADMIN, ROLE_OPERATOR, ROLE_VIEWER]);

function getRole(req) {
  return String(req.session?.user?.role || ROLE_VIEWER);
}
function requireRole(roles) {
  const allow = new Set((roles || []).map(String));
  return (req, res, next) => {
    const r = getRole(req);
    if (allow.has(r)) return next();
    return res.status(403).json({ ok: false, error: 'forbidden' });
  };
}
const requireEditor = requireRole([ROLE_ADMIN, ROLE_OPERATOR]);
const requireAdmin = requireRole([ROLE_ADMIN]);

// ===================== BITRIX + PORT =====================
const PORT = process.env.PORT || 3050;

function normalizeWebhookUrl(u) {
  const s = String(u || '').trim();
  if (!s) return '';
  return s.endsWith('/') ? s : (s + '/');
}

const BITRIX_WEBHOOK_URL = normalizeWebhookUrl(process.env.BITRIX_WEBHOOK_URL);
const ENTITY_TYPE_ID = Number(process.env.BITRIX_SMART_ENTITY_TYPE_ID) || 141;
const CATEGORY_ID = Number(process.env.BITRIX_CATEGORY_ID) || 14;

if (!BITRIX_WEBHOOK_URL) {
  console.error('ERROR: BITRIX_WEBHOOK_URL is empty. Put it into .env');
  process.exit(1);
}

// .env можно переопределить:
// BITRIX_ASSIGNED_FIELD_ID=ufCrm8_1747908559319
const BITRIX_ASSIGNED_FIELD_ID = String(process.env.BITRIX_ASSIGNED_FIELD_ID || 'ufCrm8_1747908559319').trim();

// ===================== axios keep-alive =====================
const axiosInst = axios.create({
  timeout: 20000,
  httpAgent: new http.Agent({ keepAlive: true }),
  httpsAgent: new https.Agent({ keepAlive: true })
});

async function bitrixPost(method, payload) {
  const { data } = await axiosInst.post(`${BITRIX_WEBHOOK_URL}${method}.json`, payload, { timeout: 20000 });
  return data;
}

// ===================== STAGES (логика) =====================
const STAGE_TRANSFER_TO_SHOP = 'DT141_14:UC_FDWOQ4'; // “Передан в цех”
const STAGE_OTK_WAREHOUSE    = 'DT141_14:UC_MIB2DM'; // “Заказ готов/ОТК/Склад”
const STAGE_ASSIGNED_INSTALL = 'DT141_14:UC_44PKAP'; // “Комплектация/Назначен монтаж”
const STAGE_INSTALL_DONE     = 'DT141_14:UC_1LC3F5'; // “Монтаж выполнен”

const STAGE_SUCCESS          = 'DT141_14:SUCCESS';
const STAGE_RECL_IN          = 'DT141_14:UC_9ALF9C';
const STAGE_RECL_WAIT        = 'DT141_14:UC_27ZEBX';
const RECL_STAGES = new Set([STAGE_RECL_IN, STAGE_RECL_WAIT]);

const DEFAULT_ALL_STAGES = String(process.env.BITRIX_ALL_STAGES_DEFAULT || 'false').toLowerCase();
const DEFAULT_ALL_STAGES_ON = (DEFAULT_ALL_STAGES === '1' || DEFAULT_ALL_STAGES === 'true' || DEFAULT_ALL_STAGES === 'yes');

const STAGE_FILTER = [
  STAGE_TRANSFER_TO_SHOP,
  STAGE_OTK_WAREHOUSE,
  STAGE_ASSIGNED_INSTALL,
  STAGE_INSTALL_DONE,
  STAGE_SUCCESS,
  STAGE_RECL_IN,
  STAGE_RECL_WAIT
];

// ===================== STATIC =====================
const publicDir = path.join(__dirname, '..');

app.get('/login', (req, res) => {
  if (req.session && req.session.user) return res.redirect('/');
  return res.sendFile(path.join(publicDir, 'login.html'));
});
app.get('/login.html', (req, res) => {
  if (req.session && req.session.user) return res.redirect('/');
  return res.sendFile(path.join(publicDir, 'login.html'));
});

app.get('/admin', requireAuth, requireAdmin, (req, res) => res.sendFile(path.join(publicDir, 'admin.html')));
app.get('/admin.html', requireAuth, requireAdmin, (req, res) => res.sendFile(path.join(publicDir, 'admin.html')));

app.get('/favicon.ico', (req, res) => res.status(204).end());

function isProbablyMobile(req){
  // 1) выбор пользователя в сессии
  const ui = String(req.session?.ui || '').toLowerCase();
  if (ui === 'mobile') return true;
  if (ui === 'desktop') return false;

  // 2) Client Hints
  const ch = String(req.headers['sec-ch-ua-mobile'] || '').trim();
  if (ch === '?1') return true;
  if (ch === '?0') return false;

  // 3) UA fallback
  const ua = String(req.headers['user-agent'] || '').toLowerCase();
  return /(android|iphone|ipad|ipod|windows phone|mobile)/i.test(ua);
}

// Быстрые переключатели UI
app.get('/mobile', requireAuth, (req, res) => { req.session.ui = 'mobile'; return res.redirect('/'); });
app.get('/desktop', requireAuth, (req, res) => { req.session.ui = 'desktop'; return res.redirect('/'); });

// Root: authed -> mobile.html or index.html, else -> login
app.get('/', (req, res) => {
  if (!(req.session && req.session.user)) return res.redirect('/login');

  const qUi = String(req.query?.ui || '').toLowerCase();
  if (qUi === 'mobile' || qUi === 'desktop') req.session.ui = qUi;

  const file = isProbablyMobile(req) ? 'mobile.html' : 'index.html';
  return res.sendFile(path.join(publicDir, file));
});

app.get('/index.html', requireAuth, (req, res) => res.sendFile(path.join(publicDir, 'index.html')));
app.get('/mobile.html', requireAuth, (req, res) => res.sendFile(path.join(publicDir, 'mobile.html')));

// локальные ассеты под auth
app.use('/assets', requireAuth, express.static(path.join(publicDir, 'assets')));
app.use('/static', requireAuth, express.static(path.join(publicDir, 'static')));

// ===================== DATA VERSION + SSE =====================
let dataVersion = Date.now();
const sseClients = new Set();

function sseWrite(res, obj) {
  try { res.write(`data: ${JSON.stringify(obj)}\n\n`); } catch {}
}
function broadcastVersion() {
  for (const res of sseClients) sseWrite(res, { version: dataVersion });
}

app.get('/api/stream', requireAuth, (req, res) => {
  res.status(200);
  res.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
  res.setHeader('Cache-Control', 'no-cache, no-transform');
  res.setHeader('Connection', 'keep-alive');

  try { res.write('retry: 5000\n\n'); } catch {}
  sseWrite(res, { version: dataVersion });

  const hb = setInterval(() => {
    try { res.write(': ping\n\n'); } catch {}
  }, 25000);

  sseClients.add(res);

  req.on('close', () => {
    clearInterval(hb);
    sseClients.delete(res);
  });
});

// ===================== USERS STORAGE + AUTH =====================
const USERS_FILE = path.join(__dirname, 'users.json');
let usersStore = [];

const scryptAsync = promisify(crypto.scrypt);

function normUsername(u) { return String(u || '').trim().toLowerCase(); }

function userPublic(u) {
  return {
    id: u.id,
    username: u.username,
    fullName: u.fullName,
    role: u.role,
    createdAt: u.createdAt,
    updatedAt: u.updatedAt
  };
}

async function hashPasswordScrypt(pass) {
  const salt = crypto.randomBytes(16);
  const key = await scryptAsync(String(pass), salt, 64);
  return { type: 'scrypt', salt: salt.toString('base64'), hash: Buffer.from(key).toString('base64') };
}

async function verifyPassword(pass, user) {
  const p = String(pass || '');
  const ph = user?.passHash;

  if (user?.passSha256) {
    return safeEqual(sha256hex(p), String(user.passSha256).toLowerCase());
  }

  if (!ph || ph.type !== 'scrypt' || !ph.salt || !ph.hash) return false;

  const salt = Buffer.from(ph.salt, 'base64');
  const key = await scryptAsync(p, salt, 64);
  const got = Buffer.from(key).toString('base64');
  return safeEqual(got, ph.hash);
}

async function readJsonFileSafe(filePath, fallback) {
  try {
    const raw = await fs.readFile(filePath, 'utf8');
    const data = JSON.parse(raw);
    return data ?? fallback;
  } catch (e) {
    if (e && e.code === 'ENOENT') return fallback;
    console.error('readJsonFileSafe error:', filePath, e?.message || e);
    return fallback;
  }
}

async function writeJsonAtomic(filePath, obj) {
  const tmp = filePath + '.tmp';
  const raw = JSON.stringify(obj, null, 2);
  await fs.writeFile(tmp, raw, 'utf8');
  await fs.rename(tmp, filePath);
}

async function loadUsersStore() {
  const data = await readJsonFileSafe(USERS_FILE, []);
  usersStore = Array.isArray(data) ? data : [];
}
async function saveUsersStore() { await writeJsonAtomic(USERS_FILE, usersStore); }

function findUserByUsername(username) {
  const u = normUsername(username);
  if (!u) return null;
  return usersStore.find(x => normUsername(x.username) === u) || null;
}
function findUserById(id) {
  const s = String(id || '');
  return usersStore.find(x => String(x.id) === s) || null;
}
function makeUserId() {
  return 'u-' + Date.now() + '-' + Math.floor(Math.random() * 10000);
}
function isValidRole(r) { return ROLE_SET.has(String(r)); }

async function bootstrapAdminIfNeeded() {
  const hasAdmin = usersStore.some(u => String(u.role) === ROLE_ADMIN);
  if (hasAdmin) return;

  const username = normUsername(AUTH_USER || 'admin') || 'admin';
  const fullName = String(process.env.AUTH_FULLNAME || 'Администратор').trim() || 'Администратор';

  const passPlain = AUTH_PASS || 'admin';
  const passSha = AUTH_PASS_SHA256 ? String(AUTH_PASS_SHA256).toLowerCase() : '';

  const user = {
    id: makeUserId(),
    username,
    fullName,
    role: ROLE_ADMIN,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString()
  };

  if (passSha) user.passSha256 = passSha;
  else user.passHash = await hashPasswordScrypt(passPlain);

  usersStore.push(user);
  await saveUsersStore();
  console.log('BOOTSTRAP: created admin user from .env:', username);
}

// AUTH API
app.post('/api/login', async (req, res) => {
  try {
    const u = String(req.body?.username || '').trim();
    const p = String(req.body?.password || '');

    if (!u || !p) return res.status(400).json({ ok: false, error: 'bad_request' });

    const user = findUserByUsername(u);
    if (!user) return res.status(401).json({ ok: false, error: 'invalid_credentials' });

    const ok = await verifyPassword(p, user);
    if (!ok) return res.status(401).json({ ok: false, error: 'invalid_credentials' });

    req.session.user = {
      id: user.id,
      username: user.username,
      fullName: user.fullName,
      role: user.role
    };

    res.json({ ok: true, user: req.session.user });
  } catch {
    res.status(500).json({ ok: false, error: 'login_failed' });
  }
});

app.post('/api/logout', (req, res) => {
  try { req.session.destroy(() => res.json({ ok: true })); }
  catch { res.json({ ok: true }); }
});

app.get('/api/me', requireAuth, (req, res) => {
  res.json({ ok: true, user: req.session.user });
});

// ADMIN USERS API
app.get('/api/users', requireAuth, requireAdmin, (req, res) => {
  res.json({ ok: true, items: usersStore.map(userPublic) });
});

app.post('/api/users', requireAuth, requireAdmin, async (req, res) => {
  try {
    const fullName = String(req.body?.fullName || '').trim();
    const username = normUsername(req.body?.username);
    const role = String(req.body?.role || '').trim();
    const password = String(req.body?.password || '');

    if (!fullName) return res.status(400).json({ ok: false, error: 'fullName_required' });
    if (!username) return res.status(400).json({ ok: false, error: 'username_required' });
    if (!isValidRole(role)) return res.status(400).json({ ok: false, error: 'bad_role' });
    if (!password || password.length < 4) return res.status(400).json({ ok: false, error: 'password_too_short' });

    if (findUserByUsername(username)) return res.status(409).json({ ok: false, error: 'username_exists' });

    const user = {
      id: makeUserId(),
      username,
      fullName,
      role,
      passHash: await hashPasswordScrypt(password),
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString()
    };

    usersStore.push(user);
    await saveUsersStore();

    dataVersion = Date.now();
    broadcastVersion();

    res.json({ ok: true, item: userPublic(user) });
  } catch {
    res.status(500).json({ ok: false, error: 'user_create_failed' });
  }
});

app.put('/api/users/:id', requireAuth, requireAdmin, async (req, res) => {
  try {
    const id = String(req.params.id || '');
    const user = findUserById(id);
    if (!user) return res.status(404).json({ ok: false, error: 'not_found' });

    const fullName = String(req.body?.fullName || '').trim();
    const role = String(req.body?.role || '').trim();
    const password = String(req.body?.password || '');

    if (fullName) user.fullName = fullName;
    if (role) {
      if (!isValidRole(role)) return res.status(400).json({ ok: false, error: 'bad_role' });
      user.role = role;
    }
    if (password) {
      if (password.length < 4) return res.status(400).json({ ok: false, error: 'password_too_short' });
      delete user.passSha256;
      user.passHash = await hashPasswordScrypt(password);
    }

    user.updatedAt = new Date().toISOString();
    await saveUsersStore();

    if (req.session?.user?.id && String(req.session.user.id) === String(user.id)) {
      req.session.user.fullName = user.fullName;
      req.session.user.role = user.role;
      req.session.user.username = user.username;
    }

    dataVersion = Date.now();
    broadcastVersion();

    res.json({ ok: true, item: userPublic(user) });
  } catch {
    res.status(500).json({ ok: false, error: 'user_update_failed' });
  }
});

app.delete('/api/users/:id', requireAuth, requireAdmin, async (req, res) => {
  try {
    const id = String(req.params.id || '');
    if (req.session?.user?.id && String(req.session.user.id) === id) {
      return res.status(400).json({ ok: false, error: 'cannot_delete_self' });
    }

    const before = usersStore.length;
    usersStore = usersStore.filter(u => String(u.id) !== id);
    if (usersStore.length === before) return res.status(404).json({ ok: false, error: 'not_found' });

    await saveUsersStore();

    dataVersion = Date.now();
    broadcastVersion();

    res.json({ ok: true });
  } catch {
    res.status(500).json({ ok: false, error: 'user_delete_failed' });
  }
});

// ===================== STORAGE FILES =====================
const MANUAL_FILE = path.join(__dirname, 'manual_events.json');
const COMMENTS_FILE = path.join(__dirname, 'comments.json');
const GEOCODE_CACHE_FILE = path.join(__dirname, 'geocode_cache.json');
const RECL_SEEN_FILE = path.join(__dirname, 'recl_seen.json');
const DONE_FILE = path.join(__dirname, 'done_status.json');

// snapshot on disk
const SNAPSHOT_FILE = path.join(__dirname, 'events_snapshot.json');

function manualEventColor() { return '#ef4444'; }
function isManualId(id) { return String(id).startsWith('m-'); }

// Тип камня: ID -> текст
const STONE_TYPE_MAP = {
  '8142': 'Акрил',
  '8144': 'Кварц',
  '8146': 'Натуралка',
  '8270': 'Керамика',
  '8272': 'Кварц+Акрил'
};
function stoneTypeToText(v) {
  if (v === null || v === undefined) return '';
  const s = String(v).trim();
  if (!s) return '';
  return STONE_TYPE_MAP[s] || s;
}

// stores
let manualStore = [];
let commentsStore = {};
let geocodeCache = {};
let reclSeenStore = {};
let doneStore = {}; // id -> boolean

async function loadManualStore() {
  const data = await readJsonFileSafe(MANUAL_FILE, []);
  manualStore = Array.isArray(data) ? data : [];
}
async function saveManualStore() { await writeJsonAtomic(MANUAL_FILE, manualStore); }

async function loadCommentsStore() {
  const data = await readJsonFileSafe(COMMENTS_FILE, {});
  commentsStore = (data && typeof data === 'object' && !Array.isArray(data)) ? data : {};
}
async function saveCommentsStore() { await writeJsonAtomic(COMMENTS_FILE, commentsStore); }

async function loadGeocodeCache() {
  const data = await readJsonFileSafe(GEOCODE_CACHE_FILE, {});
  geocodeCache = (data && typeof data === 'object' && !Array.isArray(data)) ? data : {};
}
async function saveGeocodeCache() { await writeJsonAtomic(GEOCODE_CACHE_FILE, geocodeCache); }

async function loadReclSeenStore() {
  const data = await readJsonFileSafe(RECL_SEEN_FILE, {});
  reclSeenStore = (data && typeof data === 'object' && !Array.isArray(data)) ? data : {};
}
async function saveReclSeenStore() { await writeJsonAtomic(RECL_SEEN_FILE, reclSeenStore); }

async function loadDoneStore() {
  const data = await readJsonFileSafe(DONE_FILE, {});
  doneStore = (data && typeof data === 'object' && !Array.isArray(data)) ? data : {};
}
async function saveDoneStore() { await writeJsonAtomic(DONE_FILE, doneStore); }

// ===================== UTILS =====================
function toNum(v) {
  if (v === null || v === undefined) return null;
  const s = String(v).trim().replace(',', '.');
  if (s === '') return null;
  const n = Number(s);
  return Number.isNaN(n) ? null : n;
}
function normalizeId(v) {
  const n = toNum(v);
  if (!n) return null;
  const i = Math.trunc(n);
  return i > 0 ? i : null;
}
function normalizeIdList(v) {
  if (!v) return [];
  if (Array.isArray(v)) return v.map(normalizeId).filter(Boolean);
  const one = normalizeId(v);
  return one ? [one] : [];
}
function firstNonEmpty(...vals) {
  for (const v of vals) {
    if (v === null || v === undefined) continue;
    if (typeof v === 'string' && v.trim() === '') continue;
    return v;
  }
  return null;
}
function joinArrayField(v) {
  if (!v) return '';
  if (Array.isArray(v)) return v.filter(Boolean).join('\n');
  return String(v);
}
function truthyDate(v) {
  if (v === null || v === undefined) return false;
  if (typeof v === 'string' && v.trim() === '') return false;
  return true;
}
function isDateOnlyString(v){
  return typeof v === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(v.trim());
}
function eventAllDayFromStart(startVal){
  return isDateOnlyString(startVal);
}

// address/coords parsing
function parseAddressAndCoords(rawAddress, latField, lngField) {
  let cleanAddress = '';
  let lat = null;
  let lng = null;

  const latNum = toNum(latField);
  const lngNum = toNum(lngField);
  if (latNum !== null && lngNum !== null && (latNum !== 0 || lngNum !== 0)) {
    lat = latNum;
    lng = lngNum;
  }

  if (rawAddress) {
    const s = String(rawAddress);
    const parts = s.split('|');
    cleanAddress = (parts[0] || '').trim().replace(/,+\s*$/, '');

    if ((lat === null || lng === null) && parts.length > 1) {
      const coordPart = (parts[1] || '').trim();
      if (coordPart.includes(';')) {
        const [a, b] = coordPart.split(';');
        const la = toNum(a);
        const lo = toNum(b);
        if (la !== null && lo !== null && (la !== 0 || lo !== 0)) {
          lat = la;
          lng = lo;
        }
      }
    }
  }

  return { cleanAddress, lat, lng };
}

function parseYandexGeoField(v) {
  if (!v) return null;
  try {
    const obj = (typeof v === 'string') ? JSON.parse(v) : v;
    if (!obj || typeof obj !== 'object') return null;
    const c = obj.coord || obj.coords || obj.coordinates;
    let lat = null, lng = null;

    if (Array.isArray(c) && c.length >= 2) {
      lat = toNum(c[0]);
      lng = toNum(c[1]);
    } else if (typeof c === 'string') {
      const s = c.trim();
      const sep = s.includes(';') ? ';' : (s.includes(',') ? ',' : null);
      if (sep) {
        const [a, b] = s.split(sep);
        lat = toNum(a);
        lng = toNum(b);
      }
    }

    if (lat !== null && lng !== null && (lat !== 0 || lng !== 0)) return { lat, lng };
  } catch {}
  return null;
}

// ===================== COLORS (по правилам) =====================
const COLOR_GRAY_DARK = '#111827';
const COLOR_BLUE      = '#2563eb';
const COLOR_GREEN     = '#16a34a';
const COLOR_PURPLE    = '#7c3aed';
const COLOR_RED       = '#ef4444';

function detectPickup({ title, stoneText, installComment, extraComment }) {
  const t = [title, stoneText, installComment, extraComment].filter(Boolean).join(' ').toLowerCase();
  return (
    t.includes('самовывоз') ||
    t.includes('без монта') ||
    t.includes('безмонтаж') ||
    t.includes('без установки')
  );
}

function pickColorByRules({ reclEver, otkDate, stoneTypeId, title, stoneText, installComment, extraComment }) {
  if (reclEver) return COLOR_RED;
  if (!truthyDate(otkDate)) return COLOR_GRAY_DARK;

  if (detectPickup({ title, stoneText, installComment, extraComment })) return COLOR_PURPLE;

  const sid = (stoneTypeId === null || stoneTypeId === undefined) ? '' : String(stoneTypeId).trim();
  if (sid === '8142') return COLOR_BLUE;
  if (sid === '8144' || sid === '8270' || sid === '8146' || sid === '8272') return COLOR_GREEN;
  return COLOR_BLUE;
}

function colorToSortKey(color) {
  const c = String(color || '').toLowerCase();
  if (c === COLOR_GREEN) return 0;
  if (c === COLOR_BLUE || c === '#3b82f6') return 1;
  if (c === COLOR_PURPLE) return 2;
  if (c === COLOR_RED) return 3;
  if (c === COLOR_GRAY_DARK || c === '#6b7280') return 4;
  return 9;
}

// ===================== USERS CACHE + batch ускорение =====================
const userCache = new Map();
function userDisplayName(u) {
  if (!u) return '';
  const name = [u.NAME, u.SECOND_NAME, u.LAST_NAME].filter(Boolean).join(' ').trim();
  return name || (u.LOGIN || '') || '';
}
function getUserNameFromCache(id) {
  const uid = normalizeId(id);
  if (!uid) return '';
  const u = userCache.get(uid);
  return u ? (u.name || '') : '';
}

// batch: до 50 команд
async function fetchUsersByBatch(ids) {
  const uniq = Array.from(new Set((ids || []).map(normalizeId).filter(Boolean)));
  if (!uniq.length) return;

  const miss = uniq.filter(id => !userCache.has(id));
  if (!miss.length) return;

  const chunk = (arr, size) => {
    const out = [];
    for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
    return out;
  };

  const packs = chunk(miss, 50);

  for (const pack of packs) {
    const cmd = {};
    for (let i = 0; i < pack.length; i++) {
      const id = pack[i];
      cmd['u' + i] = `user.get?ID=${id}`;
    }

    const data = await bitrixPost('batch', { halt: 0, cmd });
    const results = data?.result?.result || {};

    for (const k of Object.keys(results)) {
      const arr = results[k];
      const u = Array.isArray(arr) ? arr[0] : null;
      const id = u?.ID ? normalizeId(u.ID) : null;
      if (!id) continue;
      userCache.set(id, { id, name: userDisplayName(u) });
    }
  }
}

// ===================== GEOCODE CACHE =====================
function normAddrKey(addr) {
  return String(addr || '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .replace(/\|/g, ' ')
    .replace(/[“”"]/g, '')
    .replace(/\s*,\s*/g, ', ')
    .replace(/,+\s*$/g, '')
    .trim();
}
function geocodeCandidates(address) {
  const a = String(address || '').trim();
  const out = new Set();
  if (!a) return [];
  out.add(a);

  let s = a.replace(/\s*,?\s*(подъезд|парадная|эт(аж)?|кв\.?|квартира|офис|пом\.?|помещение)\s*[^,]+/gi, '');
  s = s.replace(/\s+/g, ' ').trim().replace(/,+\s*$/g, '');
  if (s && s !== a) out.add(s);

  const parts = s.split(',').map(x => x.trim()).filter(Boolean);
  if (parts.length >= 3) out.add(parts.slice(0, 3).join(', '));
  if (parts.length >= 4) out.add(parts.slice(0, 4).join(', '));

  return Array.from(out).slice(0, 6);
}
function getCoordsFromCacheByAddress(address) {
  for (const cand of geocodeCandidates(address)) {
    const key = normAddrKey(cand);
    const c = geocodeCache[key];
    if (c && c.lat !== null && c.lng !== null) return { lat: c.lat, lng: c.lng, source: 'cache' };
  }
  return null;
}

// ===================== MANUAL -> FullCalendar event =====================
function manualToFcEvent(m) {
  const id = String(m.id);
  const color = manualEventColor();
  const start = m.start || m.plannedInstall || m.assignedInstall || new Date().toISOString();
  const freeComment = (commentsStore && commentsStore[id]) ? String(commentsStore[id]) : '';
  const sortKey = colorToSortKey(color);

  const sysDone = false;
  const done = !!doneStore[id];

  return {
    id,
    title: m.title || id,
    start,
    allDay: eventAllDayFromStart(start),
    backgroundColor: color,
    borderColor: color,
    extendedProps: {
      color,
      sortKey,

      done,
      isDone: sysDone,

      stageId: 'MANUAL',
      transferToShop: null,
      otkDate: null,

      plannedInstall: m.plannedInstall || null,
      assignedInstall: m.assignedInstall || null,
      sysAssignedInstall: null,
      assignedAny: m.assignedInstall || null,
      installDone: null,

      address: m.address || '',
      lat: m.lat ?? null,
      lng: m.lng ?? null,
      phone: m.phone || '',

      stoneTypeId: null,
      stoneText: String(m.stoneCode || '').trim(),
      materialCode: '',
      stoneCode: String(m.stoneCode || '').trim(),

      thickness: m.thickness || '',
      installComment: m.installComment || '',
      extraComment: '',

      managerId: null,
      managerName: '',
      installersIds: [],
      installersNames: [],

      googleEventId: null,
      freeComment,
      hideMarker: !!m.hideMarker
    }
  };
}

// ===================== BITRIX FIELDS =====================
const SELECT_FIELDS = [
  'id', 'title', 'stageId', 'begindate', 'createdTime', 'movedTime', 'updatedTime', 'closedate', 'assignedById', 'parentId2',
  'ufCrm8_1747916236564', // transferToShop
  'ufCrm8_1747306212023', // otk
  'ufCrm8_1744626911134', // planned
  'ufCrm8_1747908559319', // assigned
  'ufCrm8_1758806212646', // sys assigned
  'ufCrm8_1744639197488', // done
  'ufCrm8_1731616801',
  'ufCrm8_1765526240853',
  'ufCrm8_1765526264050',
  'ufCrm8_1765980928',
  'ufCrm8_1758702287777',
  'ufCrm8_1758702046420',
  'ufCrm8_1744626408052',
  'ufCrm8_1758702057969',
  'ufCrm8_1732097971828',
  'ufCrm8_1744638484827',
  'ufCrm8_1731616262', // stone type
  'ufCrm8_1759661234', // material code (fallback)
  'ufCrm8_1748001850227', // thickness
  'ufCrm8_1731617119', // manager
  'ufCrm8_1748338492', // installers
  'ufCrm8GoogleCalendarEvent'
];

async function loadItemsForOneStage(stageId) {
  let start = 0;
  let all = [];
  while (true) {
    const data = await bitrixPost('crm.item.list', {
      entityTypeId: ENTITY_TYPE_ID,
      start,
      filter: { '=categoryId': CATEGORY_ID, '=stageId': stageId },
      select: SELECT_FIELDS
    });

    const items = (data?.result?.items) ? data.result.items : [];
    all = all.concat(items);

    if (data?.next === undefined || data?.next === null) break;
    start = data.next;
  }
  return all;
}

async function mapLimit(arr, limit, fn) {
  const res = new Array(arr.length);
  let idx = 0;
  const workers = new Array(Math.min(limit, arr.length)).fill(0).map(async () => {
    while (idx < arr.length) {
      const i = idx++;
      res[i] = await fn(arr[i]);
    }
  });
  await Promise.all(workers);
  return res;
}

async function loadAllSmartItems({ allStages = false, stageIds = null } = {}) {
  const useStageIds = Array.isArray(stageIds) && stageIds.length;

  if (useStageIds || !allStages) {
    const stages = useStageIds ? stageIds : STAGE_FILTER;
    const parts = await mapLimit(stages, 3, loadItemsForOneStage);
    return parts.flat();
  }

  let start = 0;
  let all = [];
  while (true) {
    const data = await bitrixPost('crm.item.list', {
      entityTypeId: ENTITY_TYPE_ID,
      start,
      filter: { '=categoryId': CATEGORY_ID },
      select: SELECT_FIELDS
    });

    const items = (data?.result?.items) ? data.result.items : [];
    all = all.concat(items);

    if (data?.next === undefined || data?.next === null) break;
    start = data.next;
  }
  return all;
}

// ===================== MAP ITEMS -> EVENTS (логика по датам) =====================
function mapItemsToEvents(items) {
  const events = [];
  const skipped = { noTransferToShop: 0, noStartDate: 0, noPlannedNoAssigned: 0 };
  let reclChanged = false;

  for (const item of items) {
    const idStr = String(item.id);
    const stageId = String(item.stageId || '').trim();

    const isReclNow = RECL_STAGES.has(stageId);
    const isReclEver = isReclNow || !!reclSeenStore[idStr];

    if (isReclNow && !reclSeenStore[idStr]) {
      reclSeenStore[idStr] = { at: new Date().toISOString(), stageId };
      reclChanged = true;
    }

    // 1) карточка существует только если transferToShop заполнен
    const transferToShop = item.ufCrm8_1747916236564 || null;
    if (!truthyDate(transferToShop)) { skipped.noTransferToShop++; continue; }

    const plannedInstall = item.ufCrm8_1744626911134 || null;
    const assignedInstall = item.ufCrm8_1747908559319 || null;
    const sysAssignedInstall = item.ufCrm8_1758806212646 || null;
    const installDone = item.ufCrm8_1744639197488 || null;

    const assignedAny = firstNonEmpty(assignedInstall, sysAssignedInstall);

    // 2) дата события: installDone > assignedAny > plannedInstall
    let dateValue = null;
    if (truthyDate(installDone)) dateValue = installDone;
    else if (truthyDate(assignedAny)) dateValue = assignedAny;
    else if (truthyDate(plannedInstall)) dateValue = plannedInstall;
    else { skipped.noPlannedNoAssigned++; continue; }

    if (!truthyDate(dateValue)) { skipped.noStartDate++; continue; }

   // const sysDone = truthyDate(installDone); // system done (Bitrix)
const sysDone = (String(stageId) === STAGE_SUCCESS);
    // address/coords
    const parsed = parseAddressAndCoords(
      item.ufCrm8_1731616801,
      item.ufCrm8_1765526240853,
      item.ufCrm8_1765526264050
    );

    const cleanAddress = parsed.cleanAddress;
    let lat = parsed.lat;
    let lng = parsed.lng;

    if (lat === null || lng === null) {
      const y = parseYandexGeoField(item.ufCrm8_1765980928);
      if (y) { lat = y.lat; lng = y.lng; }
    }

    if ((lat === null || lng === null) && cleanAddress) {
      const cached = getCoordsFromCacheByAddress(cleanAddress);
      if (cached) { lat = cached.lat; lng = cached.lng; }
    }

    // 4) system done => точку скрыть, coords обнулить
    const hideMarker = sysDone;
    //if (hideMarker) { lat = null; lng = null; }

    const orderNumber = String(item.ufCrm8_1758702287777 || '').trim();
    const customerName = String(item.ufCrm8_1758702046420 || '').trim();
    const prefix = orderNumber ? `${orderNumber}` : `${item.id}`;
    let title = prefix;
    if (customerName) title = `${prefix} — ${customerName}`;

    const phone = firstNonEmpty(item.ufCrm8_1744626408052, item.ufCrm8_1758702057969) || '';

    const installComment = joinArrayField(item.ufCrm8_1732097971828);
    const extraComment = joinArrayField(item.ufCrm8_1744638484827);

    const stoneTypeId = item.ufCrm8_1731616262 ?? null;
    const stoneTextFromId = stoneTypeToText(stoneTypeId);
    const materialCode = String(item.ufCrm8_1759661234 || '').trim();
    const stoneText = (stoneTextFromId || materialCode || '').trim();

    const thickness = String(item.ufCrm8_1748001850227 || '').trim();

    const managerId = normalizeId(item.ufCrm8_1731617119);
    const installersIds = normalizeIdList(item.ufCrm8_1748338492);

    const managerName = managerId ? getUserNameFromCache(managerId) : '';
    const installersNames = installersIds.map(getUserNameFromCache).filter(Boolean);

    const otkDate = item.ufCrm8_1747306212023 || null;

    // 3) цвет: если нет otkDate => серый, иначе по правилам; рекламация => красный
    const color = pickColorByRules({
      reclEver: isReclEver,
      otkDate,
      stoneTypeId,
      title,
      stoneText,
      installComment,
      extraComment
    });
    const sortKey = colorToSortKey(color);

    const freeComment = (commentsStore && commentsStore[idStr]) ? String(commentsStore[idStr]) : '';
    const googleEventId = String(item.ufCrm8GoogleCalendarEvent || '').trim() || null;

    // done:
    const done = sysDone ? true : (isReclEver ? false : !!doneStore[idStr]);

    events.push({
      id: idStr,
      title,
      start: dateValue,
      allDay: eventAllDayFromStart(dateValue),
      backgroundColor: color,
      borderColor: color,
      extendedProps: {
        color,
        sortKey,

        done,
        isDone: sysDone,

        stageId,

        transferToShop,
        otkDate,

        plannedInstall,
        assignedInstall: assignedInstall || null,
        sysAssignedInstall: sysAssignedInstall || null,
        assignedAny: assignedAny || null,
        installDone,

        orderNumber,
        customerName,
        phone,

        address: cleanAddress,
        rawAddress: item.ufCrm8_1731616801 || null,
        lat,
        lng,

        installComment,
        extraComment,

        stoneTypeId,
        stoneText,
        materialCode,
        stoneCode: stoneText,
        thickness,

        managerId,
        managerName,
        installersIds,
        installersNames,

        googleEventId,
        freeComment,

        hideMarker
      }
    });
  }

  return { events, skipped, reclChanged };
}

// ===================== SNAPSHOT (мгновенный /api/events) =====================
let eventsSnapshot = {
  version: dataVersion,
  builtAt: 0,
  key: null,
  payload: { version: dataVersion, meta: { warm: false }, events: [] },
  digest: null
};

async function loadSnapshotFromDisk() {
  const snap = await readJsonFileSafe(SNAPSHOT_FILE, null);
  if (!snap || typeof snap !== 'object') return false;
  if (!snap.payload || !snap.payload.events) return false;

  eventsSnapshot = {
    version: Number(snap.version) || Date.now(),
    builtAt: Number(snap.builtAt) || Date.now(),
    key: snap.key || null,
    payload: snap.payload,
    digest: snap.digest || null
  };

  dataVersion = eventsSnapshot.version;
  return true;
}

async function saveSnapshotToDisk() {
  const snap = {
    version: eventsSnapshot.version,
    builtAt: eventsSnapshot.builtAt,
    key: eventsSnapshot.key,
    digest: eventsSnapshot.digest,
    payload: eventsSnapshot.payload
  };
  await writeJsonAtomic(SNAPSHOT_FILE, snap);
}

function snapshotDigest(payload) {
  const h = crypto.createHash('sha1');
  const evs = payload?.events || [];
  h.update(String(evs.length));
  for (const e of evs) {
    const p = e.extendedProps || {};
    h.update('|');
    h.update(String(e.id));
    h.update('|');
    h.update(String(e.start || ''));
    h.update('|');
    h.update(String(e.backgroundColor || ''));
    h.update('|');
    h.update(String(!!p.done));
    h.update('|');
    h.update(String(!!p.isDone));
    h.update('|');
    h.update(String(!!p.hideMarker));
    h.update('|');
    h.update(String(p.otkDate || ''));
    h.update('|');
    h.update(String(p.stageId || ''));
  }
  return h.digest('hex');
}

let refreshInFlight = null;
let refreshPending = false;
let refreshTimer = null;

function scheduleRefresh(reason = 'unknown') {
  if (refreshTimer) clearTimeout(refreshTimer);
  refreshTimer = setTimeout(() => {
    refreshTimer = null;
    refreshSnapshot(reason).catch(() => {});
  }, 250);
}

async function refreshSnapshot(reason = 'manual') {
  if (refreshInFlight) { refreshPending = true; return refreshInFlight; }

  refreshInFlight = (async () => {
    const t0 = Date.now();
    try {
      const allStages = DEFAULT_ALL_STAGES_ON;
      const stageIds = null;

      const items = await loadAllSmartItems({ allStages, stageIds });

      // warm users cache
      const ids = [];
      for (const it of items) {
        const mid = normalizeId(it.ufCrm8_1731617119);
        if (mid) ids.push(mid);
        const arr = normalizeIdList(it.ufCrm8_1748338492);
        for (const x of arr) ids.push(x);
      }
      await fetchUsersByBatch(ids);

      const mapped = mapItemsToEvents(items);
      if (mapped.reclChanged) await saveReclSeenStore();

      const manualEvents = (manualStore || [])
        .filter(m => m && isManualId(m.id))
        .map(manualToFcEvent);

      const payload = {
        version: dataVersion,
        meta: {
          reason,
          builtAt: new Date().toISOString(),
          perfMs: Date.now() - t0,
          bitrixItems: items.length,
          eventsOut: mapped.events.length,
          manualOut: manualEvents.length,
          skipped: mapped.skipped
        },
        events: mapped.events.concat(manualEvents)
      };

      const digest = snapshotDigest(payload);

      if (eventsSnapshot.digest && eventsSnapshot.digest === digest) {
        eventsSnapshot.builtAt = Date.now();
        eventsSnapshot.payload.meta = { ...(eventsSnapshot.payload.meta || {}), lastRebuildMs: Date.now() - t0, reason, unchanged: true };
        await saveSnapshotToDisk().catch(() => {});
        return;
      }

      dataVersion = Date.now();
      payload.version = dataVersion;

      eventsSnapshot = {
        version: dataVersion,
        builtAt: Date.now(),
        key: null,
        payload,
        digest
      };

      await saveSnapshotToDisk().catch(() => {});
      broadcastVersion();
    } catch (e) {
      console.error('refreshSnapshot error:', e?.response?.data || e?.message || e);
    } finally {
      refreshInFlight = null;
      if (refreshPending) {
        refreshPending = false;
        scheduleRefresh('pending');
      }
    }
  })();

  return refreshInFlight;
}

// ✅ ПАТЧ СНАПШОТА (чтобы перенос сразу был виден всем, без "двух перезагрузок")
function patchSnapshotAssignedDate(idStr, value) {
  try {
    const evs = eventsSnapshot?.payload?.events;
    if (!Array.isArray(evs)) return false;

    const idx = evs.findIndex(e => String(e?.id) === String(idStr));
    if (idx < 0) return false;

    const ev = evs[idx];
    const p = ev.extendedProps || {};

    // если системно выполнено — не патчим
    if (p.isDone === true || p.hideMarker === true) return false;

    ev.start = value;
    ev.allDay = eventAllDayFromStart(value);

    p.assignedInstall = value;
    p.assignedAny = value;

    ev.extendedProps = p;

    // bump version
    dataVersion = Date.now();
    eventsSnapshot.version = dataVersion;
    eventsSnapshot.builtAt = Date.now();
    if (eventsSnapshot.payload) {
      eventsSnapshot.payload.version = dataVersion;
      eventsSnapshot.payload.meta = { ...(eventsSnapshot.payload.meta || {}), reason: 'assigned_patch', patchedAt: new Date().toISOString() };
    }
    eventsSnapshot.digest = snapshotDigest(eventsSnapshot.payload);

    // persist + notify
    saveSnapshotToDisk().catch(() => {});
    broadcastVersion();

    return true;
  } catch {
    return false;
  }
}

function patchSnapshotManualDate(idStr, value) {
  try {
    const evs = eventsSnapshot?.payload?.events;
    if (!Array.isArray(evs)) return false;

    const idx = evs.findIndex(e => String(e?.id) === String(idStr));
    if (idx < 0) return false;

    const ev = evs[idx];
    const p = ev.extendedProps || {};

    ev.start = value;
    ev.allDay = eventAllDayFromStart(value);
    p.assignedInstall = value;
    p.assignedAny = value;
    ev.extendedProps = p;

    dataVersion = Date.now();
    eventsSnapshot.version = dataVersion;
    eventsSnapshot.builtAt = Date.now();
    if (eventsSnapshot.payload) {
      eventsSnapshot.payload.version = dataVersion;
      eventsSnapshot.payload.meta = { ...(eventsSnapshot.payload.meta || {}), reason: 'manual_patch', patchedAt: new Date().toISOString() };
    }
    eventsSnapshot.digest = snapshotDigest(eventsSnapshot.payload);

    saveSnapshotToDisk().catch(() => {});
    broadcastVersion();
    return true;
  } catch {
    return false;
  }
}

// ===================== Bitrix polling (страховка реактивности) =====================
const POLL_INTERVAL_MS = Math.max(2000, Number(process.env.BITRIX_POLL_MS) || 8000);
let lastTopUpdatedTime = null;

async function pollBitrixTopUpdate() {
  try {
    const data = await bitrixPost('crm.item.list', {
      entityTypeId: ENTITY_TYPE_ID,
      start: 0,
      order: { updatedTime: 'DESC' },
      filter: { '=categoryId': CATEGORY_ID },
      select: ['id', 'updatedTime', 'stageId']
    });

    const top = (data?.result?.items && data.result.items[0]) ? data.result.items[0] : null;
    const ut = top?.updatedTime ? String(top.updatedTime) : null;

    if (ut && lastTopUpdatedTime && ut !== lastTopUpdatedTime) {
      scheduleRefresh('bitrix_poll');
    }
    if (ut) lastTopUpdatedTime = ut;
  } catch {
  } finally {
    setTimeout(pollBitrixTopUpdate, POLL_INTERVAL_MS);
  }
}

// ===================== API =====================

// 🚀 мгновенно: отдаём snapshot из памяти (и с диска после рестарта)
app.get('/api/events', requireAuth, async (req, res) => {
  if (!eventsSnapshot.payload?.events?.length) {
    scheduleRefresh('first_request');
  }
  res.json(eventsSnapshot.payload);
});

app.get('/api/ping', requireAuth, (req, res) => res.json({ version: dataVersion }));

app.get('/api/diag', requireAuth, (req, res) => {
  res.json({
    ok: true,
    port: PORT,
    user: req.session?.user || null,
    bitrixWebhookUrl: BITRIX_WEBHOOK_URL ? 'set' : 'empty',
    entityTypeId: ENTITY_TYPE_ID,
    categoryId: CATEGORY_ID,
    stageFilter: STAGE_FILTER,
    defaultAllStages: DEFAULT_ALL_STAGES_ON,
    stores: {
      users: usersStore.length,
      manualItems: manualStore.length,
      comments: Object.keys(commentsStore || {}).length,
      geocodeCacheKeys: Object.keys(geocodeCache || {}).length,
      reclSeen: Object.keys(reclSeenStore || {}).length,
      done: Object.keys(doneStore || {}).length
    },
    usersCache: { size: userCache.size },
    snapshot: {
      version: eventsSnapshot.version,
      builtAt: eventsSnapshot.builtAt,
      events: (eventsSnapshot.payload?.events || []).length,
      digest: eventsSnapshot.digest
    },
    sse: { clients: sseClients.size },
    poll: { intervalMs: POLL_INTERVAL_MS, lastTopUpdatedTime }
  });
});

// ✅ Bitrix hook: просто планируем обновление снапшота
app.post('/api/bitrix/hook', requireAuth, requireEditor, (req, res) => {
  scheduleRefresh('bitrix_hook');
  res.json({ ok: true, scheduled: true });
});

// ===== DONE API (общий статус) =====
app.get('/api/done/:id', requireAuth, (req, res) => {
  const id = String(req.params.id || '');
  res.json({ ok: true, id, done: !!doneStore[id] });
});

app.put('/api/done/:id', requireAuth, requireEditor, async (req, res) => {
  try {
    const id = String(req.params.id || '');
    const done = !!req.body?.done;
    doneStore[id] = done;
    await saveDoneStore();
    scheduleRefresh('done_update');
    res.json({ ok: true, id, done });
  } catch {
    res.status(500).json({ ok: false, error: 'done_save_failed' });
  }
});

// ===== GEOCODE API =====
app.get('/api/geocode', requireAuth, async (req, res) => {
  try {
    const address = String(req.query.address || '').trim();
    if (!address) return res.status(400).json({ ok: false, error: 'address_required' });

    const cached = getCoordsFromCacheByAddress(address);
    if (cached) return res.json({ ok: true, lat: cached.lat, lng: cached.lng, source: cached.source });

    const NOMINATIM_URL = process.env.NOMINATIM_URL || 'https://nominatim.openstreetmap.org/search';
    const UA = process.env.NOMINATIM_UA || 'calendar-map/1.0';

    const { data } = await axiosInst.get(NOMINATIM_URL, {
      timeout: 12000,
      headers: { 'User-Agent': UA },
      params: { format: 'json', limit: 1, q: address }
    });

    const top = Array.isArray(data) ? data[0] : null;
    const lat = toNum(top?.lat);
    const lng = toNum(top?.lon);

    if (lat === null || lng === null) return res.json({ ok: false, error: 'not_found' });

    const key = normAddrKey(address);
    geocodeCache[key] = { lat, lng, at: new Date().toISOString() };
    await saveGeocodeCache();

    res.json({ ok: true, lat, lng, source: 'nominatim' });
  } catch {
    res.status(500).json({ ok: false, error: 'geocode_failed' });
  }
});

// ===== COMMENTS API =====
app.get('/api/comment/:id', requireAuth, (req, res) => {
  const id = String(req.params.id || '');
  const text = (commentsStore && commentsStore[id]) ? String(commentsStore[id]) : '';
  res.json({ ok: true, id, text });
});

app.put('/api/comment/:id', requireAuth, requireEditor, async (req, res) => {
  try {
    const id = String(req.params.id || '');
    const text = String((req.body && req.body.text) ? req.body.text : '');
    commentsStore[id] = text;
    await saveCommentsStore();
    scheduleRefresh('comment_update');
    res.json({ ok: true, id, text });
  } catch {
    res.status(500).json({ error: 'comment_save_failed' });
  }
});

// ===== MANUAL API =====
app.get('/api/manual', requireAuth, (req, res) => res.json({ ok: true, items: manualStore || [] }));

app.post('/api/manual', requireAuth, requireEditor, async (req, res) => {
  try {
    const b = req.body || {};
    const title = String(b.title || '').trim();
    if (!title) return res.status(400).json({ error: 'title_required' });

    const id = 'm-' + Date.now() + '-' + Math.floor(Math.random() * 1000);

    const obj = {
      id,
      title,
      phone: String(b.phone || '').trim(),
      address: String(b.address || '').trim(),
      lat: (b.lat === null || b.lat === undefined) ? null : toNum(b.lat),
      lng: (b.lng === null || b.lng === undefined) ? null : toNum(b.lng),
      stoneCode: String(b.stoneCode || '').trim(),
      thickness: String(b.thickness || '').trim(),
      plannedInstall: b.plannedInstall || null,
      assignedInstall: b.assignedInstall || null,
      installComment: String(b.installComment || '').trim(),
      start: b.start || null,
      createdAt: new Date().toISOString()
    };

    if ((obj.lat === null || obj.lng === null) && obj.address) {
      const cached = getCoordsFromCacheByAddress(obj.address);
      if (cached) { obj.lat = cached.lat; obj.lng = cached.lng; }
    }

    manualStore.push(obj);
    await saveManualStore();

    scheduleRefresh('manual_create');
    res.json({ ok: true, event: manualToFcEvent(obj) });
  } catch {
    res.status(500).json({ error: 'manual_create_failed' });
  }
});

// ✅ UPDATE manual (чтобы можно было переносить дату)
app.put('/api/manual/:id', requireAuth, requireEditor, async (req, res) => {
  try {
    const id = String(req.params.id || '');
    if (!isManualId(id)) return res.status(400).json({ ok:false, error:'bad_id' });

    const start = String(req.body?.start || '').trim();
    const assignedInstall = String(req.body?.assignedInstall || start || '').trim();

    const idx = (manualStore || []).findIndex(x => String(x?.id) === id);
    if (idx < 0) return res.status(404).json({ ok:false, error:'not_found' });

    if (start) manualStore[idx].start = start;
    if (assignedInstall) manualStore[idx].assignedInstall = assignedInstall;

    await saveManualStore();

    // мгновенно патчим снапшот
    patchSnapshotManualDate(id, start || assignedInstall);

    // потом фоном пересоберём снапшот (на всякий)
    setTimeout(() => scheduleRefresh('manual_update'), 1500);

    res.json({ ok:true, id, start: start || null, assignedInstall: assignedInstall || null });
  } catch (e) {
    res.status(500).json({ ok:false, error:'manual_update_failed', details: e?.message || String(e) });
  }
});

app.delete('/api/manual/:id', requireAuth, requireEditor, async (req, res) => {
  try {
    const id = String(req.params.id || '');
    if (!isManualId(id)) return res.status(400).json({ error: 'bad_id' });

    const before = manualStore.length;
    manualStore = manualStore.filter(x => String(x.id) !== id);
    if (manualStore.length === before) return res.status(404).json({ error: 'not_found' });

    await saveManualStore();

    if (commentsStore && Object.prototype.hasOwnProperty.call(commentsStore, id)) {
      delete commentsStore[id];
      await saveCommentsStore();
    }
    if (doneStore && Object.prototype.hasOwnProperty.call(doneStore, id)) {
      delete doneStore[id];
      await saveDoneStore();
    }

    scheduleRefresh('manual_delete');
    res.json({ ok: true });
  } catch {
    res.status(500).json({ error: 'manual_delete_failed' });
  }
});

// ===================== ✅ Перенос "Назначено" в Bitrix =====================
// Требуемый endpoint:
app.put('/api/assigned-date/:id', requireAuth, requireEditor, async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!Number.isFinite(id) || id <= 0) return res.status(400).json({ ok:false, error:'bad_id' });

    const value = String(req.body?.value || '').trim(); // ожидаем ISO/bitrix datetime
    if (!value) return res.status(400).json({ ok:false, error:'value_required' });

    const fieldId = BITRIX_ASSIGNED_FIELD_ID;

    // основной метод для smart-process:
    const data = await bitrixPost('crm.item.update', {
      entityTypeId: ENTITY_TYPE_ID,
      id,
      fields: { [fieldId]: value }
    });

    // ✅ мгновенно патчим текущий снапшот (чтобы у всех сразу стало видно)
    patchSnapshotAssignedDate(String(id), value);

    // ✅ и в фоне пересоберём снапшот чуть позже (чтобы синкнуться с Bitrix)
    setTimeout(() => scheduleRefresh('assigned_update'), 2500);

    res.json({ ok:true, id, fieldId, value, result: data?.result ?? null });
  } catch (e) {
    res.status(500).json({ ok:false, error:'bitrix_update_failed', details: e?.response?.data || e?.message || String(e) });
  }
});

// Алиас под старое/привычное
app.put('/api/bitrix/assigned/:id', requireAuth, requireEditor, async (req, res) => {
  // прокидываем на тот же handler логикой
  req.url = '/api/assigned-date/' + encodeURIComponent(String(req.params.id || ''));
  return app._router.handle(req, res, () => {});
});

// ===================== START =====================
(async () => {
  await loadUsersStore();
  await bootstrapAdminIfNeeded();

  await loadManualStore();
  await loadCommentsStore();
  await loadGeocodeCache();
  await loadReclSeenStore();
  await loadDoneStore();

  const loaded = await loadSnapshotFromDisk();
  if (!loaded) scheduleRefresh('startup');
  else scheduleRefresh('startup_warm');

  pollBitrixTopUpdate();

  app.listen(PORT, () => {
    console.log(`API listening on port ${PORT}`);
    console.log(`Login page: http://localhost:${PORT}/login`);
    console.log(`Calendar:    http://localhost:${PORT}/`);
    console.log(`Mobile:      http://localhost:${PORT}/mobile`);
    console.log(`Desktop:     http://localhost:${PORT}/desktop`);
    console.log(`Admin:       http://localhost:${PORT}/admin (admin only)`);
    console.log(`SSE: /api/stream`);
    console.log(`DIAG: http://localhost:${PORT}/api/diag`);
    console.log(`Polling: every ~${POLL_INTERVAL_MS}ms`);
  });
})();
