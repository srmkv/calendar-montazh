// server.js
// smart-process + static index.html
// + фильтр/логика по датам (см. “Логика основанная на датах”)
// + координаты + карта + цвета
// + менеджер + монтажники (ФИО пользователей)
// + тип камня (ID -> текст)
// + ручные карточки (manual_events.json) и видны всем
// + "Комментарий свободный" (comments.json)
// + geo из Bitrix ufCrm8_1765980928 (coord:[lat,lng])
// + геокодинг Nominatim с кешем (geocode_cache.json)
// + DIAG: /api/diag
//
// PERF/FIX:
//  - gzip (compression)
//  - axios keep-alive
//  - user.get через batch (50 команд за запрос)
//  - загрузка стадий параллельно (limit=3) (если используем stageIds)
//  - /api/events cache TTL 60s
//  - /api/geocode (для фронтового fallback)

const express = require('express');
const cors = require('cors');
const axios = require('axios');
const compression = require('compression');
const dotenv = require('dotenv');
const path = require('path');
const fs = require('fs/promises');
const http = require('http');
const https = require('https');

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json({ limit: '2mb' }));
app.use(compression());

// ==== НАСТРОЙКИ BITRIX + ПОРТ ====
const PORT = process.env.PORT || 3050;

function normalizeWebhookUrl(u) {
  const s = String(u || '').trim();
  if (!s) return '';
  return s.endsWith('/') ? s : (s + '/');
}

const BITRIX_WEBHOOK_URL = normalizeWebhookUrl(process.env.BITRIX_WEBHOOK_URL);
const ENTITY_TYPE_ID = Number(process.env.BITRIX_SMART_ENTITY_TYPE_ID) || 141;
const CATEGORY_ID = Number(process.env.BITRIX_CATEGORY_ID) || 14;

// ==== ЛОГИКА СТАДИЙ (для цвета/статуса) ====
const STAGE_TRANSFER_TO_SHOP = 'DT141_14:UC_FDWOQ4'; // “Передан в цех” (инфо)
const STAGE_OTK_WAREHOUSE    = 'DT141_14:UC_MIB2DM'; // “Заказ готов/ОТК/Склад” (инфо)
const STAGE_SUCCESS          = 'DT141_14:SUCCESS';   // “Выполнено” (может быть, но DONE берём по дате завершения монтажа)
const STAGE_RECL_IN          = 'DT141_14:UC_9ALF9C';  // “Рекламация/Не вып (входящий)”
const STAGE_RECL_WAIT        = 'DT141_14:UC_27ZEBX';  // “Рекламация/Не вып (ждут выезда)”
const RECL_STAGES = new Set([STAGE_RECL_IN, STAGE_RECL_WAIT]);

// ⚠️ ВАЖНО: чтобы “не пропадали карточки”, по умолчанию грузим ВСЕ стадии.
// Можно переопределить env BITRIX_ALL_STAGES_DEFAULT=false и тогда будут использоваться STAGE_FILTER ниже.
const DEFAULT_ALL_STAGES = String(process.env.BITRIX_ALL_STAGES_DEFAULT || 'true').toLowerCase();
const DEFAULT_ALL_STAGES_ON = (DEFAULT_ALL_STAGES === '1' || DEFAULT_ALL_STAGES === 'true' || DEFAULT_ALL_STAGES === 'yes');

// По умолчанию (если отключите allStages) — минимально полезный набор стадий.
// Если у вас есть другие стадии после “Передан в цех”, добавьте их сюда или включите allStages.
const STAGE_FILTER = [
  STAGE_TRANSFER_TO_SHOP,
  STAGE_OTK_WAREHOUSE,
  STAGE_SUCCESS,
  STAGE_RECL_IN,
  STAGE_RECL_WAIT
];

if (!BITRIX_WEBHOOK_URL) {
  console.error('ERROR: BITRIX_WEBHOOK_URL is empty. Put it into .env');
  process.exit(1);
}

// ==== axios keep-alive ====
const axiosInst = axios.create({
  timeout: 20000,
  httpAgent: new http.Agent({ keepAlive: true }),
  httpsAgent: new https.Agent({ keepAlive: true })
});

// ==== СТАТИКА ====
const publicDir = path.join(__dirname, '..');
app.use(express.static(publicDir));
app.get('/', (req, res) => res.sendFile(path.join(publicDir, 'index.html')));
app.get('/favicon.ico', (req, res) => res.status(204).end());

// ==== "версия данных" для фронта ====
let dataVersion = Date.now();

// ==== STORAGE (server files) ====
const MANUAL_FILE = path.join(__dirname, 'manual_events.json');
const COMMENTS_FILE = path.join(__dirname, 'comments.json');
const GEOCODE_CACHE_FILE = path.join(__dirname, 'geocode_cache.json');

function manualEventColor() { return '#ef4444'; }
function isManualId(id) { return String(id).startsWith('m-'); }

// ==== Тип камня: ID -> текст ====
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

// ==== file helpers ====
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

// ==== stores ====
let manualStore = [];
let commentsStore = {};
let geocodeCache = {};

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

// ==== utils ====
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

    // "адрес|;|6940" -> берём до первого "|"
    const parts = s.split('|');
    cleanAddress = (parts[0] || '').trim().replace(/,+\s*$/, '');

    // иногда "адрес|59.9;30.3"
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

// ✅ coord из ufCrm8_1765980928
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

function truthyDate(v) {
  if (v === null || v === undefined) return false;
  if (typeof v === 'string' && v.trim() === '') return false;
  return true;
}

function isDateOnlyString(v){
  return typeof v === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(v.trim());
}

function eventAllDayFromStart(startVal){
  // чтобы не “пропадали” события из-за mismatch allDay/start
  return isDateOnlyString(startVal);
}

// ==== Цвета по правилам ====
const COLOR_GRAY_DARK = '#111827'; // “черные/серые” до ОТК (читабельнее)
const COLOR_BLUE      = '#2563eb'; // акрил
const COLOR_GREEN     = '#16a34a'; // кварц/керамика/натуралка/кварц+акрил
const COLOR_PURPLE    = '#7c3aed'; // самовывоз
const COLOR_RED       = '#ef4444'; // рекламация

function detectPickup({ title, stoneText, installComment, extraComment }) {
  const t = [title, stoneText, installComment, extraComment]
    .filter(Boolean)
    .join(' ')
    .toLowerCase();

  return (
    t.includes('самовывоз') ||
    t.includes('без монта') ||
    t.includes('безмонтаж') ||
    t.includes('без установки')
  );
}

function pickColorByRules({ stageId, otkDate, stoneTypeId, title, stoneText, installComment, extraComment }) {
  // рекламация: всегда красная
  if (RECL_STAGES.has(String(stageId || ''))) return COLOR_RED;

  // до ОТК: “черная/серая”
  if (!truthyDate(otkDate)) return COLOR_GRAY_DARK;

  // после ОТК: самовывоз / тип камня
  if (detectPickup({ title, stoneText, installComment, extraComment })) return COLOR_PURPLE;

  const sid = (stoneTypeId === null || stoneTypeId === undefined) ? '' : String(stoneTypeId).trim();
  if (sid === '8142') return COLOR_BLUE; // Акрил
  if (sid === '8144' || sid === '8270' || sid === '8146' || sid === '8272') return COLOR_GREEN;

  // fallback
  // если тип камня не из списка — пусть будет синий (как раньше)
  return COLOR_BLUE;
}

// ==== сортировка внутри дня: green -> blue -> purple -> red -> gray ====
function colorToSortKey(color) {
  const c = String(color || '').toLowerCase();
  if (c === COLOR_GREEN) return 0;
  if (c === COLOR_BLUE || c === '#3b82f6') return 1; // на всякий случай
  if (c === COLOR_PURPLE) return 2;
  if (c === COLOR_RED) return 3;
  if (c === COLOR_GRAY_DARK || c === '#6b7280') return 4;
  return 9;
}

// ==== USERS CACHE + batch ускорение ====
const userCache = new Map(); // id -> {id,name}

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

// batch: до 50 команд за вызов
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

// ==== GEOCODING cache ====
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

// ==== MANUAL -> FullCalendar event ====
function manualToFcEvent(m) {
  const id = String(m.id);
  const color = manualEventColor();
  const start = m.start || m.plannedInstall || m.assignedInstall || new Date().toISOString();
  const freeComment = (commentsStore && commentsStore[id]) ? String(commentsStore[id]) : '';
  const sortKey = colorToSortKey(color);

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
      stageId: 'MANUAL',
      isDone: false,

      // для совместимости с фронтом
      transferToShop: null,
      otkDate: null,

      plannedInstall: m.plannedInstall || null,
      assignedInstall: m.assignedInstall || null,
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

// ==== BITRIX CALL WRAPPER + DIAG ====
let lastBitrixOkAt = null;
let lastBitrixError = null;
let lastBitrixItemsCount = null;

async function bitrixPost(method, payload) {
  try {
    const { data } = await axiosInst.post(`${BITRIX_WEBHOOK_URL}${method}.json`, payload, { timeout: 20000 });
    lastBitrixError = null;
    lastBitrixOkAt = new Date().toISOString();
    return data;
  } catch (e) {
    const info = {
      at: new Date().toISOString(),
      message: e?.message || String(e),
      status: e?.response?.status || null,
      data: e?.response?.data || null
    };
    lastBitrixError = info;
    throw e;
  }
}

// ==== ЗАГРУЗКА ЭЛЕМЕНТОВ СМАРТ-ПРОЦЕССА ====
const SELECT_FIELDS = [
  'id', 'title', 'stageId', 'begindate', 'createdTime', 'movedTime', 'updatedTime', 'closedate', 'assignedById', 'parentId2',

  // ✅ “Дата передачи заказа в цех” — условие создания карточки
  'ufCrm8_1747916236564', // Дата передачи заказа в цех

  // ✅ “Дата прохождения ОТК” — условие перекрашивания из серого
  'ufCrm8_1747306212023', // Дата прохождения ОТК

  // Даты монтажа
  'ufCrm8_1744626911134', // Планируемая дата монтажа
  'ufCrm8_1747908559319', // Назначенные дата и время монтажа
  'ufCrm8_1758806212646', // Sys назначенная дата монтажа (если используете)
  'ufCrm8_1744639197488', // Дата завершения монтажа

  // Адрес/координаты
  'ufCrm8_1731616801',    // адрес
  'ufCrm8_1765526240853', // lat
  'ufCrm8_1765526264050', // lng
  'ufCrm8_1765980928',    // geo json coord

  // Карточка
  'ufCrm8_1758702287777', // № заказа
  'ufCrm8_1758702046420', // ФИО
  'ufCrm8_1744626408052', // телефон
  'ufCrm8_1758702057969', // телефон заказчика

  'ufCrm8_1732097971828', // комм. к монтажу
  'ufCrm8_1744638484827', // доп. примеч.

  'ufCrm8_1731616262',    // тип камня (ID)
  'ufCrm8_1759661234',    // фолбек/код

  'ufCrm8_1748001850227', // толщина

  'ufCrm8_1731617119',    // менеджер
  'ufCrm8_1748338492',    // монтажники

  'ufCrm8GoogleCalendarEvent' // event_id_google (DIAG)
];

async function loadItemsForOneStage(stageId) {
  let start = 0;
  let all = [];
  while (true) {
    const data = await bitrixPost('crm.item.list', {
      entityTypeId: ENTITY_TYPE_ID,
      start,
      filter: {
        '=categoryId': CATEGORY_ID,
        '=stageId': stageId
      },
      select: SELECT_FIELDS
    });

    const items = (data && data.result && data.result.items) ? data.result.items : [];
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
    // ✅ параллелим стадии (мягко)
    const parts = await mapLimit(stages, 3, loadItemsForOneStage);
    return parts.flat();
  }

  // allStages=true — большой проход по категории
  let start = 0;
  let all = [];
  while (true) {
    const data = await bitrixPost('crm.item.list', {
      entityTypeId: ENTITY_TYPE_ID,
      start,
      filter: { '=categoryId': CATEGORY_ID },
      select: SELECT_FIELDS
    });

    const items = (data && data.result && data.result.items) ? data.result.items : [];
    all = all.concat(items);

    if (data?.next === undefined || data?.next === null) break;
    start = data.next;
  }
  return all;
}

// ==== МАППИНГ В СОБЫТИЯ КАЛЕНДАРЯ (ПО ДОКУМЕНТУ ЛОГИКИ) ====
// Условие существования карточки:
// 1) Есть дата “Дата передачи заказа в цех” (ufCrm8_1747916236564)
// 2) Дата события берётся так:
//    - если есть “Дата завершения монтажа” -> ставим на неё, isDone=true, точку скрываем
//    - иначе если есть “Назначенные дата и время монтажа” -> ставим на неё
//    - иначе ставим на “Планируемую дату монтажа”
//    - если “Планируемой даты монтажа” нет и нет назначенной/завершения -> карточку не выводим
//
// Цвет:
//  - если стадия рекламации -> красный (и точка остаётся)
//  - иначе если “Дата прохождения ОТК” пустая -> “черная/серая” (тёмно-серый)
//  - иначе: самовывоз -> фиолетовый, иначе тип камня: акрил=синий, кварц/керамика/натуралка/кварц+акрил=зелёный
function mapItemsToEvents(items) {
  const events = [];
  const skipped = {
    noTransferToShop: 0,
    noStartDate: 0,
    noPlannedNoAssigned: 0
  };

  for (const item of items) {
    const idStr = String(item.id);

    const stageId = String(item.stageId || '').trim();

    const transferToShop = item.ufCrm8_1747916236564 || null;
    if (!truthyDate(transferToShop)) {
      skipped.noTransferToShop++;
      continue;
    }

    const plannedInstall = item.ufCrm8_1744626911134 || null;
    const assignedInstall = item.ufCrm8_1747908559319 || null;
    const sysAssignedInstall = item.ufCrm8_1758806212646 || null;
    const installDone = item.ufCrm8_1744639197488 || null;

    // “Назначенные дата и время монтажа” — приоритетнее sysAssigned (если оба есть)
    const assignedAny = firstNonEmpty(assignedInstall, sysAssignedInstall);

    // ✅ дата события
    let dateValue = null;
    if (truthyDate(installDone)) {
      dateValue = installDone;
    } else if (truthyDate(assignedAny)) {
      dateValue = assignedAny;
    } else if (truthyDate(plannedInstall)) {
      dateValue = plannedInstall;
    } else {
      skipped.noPlannedNoAssigned++;
      continue;
    }

    if (!truthyDate(dateValue)) {
      skipped.noStartDate++;
      continue;
    }

    // DONE — по наличию даты завершения монтажа
    const isDone = truthyDate(installDone);

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

    // ✅ выполнено: на карте не показываем
    const hideMarker = isDone;
    if (hideMarker) { lat = null; lng = null; }

    const orderNumber = String(item.ufCrm8_1758702287777 || '').trim();
    const customerName = String(item.ufCrm8_1758702046420 || '').trim();
    const rawTitle = String(item.title || '').trim();

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

    const color = pickColorByRules({
      stageId,
      otkDate,
      stoneTypeId,
      title: [rawTitle, title].filter(Boolean).join(' '),
      stoneText,
      installComment,
      extraComment
    });

    const sortKey = colorToSortKey(color);

    const freeComment = (commentsStore && commentsStore[idStr]) ? String(commentsStore[idStr]) : '';
    const googleEventId = String(item.ufCrm8GoogleCalendarEvent || '').trim() || null;

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
        stageId,
        isDone,

        // ключевые даты по логике
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

        hideMarker,

        createdTime: item.createdTime,
        movedTime: item.movedTime,
        updatedTime: item.updatedTime,
        assignedById: item.assignedById,
        parentDealId: item.parentId2
      }
    });
  }

  return { events, skipped };
}

// ==== Лёгкий кэш /api/events ====
let eventsCache = { version: null, key: null, at: 0, payload: null };
function cacheKeyOf(query) { return JSON.stringify(query || {}); }
function cacheFresh() { return Date.now() - (eventsCache.at || 0) < 60000; } // 60 секунд

// ==== API ====
app.get('/api/events', async (req, res) => {
  const t0 = Date.now();
  try {
    const qAllStagesRaw = String(req.query.allStages || '').toLowerCase();
    const qAllStages = (qAllStagesRaw === '1' || qAllStagesRaw === 'true' || qAllStagesRaw === 'yes');

    const forceAllStages = (req.query.allStages === undefined || req.query.allStages === null)
      ? DEFAULT_ALL_STAGES_ON
      : qAllStages;

    const stagesRaw = String(req.query.stages || '').trim();
    const stageIds = stagesRaw
      ? stagesRaw.split(',').map(s => s.trim()).filter(Boolean)
      : null;

    const key = cacheKeyOf({ allStages: forceAllStages, stages: stageIds });

    if (
      eventsCache.payload &&
      eventsCache.version === dataVersion &&
      eventsCache.key === key &&
      cacheFresh()
    ) {
      return res.json(eventsCache.payload);
    }

    const items = await loadAllSmartItems({ allStages: forceAllStages ? true : false, stageIds });
    lastBitrixItemsCount = items.length;

    // ✅ подтягиваем только нужных пользователей, но через batch
    const ids = [];
    for (const it of items) {
      const mid = normalizeId(it.ufCrm8_1731617119);
      if (mid) ids.push(mid);
      const arr = normalizeIdList(it.ufCrm8_1748338492);
      for (const x of arr) ids.push(x);
    }
    await fetchUsersByBatch(ids);

    const mapped = mapItemsToEvents(items);
    const bitrixEvents = mapped.events;

    const manualEvents = (manualStore || [])
      .filter(m => m && isManualId(m.id))
      .map(manualToFcEvent);

    const payload = {
      version: dataVersion,
      meta: {
        bitrixItems: lastBitrixItemsCount,
        eventsOut: bitrixEvents.length,
        manualOut: manualEvents.length,
        skipped: mapped.skipped,
        query: {
          allStages: forceAllStages,
          stages: stageIds
        },
        perfMs: Date.now() - t0
      },
      events: bitrixEvents.concat(manualEvents)
    };

    eventsCache = { version: dataVersion, key, at: Date.now(), payload };

    res.json(payload);
  } catch (e) {
    console.error('api/events error:', e?.response?.data || e?.message || e);
    res.status(502).json({
      error: 'bitrix_or_server_error',
      bitrix: lastBitrixError,
      details: e?.response?.data || String(e?.message || e)
    });
  }
});

app.get('/api/ping', (req, res) => res.json({ version: dataVersion }));

app.get('/api/diag', (req, res) => {
  res.json({
    ok: true,
    port: PORT,
    bitrixWebhookUrl: BITRIX_WEBHOOK_URL ? 'set' : 'empty',
    entityTypeId: ENTITY_TYPE_ID,
    categoryId: CATEGORY_ID,
    stages: {
      transferToShop: STAGE_TRANSFER_TO_SHOP,
      otkWarehouse: STAGE_OTK_WAREHOUSE,
      success: STAGE_SUCCESS,
      reclamationIn: STAGE_RECL_IN,
      reclamationWait: STAGE_RECL_WAIT
    },
    stageFilter: STAGE_FILTER,
    defaultAllStages: DEFAULT_ALL_STAGES_ON,
    lastBitrixOkAt,
    lastBitrixError,
    lastBitrixItemsCount,
    stores: {
      manualItems: manualStore.length,
      comments: Object.keys(commentsStore || {}).length,
      geocodeCacheKeys: Object.keys(geocodeCache || {}).length
    },
    usersCache: {
      size: userCache.size
    },
    eventsCache: {
      hasPayload: !!eventsCache.payload,
      at: eventsCache.at || null,
      ttlMs: 60000,
      version: eventsCache.version || null
    }
  });
});

app.post('/api/bitrix/hook', (req, res) => {
  dataVersion = Date.now();
  res.json({ ok: true, version: dataVersion });
});

// ===== GEOCODE API (для фронтового fallback) =====
app.get('/api/geocode', async (req, res) => {
  try {
    const address = String(req.query.address || '').trim();
    if (!address) return res.status(400).json({ ok: false, error: 'address_required' });

    const cached = getCoordsFromCacheByAddress(address);
    if (cached) {
      return res.json({ ok: true, lat: cached.lat, lng: cached.lng, source: cached.source });
    }

    // Nominatim (только тут, не в /api/events)
    const NOMINATIM_URL = process.env.NOMINATIM_URL || 'https://nominatim.openstreetmap.org/search';
    const UA = process.env.NOMINATIM_UA || 'calendar-map/1.0';

    const { data } = await axiosInst.get(NOMINATIM_URL, {
      timeout: 12000,
      headers: { 'User-Agent': UA },
      params: {
        format: 'json',
        limit: 1,
        q: address
      }
    });

    const top = Array.isArray(data) ? data[0] : null;
    const lat = toNum(top?.lat);
    const lng = toNum(top?.lon);

    if (lat === null || lng === null) {
      return res.json({ ok: false, error: 'not_found' });
    }

    // пишем в кеш
    const key = normAddrKey(address);
    geocodeCache[key] = { lat, lng, at: new Date().toISOString() };
    await saveGeocodeCache();

    dataVersion = Date.now();
    res.json({ ok: true, lat, lng, source: 'nominatim' });
  } catch (e) {
    console.error('geocode error:', e?.message || e);
    res.status(500).json({ ok: false, error: 'geocode_failed' });
  }
});

// ===== COMMENTS API =====
app.get('/api/comment/:id', (req, res) => {
  const id = String(req.params.id || '');
  const text = (commentsStore && commentsStore[id]) ? String(commentsStore[id]) : '';
  res.json({ ok: true, id, text });
});

app.put('/api/comment/:id', async (req, res) => {
  try {
    const id = String(req.params.id || '');
    const text = String((req.body && req.body.text) ? req.body.text : '');
    commentsStore[id] = text;
    await saveCommentsStore();
    dataVersion = Date.now();
    res.json({ ok: true, version: dataVersion, id, text });
  } catch (e) {
    console.error('comment save error:', e?.message || e);
    res.status(500).json({ error: 'comment_save_failed' });
  }
});

// ===== MANUAL API =====
app.get('/api/manual', (req, res) => res.json({ version: dataVersion, items: manualStore || [] }));

app.post('/api/manual', async (req, res) => {
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

    // только cache (быстро)
    if ((obj.lat === null || obj.lng === null) && obj.address) {
      const cached = getCoordsFromCacheByAddress(obj.address);
      if (cached) {
        obj.lat = cached.lat;
        obj.lng = cached.lng;
      }
    }

    manualStore.push(obj);
    await saveManualStore();

    dataVersion = Date.now();
    res.json({ ok: true, version: dataVersion, event: manualToFcEvent(obj) });
  } catch (e) {
    console.error(e?.message || e);
    res.status(500).json({ error: 'manual_create_failed' });
  }
});

app.delete('/api/manual/:id', async (req, res) => {
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

    dataVersion = Date.now();
    res.json({ ok: true, version: dataVersion });
  } catch (e) {
    console.error(e?.message || e);
    res.status(500).json({ error: 'manual_delete_failed' });
  }
});

// ==== START ====
(async () => {
  await loadManualStore();
  await loadCommentsStore();
  await loadGeocodeCache();

  app.listen(PORT, () => {
    console.log(`API listening on port ${PORT}`);
    console.log(`Bitrix webhook: ${BITRIX_WEBHOOK_URL}`);
    console.log(`EntityTypeId=${ENTITY_TYPE_ID} CategoryId=${CATEGORY_ID}`);
    console.log(`Default allStages=${DEFAULT_ALL_STAGES_ON}`);
    console.log(`StageFilter=${JSON.stringify(STAGE_FILTER)}`);
    console.log(`Manual store: ${MANUAL_FILE} (${manualStore.length} items)`);
    console.log(`Comments store: ${COMMENTS_FILE} (${Object.keys(commentsStore || {}).length} items)`);
    console.log(`Geocode cache: ${GEOCODE_CACHE_FILE} (${Object.keys(geocodeCache || {}).length} keys)`);
    console.log(`DIAG: http://localhost:${PORT}/api/diag`);
  });
})();
