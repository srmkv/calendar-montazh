// server.js – смарт-процесс + статика index.html + цвет из карточки (стадия)

const express = require('express');
const cors = require('cors');
const axios = require('axios');
const dotenv = require('dotenv');
const path = require('path');

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json());

// ==== НАСТРОЙКИ BITRIX + ПОРТ ====
const PORT = process.env.PORT || 3050;
const BITRIX_WEBHOOK_URL = process.env.BITRIX_WEBHOOK_URL; // например: https://karatspb.bitrix24.ru/rest/44/6nwr6cy757vhl2nd/
const ENTITY_TYPE_ID = Number(process.env.BITRIX_SMART_ENTITY_TYPE_ID) || 141;

// тип статусов для stageId из crm.item.fields (statusType":"DYNAMIC_141_STAGE_14")
const STAGE_ENTITY_ID = process.env.BITRIX_STAGE_ENTITY_ID || 'DYNAMIC_141_STAGE_14';

// ==== ОТДАЁМ index.html И СТАТИКУ ====
// __dirname = /var/www/html/calendar-map/b24-calendar-api
// index.html лежит на уровень выше: /var/www/html/calendar-map/index.html

const publicDir = path.join(__dirname, '..'); // /var/www/html/calendar-map

app.use(express.static(publicDir));

app.get('/', (req, res) => {
  res.sendFile(path.join(publicDir, 'index.html'));
});

// ==== ЗАГРУЗКА ЭЛЕМЕНТОВ СМАРТ-ПРОЦЕССА ====

async function loadAllSmartItems() {
  let start = 0;
  let all = [];

  while (true) {
    const { data } = await axios.post(
      `${BITRIX_WEBHOOK_URL}crm.item.list.json`,
      {
        entityTypeId: ENTITY_TYPE_ID,
        start,
        select: [
          'id',
          'title',
          'stageId',
          'begindate',
          'closedate',

          // даты для календаря
          'ufCrm8_1747908559319', // Назначенные дата и время монтажа
          'ufCrm8_1744626911134', // Планируемая дата монтажа
          'ufCrm8_1744626362431', // Дата замера

          // адрес
          'ufCrm8_1731616801',    // Адрес объекта

          // инфа по заказу/клиенту
          'ufCrm8_1758702287777', // № Заказа
          'ufCrm8_1758702046420', // ФИО заказчика
          'ufCrm8_1758702057969'  // Телефон заказчика
        ]
      }
    );

    const items = (data.result && data.result.items) || [];
    all = all.concat(items);

    if (data.next === undefined || data.next === null) {
      break;
    }

    start = data.next;
  }

  return all;
}

// ==== ЗАГРУЗКА ЦВЕТОВ СТАДИЙ ====

async function loadStageColors() {
  try {
    const { data } = await axios.post(
      `${BITRIX_WEBHOOK_URL}crm.status.list.json`,
      {
        filter: {
          ENTITY_ID: STAGE_ENTITY_ID
        }
      }
    );

    const list = data.result || [];
    const map = {};

    // ожидаем, что STATUS_ID совпадает со stageId карточки
    for (const s of list) {
      if (s.STATUS_ID) {
        map[s.STATUS_ID] = s.COLOR || null;
      }
    }

    return map;
  } catch (e) {
    console.error('Error loading stage colors:', e?.response?.data || e);
    return {};
  }
}

// ==== МАППИНГ В СОБЫТИЯ КАЛЕНДАРЯ ====

function mapItemsToEvents(items, stageColors) {
  const defaultColor = '#3b82f6';

  return items
    .map((item) => {
      const installDate =
        item.ufCrm8_1747908559319 ||
        item.ufCrm8_1744626911134 ||
        item.ufCrm8_1744626362431 ||
        item.begindate;

      if (!installDate) return null;

      const orderNumber = item.ufCrm8_1758702287777;
      const customerName = item.ufCrm8_1758702046420;
      const customerPhone = item.ufCrm8_1758702057969;

      let title = item.title || '';
      const parts = [];
      if (orderNumber) parts.push(`№${orderNumber}`);
      if (customerName) parts.push(customerName);

      if (!title && parts.length) {
        title = parts.join(' / ');
      } else if (title && parts.length) {
        title = `${title} – ${parts.join(' / ')}`;
      }

      const address = item.ufCrm8_1731616801 || null;

      const stageId = item.stageId;
      const stageColor = (stageId && stageColors[stageId]) || null;
      const color = stageColor || defaultColor;

      return {
        id: item.id,
        title: title || `Заказ #${item.id}`,
        start: installDate,
        end: installDate,
        color,
        extendedProps: {
          color,
          stageId,
          orderNumber,
          customerName,
          customerPhone,
          address,
          raw: item
        }
      };
    })
    .filter(Boolean);
}

// ==== API ДЛЯ КАЛЕНДАРЯ ====

app.get('/api/events', async (req, res) => {
  try {
    const [items, stageColors] = await Promise.all([
      loadAllSmartItems(),
      loadStageColors()
    ]);

    const events = mapItemsToEvents(items, stageColors);
    res.json(events);
  } catch (e) {
    console.error(e?.response?.data || e);
    res.status(500).json({ error: 'server_error' });
  }
});

app.listen(PORT, () => {
  console.log(`Smart-process calendar API + static UI listening on port ${PORT}`);
});

