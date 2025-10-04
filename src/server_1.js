// server.js (patched)
// - add MySQL pool (mysql2/promise)
// - implement MQTT -> MySQL for moisture/climate/irrigation
// - add broadcast() using existing SSE buffer

require('dotenv').config();

const express = require('express');
const mqtt = require('mqtt');
const cors = require('cors');
const helmet = require('helmet');
const morgan = require('morgan');
const { createPool } = require('mysql2/promise');

// ====== Config from ENV ======
const PORT = parseInt(process.env.PORT || '4000', 10);

// รองรับหลาย URL คั่นด้วย comma (หากไม่กำหนด ใช้เซ็ตมาตรฐานของ EMQX)
const MQTT_URLS = (process.env.MQTT_URLS || [
  'mqtt://broker.emqx.io:1883',
  'mqtts://broker.emqx.io:8883',
  'ws://broker.emqx.io:8083/mqtt',
  'wss://broker.emqx.io:8084/mqtt'
].join(',')).split(',').map(s => s.trim()).filter(Boolean);

// topic เดียวหรือหลายอันคั่นด้วย comma
const SUB_TOPICS = (process.env.MQTT_TOPIC || process.env.MQTT_SUB_TOPICS || '/buu/iot/+/moisture/state')
  .split(',').map(s => s.trim()).filter(Boolean);

const MSG_BUFFER_MAX = parseInt(process.env.MSG_BUFFER_MAX || '1000', 10);
const RECONNECT_PERIOD = parseInt(process.env.MQTT_RECONNECT_PERIOD || '2000', 10); // ms
const CONNECT_TIMEOUT  = parseInt(process.env.MQTT_CONNECT_TIMEOUT  || '15000', 10); // ms
const KEEPALIVE        = parseInt(process.env.MQTT_KEEPALIVE        || '30', 10);    // sec
const MAX_ATTEMPTS_PER_URL = parseInt(process.env.MAX_ATTEMPTS_PER_URL || '5', 10);

// ใช้เฉพาะกรณีทดสอบ TLS/WSS หลังไฟร์วอลล์/พร็อกซี (โปรดปิดในโปรดักชัน)
const MQTT_TLS_INSECURE = process.env.MQTT_TLS_INSECURE === '1';

// auth gating: จะเปิดใช้ก็ต่อเมื่อมี “ทั้งคู่”
const HAS_AUTH = !!(process.env.MQTT_USERNAME && process.env.MQTT_PASSWORD);

// ====== MySQL Pool (สำคัญ: ต้องมาก่อน handler) ======
const pool = createPool({
  host: process.env.DB_HOST,
  port: Number(process.env.DB_PORT || 3306),
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  database: process.env.DB_NAME,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
  timezone: 'Z', // เก็บเวลาเป็น UTC
});

pool.query('SELECT 1').then(() => {
  console.log('[DB] connected');
}).catch(e => {
  console.error('[DB] connect error:', e && e.message ? e.message : e);
});

// ====== State / SSE ======
let mqttClient = null;
let currentUrlIdx = 0;
let currentUrl = MQTT_URLS[currentUrlIdx];
let everConnected = false;
let attemptsForCurrentUrl = 0;

const messages = [];
const sseClients = new Set();

const nowIso = () => new Date().toISOString();

function pushMessage(msg) {
  messages.push(msg);
  if (messages.length > MSG_BUFFER_MAX) messages.shift();
  const data = JSON.stringify(msg);
  for (const res of sseClients) res.write(`data: ${data}\n\n`);
}

// ให้ frontend ใช้งานง่าย: ใช้ชื่อ broadcast
function broadcast(obj) { pushMessage(obj); }

function rotateUrl() {
  currentUrlIdx = (currentUrlIdx + 1) % MQTT_URLS.length;
  currentUrl = MQTT_URLS[currentUrlIdx];
  attemptsForCurrentUrl = 0;
  console.warn(`[MQTT] rotating to next URL => ${currentUrl}`);
}

function makeMqttOptions(url) {
  const clientId = `express-mqtt-api_${Math.random().toString(16).slice(2)}`;
  const options = {
    clientId,
    clean: true,
    reconnectPeriod: RECONNECT_PERIOD,
    connectTimeout: CONNECT_TIMEOUT,
    keepalive: KEEPALIVE,
    protocolVersion: 4, // MQTT 3.1.1
    resubscribe: true,
    will: {
      topic: '/system/express-mqtt-api/status',
      payload: JSON.stringify({ clientId, status: 'offline', ts: nowIso() }),
      qos: 0, retain: false
    }
  };
  if (HAS_AUTH) {
    options.username = process.env.MQTT_USERNAME;
    options.password = process.env.MQTT_PASSWORD;
    console.log('[MQTT] auth: on');
  } else {
    console.log('[MQTT] auth: off');
  }
  if (url.startsWith('mqtts://') || url.startsWith('wss://')) {
    options.rejectUnauthorized = !MQTT_TLS_INSECURE;
  }
  return options;
}





// ====== Helpers (mapping / validation) ======
async function smKeyToSid(pool, smKey) {
  try {
    const [[row]] = await pool.query('SELECT sid FROM soil_sensor_code WHERE code=? LIMIT 1', [smKey]);
    if (row?.sid) return Number(row.sid);
  } catch {}
  const n = Number(String(smKey).replace(/^sm/, ''));
  return Number.isFinite(n) ? n : null;
}
function toDateOrNow(v) { try { return v ? new Date(v) : new Date(); } catch { return new Date(); } }
function toNumOrNull(v) { const n = Number(v); return Number.isFinite(n) ? n : null; }
function toStateEnum(v) {
  if (typeof v === 'string') { const s = v.trim().toUpperCase(); if (s === 'ON' || s === 'OFF') return s; }
  if (v === true || v === 1 || v === '1') return 'ON';
  if (v === false || v === 0 || v === '0') return 'OFF';
  return null;
}
function parseVidFromTopic(topic) { const m = topic.match(/\/valve\/(\d+)\b/i); return m ? Number(m[1]) : null; }


// --- DB entities per schema (smartfarm) ---
// app_user.user_id → plot.user_id → zone.pid → soilSensor.zid
// weatherStation.wid → climate.wid (NOT NULL, FK)
const DEFAULT_WID = Number(process.env.CLIMATE_DEFAULT_WID || '1');

// ดึง wid จาก topic เช่น .../weather/123/state
function parseWidFromWeatherTopic(topic) {
  const m = /\/weather\/(\d+)\/state\b/i.exec(topic);
  return m ? Number(m[1]) : null;
}

async function resolveWidForClimate(sensorItem, topic) {
  // 1) จาก payload
  if (Number.isFinite(Number(sensorItem?.wid))) return Number(sensorItem.wid);
  // 2) จาก topic
  const wFromTopic = parseWidFromWeatherTopic(topic);
  if (Number.isFinite(wFromTopic)) return wFromTopic;
  // 3) จาก .env
  if (Number.isFinite(DEFAULT_WID)) return DEFAULT_WID;
  return null;
}

async function ensureWidExists(wid) {
  const [[row]] = await pool.query('SELECT 1 FROM weatherStation WHERE wid=? LIMIT 1', [wid]);
  return !!row;
}




// ====== INSERTs ======
async function insertMoistureRows(pool, rows) {
  if (!rows.length) return 0;
  const sql = `INSERT INTO moisture (sid, moisture, temperature, measured_at) VALUES (?, ?, ?, ?)`;
  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();
    for (const r of rows) {
      await conn.execute(sql, [
        r.sid,
        r.moisture,
        (typeof r.temperature === 'number') ? r.temperature : null,
        r.measured_at instanceof Date ? r.measured_at : new Date(r.measured_at)
      ]);
    }
    await conn.commit();
    return rows.length;
  } catch (e) {
    await conn.rollback(); throw e;
  } finally { conn.release(); }
}

async function insertClimateRows(pool, rows) {
  if (!rows.length) return 0;
  const sql = `
    INSERT INTO climate
      (temperature, humidity, vpd, dew_point, wind_speed, wind_gust,
       wind_direction, wind_max_day, solar_radiation, uv_index, rain_day,
       measured_at, wid)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `;
  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();
    for (const r of rows) {
      await conn.execute(sql, [
        toNumOrNull(r.temperature),
        toNumOrNull(r.humidity),
        toNumOrNull(r.vpd),
        toNumOrNull(r.dew_point),
        toNumOrNull(r.wind_speed),
        toNumOrNull(r.wind_gust),
        toNumOrNull(r.wind_direction),
        toNumOrNull(r.wind_max_day),
        toNumOrNull(r.solar_radiation),
        toNumOrNull(r.uv_index),
        toNumOrNull(r.rain_day),
        (r.measured_at instanceof Date ? r.measured_at : new Date(r.measured_at)),
        r.wid
      ]);
    }
    await conn.commit();
    return rows.length;
  } catch (e) {
    await conn.rollback(); throw e;
  } finally { conn.release(); }
}


async function insertIrrigationRows(pool, rows) {
  if (!rows.length) return 0;
  const sql = `INSERT INTO irrigation (state, date_time, vid) VALUES (?, ?, ?)`;
  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();
    for (const r of rows) {
      await conn.execute(sql, [
        r.state, // 'ON' | 'OFF'
        r.date_time instanceof Date ? r.date_time : new Date(r.date_time),
        r.vid
      ]);
    }
    await conn.commit();
    return rows.length;
  } catch (e) {
    await conn.rollback(); throw e;
  } finally { conn.release(); }
}

// ====== MQTT Connect / Handlers ======
function connectMqtt(url) {
  if (!url) { console.error('[MQTT] No URL to connect. Check MQTT_URLS.'); return; }
  if (mqttClient) { try { mqttClient.end(true); } catch {} mqttClient = null; }
  everConnected = false; attemptsForCurrentUrl = 0;

  const options = makeMqttOptions(url);
  console.log(`[MQTT] connecting ${url} ...`);
  mqttClient = mqtt.connect(url, options);

  mqttClient.on('connect', () => {
    everConnected = true; attemptsForCurrentUrl = 0;
    console.log(`[MQTT] connected: ${url}`);
    SUB_TOPICS.forEach(t => {
      mqttClient.subscribe(t, { qos: 0 }, (err) => {
        if (err) console.error(`[MQTT] subscribe error: ${t}`, err.message);
        else console.log(`[MQTT] subscribed: ${t}`);
      });
    });
    mqttClient.publish('/system/express-mqtt-api/status',
      JSON.stringify({ clientId: options.clientId, status: 'online', ts: nowIso(), url }),
      { qos: 0, retain: false }
    );
  });

  const maybeRotate = (reason) => {
    if (everConnected) return; // เคยต่อสำเร็จแล้ว → ปล่อย auto-reconnect
    attemptsForCurrentUrl += 1;
    console.warn(`[MQTT] ${reason} (attempt ${attemptsForCurrentUrl}/${MAX_ATTEMPTS_PER_URL})`);
    if (attemptsForCurrentUrl >= MAX_ATTEMPTS_PER_URL) {
      currentUrlIdx = (currentUrlIdx + 1) % MQTT_URLS.length;
      currentUrl = MQTT_URLS[currentUrlIdx];
      attemptsForCurrentUrl = 0;
      console.warn(`[MQTT] rotating to next URL => ${currentUrl}`);
      setTimeout(() => connectMqtt(currentUrl), 1000);
    }
  };

  mqttClient.on('offline', () => { maybeRotate('offline'); });
  mqttClient.on('close',   () => { maybeRotate('connection closed'); });
  mqttClient.on('error',   (err) => { console.error('[MQTT] error:', err?.message || err); maybeRotate('error'); });

  mqttClient.on('message', async (topic, payloadBuf, packet) => {
    const payloadText = payloadBuf.toString('utf8').trim();
    console.log('[MQTT] RECV', { topic, retain: !!packet?.retain, len: payloadBuf.length });

    // 1) parse JSON
    let msg = null;
    try {
      msg = JSON.parse(payloadText);
    } catch {
      console.warn('[MQTT] Non-JSON payload => ignored');
      return;
    }

    try {
      // ---------------- CASE 1: sensor[] ----------------
      if (Array.isArray(msg?.sensor) && msg.sensor.length > 0) {
        const type = String(msg.sensor[0]?.type || '').toLowerCase();

        // 1A) moisture: smN (moisture) + stN (temperature) → moisture table
        if (type === 'moisture') {
          const rows = [];

          // msg.sensor เป็น array ของ object; แต่ละ object อาจมี sm1, st1, sm2, st2, ...
          for (const s of msg.sensor) {
            const when = toDateOrNow(s.date);

            // อุณหภูมิ fallback ระดับกลุ่ม (ถ้าไม่พบ stN)
            const genericTemp =
              (typeof s.temperature === 'number') ? s.temperature :
              (typeof s.temp === 'number')        ? s.temp        : null;

            // ไล่ทุกคีย์ → หา smN แล้วจับคู่ stN ของ N เดียวกัน
            for (const [k, v] of Object.entries(s)) {
              const m = /^sm(\d+)$/.exec(k);
              if (!m || typeof v !== 'number') continue;

              const idx = Number(m[1]); // N จาก smN
              // map smN -> sid (ถ้าไม่มี mapping ในตาราง code จะ fallback เป็นเลข N)
              const sidFromMap = await smKeyToSid(pool, `sm${idx}`);
              const sid = (sidFromMap ?? idx);

              // เลือก temperature: stN หรือ tN ถ้าไม่มีใช้ generic
              let temp = genericTemp;
              if (typeof s[`st${idx}`] === 'number')      temp = s[`st${idx}`];
              else if (typeof s[`t${idx}`]  === 'number') temp = s[`t${idx}`];

              rows.push({
                sid,
                moisture: v, // ค่าจาก smN
                temperature: (typeof temp === 'number') ? temp : null,
                measured_at: when,
              });
            }
          }

          if (!rows.length) {
            console.warn('[MQTT] moisture payload has no smN keys => ignored');
            return;
          }

          // INSERT ด้วย helper
          const nInserted = await insertMoistureRows(pool, rows);
          console.log(`[MQTT] moisture inserted: ${nInserted} row(s)`);

          // ✅ broadcast SSE ทีละ record ให้ frontend
          for (const r of rows) {
            broadcastEvent({
              type: 'moisture',
              sid: r.sid,
              moisture: r.moisture,
              temperature: r.temperature ?? null,
              measured_at: (
                r.measured_at instanceof Date ? r.measured_at : new Date(r.measured_at)
              ).toISOString(),
            });
          }

          return;
        }

        // 1B) weather → climate table
        if (type === 'weather') {
          const rows = [];

          for (const s of msg.sensor) {
            const when = toDateOrNow(s.date);
            const wid  = await resolveWidForClimate(s, topic);

            if (!Number.isFinite(wid)) {
              console.warn('[MQTT] climate: wid missing and no DEFAULT_WID -> skip item');
              continue;
            }
            const ok = await ensureWidExists(wid);
            if (!ok) {
              console.warn(`[MQTT] climate: wid ${wid} not found in weatherStation -> skip item`);
              continue;
            }

            // normalize ตัวเลข/เวลา
            rows.push({
              temperature:     toNumOrNull(s.temperature),
              humidity:        toNumOrNull(s.humidity),
              vpd:             toNumOrNull(s.vpd),
              dew_point:       toNumOrNull(s.dew_point),
              wind_speed:      toNumOrNull(s.wind_speed),
              wind_gust:       toNumOrNull(s.wind_gust),
              wind_direction:  toNumOrNull(s.wind_direction),
              wind_max_day:    toNumOrNull(s.wind_max_day),
              solar_radiation: toNumOrNull(s.solar_radiation),
              uv_index:        toNumOrNull(s.uv_index),
              rain_day:        toNumOrNull(s.rain_day),
              measured_at:     when,
              wid,
            });
          }

          if (!rows.length) {
            console.warn('[MQTT] climate: no valid rows -> ignored');
            return;
          }

          // INSERT ด้วย helper
          const n = await insertClimateRows(pool, rows);
          console.log(`[MQTT] climate inserted: ${n} row(s)`);

          // ✅ broadcast SSE ทีละ record ให้ frontend
          for (const r of rows) {
            broadcastEvent({
              type: 'weather',
              wid: r.wid,
              temperature: r.temperature,
              humidity: r.humidity,
              vpd: r.vpd,
              dew_point: r.dew_point,
              wind_speed: r.wind_speed,
              wind_gust: r.wind_gust,
              wind_direction: r.wind_direction,
              wind_max_day: r.wind_max_day,
              solar_radiation: r.solar_radiation,
              uv_index: r.uv_index,
              rain_day: r.rain_day,
              measured_at: (
                r.measured_at instanceof Date ? r.measured_at : new Date(r.measured_at)
              ).toISOString(),
            });
          }

          return;
        }

        console.warn('[MQTT] unknown sensor.type => ignored:', type);
        return;
      }


      // ---------------- CASE 2: irrigation[] ----------------
      // if (Array.isArray(msg?.irrigation) && msg.irrigation.length > 0) {
      //   // (แก้บั๊กเดิมที่อ้าง msg.sensor[0]) → อ่านจาก irrigation เอง
      //   const type = String(msg.irrigation[0]?.type || '').toLowerCase();
      //   if (type === 'irrigation') {
      //     const rows = [];
      //     for (const it of msg.irrigation) {
      //       const state = toStateEnum(it.state);
      //       const when  = toDateOrNow(it.date || it.date_time);
      //       // ต้องมี vid: ดึงจาก payload หรือ topic (/valve/<vid>/…)
      //       const vid   = Number.isFinite(Number(it.vid)) ? Number(it.vid) : parseVidFromTopic(topic);
      //       if (!state)                { console.warn('[MQTT] irrigation: invalid state => skip', it.state); continue; }
      //       if (!Number.isFinite(vid)) { console.warn('[MQTT] irrigation: missing vid => skip'); continue; }
      //       rows.push({ state, date_time: when, vid });
      //     }
      //     if (!rows.length) { console.warn('[MQTT] irrigation payload has no valid items => ignored'); return; }
      //     const n = await insertIrrigationRows(pool, rows);
      //     console.log(`[MQTT] irrigation inserted: ${n} row(s)`);
      //     return;
      //   }
      // }

      console.warn('[MQTT] unsupported payload shape => ignored');
    } catch (e) {
      console.error('[MQTT] ingest error:', e?.message || e);
    }
  });

}

// ====== Express App ======
const app = express();
app.use(helmet());
// app.use(cors());
app.use(cors({ origin: ['http://127.0.0.1:5173','http://localhost:5173'] }))

app.use(express.json({ limit: '1mb' }));
app.use(morgan('dev'));
app.use(express.json());

// const server = http.createServer(app)
// const wss = new WebSocketServer({ server, path: '/ws' })
// wss.on('connection', ws => ws.send(JSON.stringify({ type: 'hello', message: 'connected' })))


// ---- Health ----
app.get('/api/health', async (req, res) => {
  try {
    await pool.query('SELECT 1');
    res.json({ ok: true });
  } catch (e) {
    req.log.error(e);
    res.status(500).json({ ok: false, error: 'DB error' });
  }
});

// ---- Soil sensors listing (join with zone for context) ----
app.get('/api/soil-sensors', async (req, res) => {
  const { uid, pid, zid, sid } = req.query;
  const where = [], params = [];

  if (sid) { where.push('s.sid = ?'); params.push(Number(sid)); }
  if (zid) { where.push('s.zid = ?'); params.push(Number(zid)); }
  if (pid) { where.push('z.pid = ?'); params.push(Number(pid)); }
  if (uid) { where.push('p.user_id = ?'); params.push(Number(uid)); }

  const whereSql = where.length ? `WHERE ${where.join(' AND ')}` : '';
  const [rows] = await pool.query(`
    SELECT s.sid, s.name, s.type, s.modbus_id, s.zid, z.pid, p.user_id AS uid
    FROM soilSensor s
    JOIN zone z ON z.zid = s.zid
    JOIN plot p ON p.pid = z.pid
    ${whereSql}
    ORDER BY s.sid
  `, params);

  res.json(rows);
});

app.get('/api/soil-sensors/latest', async (req, res) => {
  const { uid, pid, zid, sid } = req.query;
  const where = [], params = [];

  if (sid) { where.push('s.sid = ?'); params.push(Number(sid)); }
  if (zid) { where.push('s.zid = ?'); params.push(Number(zid)); }
  if (pid) { where.push('z.pid = ?'); params.push(Number(pid)); }
  if (uid) { where.push('p.user_id = ?'); params.push(Number(uid)); }

  const whereSql = where.length ? `WHERE ${where.join(' AND ')}` : '';

  const [rows] = await pool.query(`
    SELECT s.sid, s.name, s.type, s.zid, z.pid, p.user_id AS uid,
      (SELECT m.moisture    FROM moisture m WHERE m.sid=s.sid ORDER BY m.measured_at DESC LIMIT 1) AS moisture,
      (SELECT m.temperature FROM moisture m WHERE m.sid=s.sid ORDER BY m.measured_at DESC LIMIT 1) AS temperature,
      (SELECT m.measured_at FROM moisture m WHERE m.sid=s.sid ORDER BY m.measured_at DESC LIMIT 1) AS measured_at
    FROM soilSensor s
    JOIN zone z ON z.zid = s.zid
    JOIN plot p ON p.pid = z.pid
    ${whereSql}
    ORDER BY s.sid
  `, params);

  res.json(rows);
});





// โครงสร้างที่คืนจะเป็น: users[] -> plots[] -> zones[] -> sensors[] เพื่อป้อนให้ 4 dropdown
app.get('/api/filters/hierarchy', async (req, res) => {
  const [rows] = await pool.query(`
    SELECT 
      u.user_id              AS uid,
      u.name                 AS user_name,
      p.pid                  AS pid,
      p.name                 AS plot_name,
      z.zid                  AS zid,
      z.name                 AS zone_name,
      s.sid                  AS sid,
      s.name                 AS sensor_name,
      s.type                 AS sensor_type
    FROM app_user u
    JOIN plot p ON p.user_id = u.user_id
    JOIN zone z ON z.pid = p.pid
    JOIN soilSensor s ON s.zid = z.zid
    ORDER BY u.user_id, p.pid, z.zid, s.sid
  `);

  const users = [];
  const byUid = new Map(), byPid = new Map(), byZid = new Map();

  for (const r of rows) {
    if (!byUid.has(r.uid)) {
      const u = { uid: r.uid, name: r.user_name, plots: [] };
      byUid.set(r.uid, u); users.push(u);
    }
    const u = byUid.get(r.uid);

    if (!byPid.has(r.pid)) {
      const p = { pid: r.pid, name: r.plot_name, zones: [] };
      byPid.set(r.pid, p); u.plots.push(p);
    }
    const p = byPid.get(r.pid);

    if (!byZid.has(r.zid)) {
      const z = { zid: r.zid, name: r.zone_name, sensors: [] };
      byZid.set(r.zid, z); p.zones.push(z);
    }
    const z = byZid.get(r.zid);

    z.sensors.push({ sid: r.sid, name: r.sensor_name, type: r.sensor_type });
  }

  res.json({ users });
});



// ---- History window ----
app.get('/api/soil-sensors/:sid/history', async (req, res) => {
  const { sid } = req.params;
  const { from, to, limit = 500 } = req.query;
  const where = ['m.sid = ?'];
  const params = [Number(sid)];
  if (from) { where.push('m.measured_at >= ?'); params.push(new Date(from)); }
  if (to)   { where.push('m.measured_at <= ?'); params.push(new Date(to)); }

  const [rows] = await pool.query(`
    SELECT m.mid, m.moisture, m.temperature, m.measured_at
    FROM moisture m
    WHERE ${where.join(' AND ')}
    ORDER BY m.measured_at DESC
    LIMIT ?
  `, [...params, Number(limit)]);
  res.json(rows);
});

// ---- Manual ingest (testing) ----
app.post('/api/ingest-moisture', async (req, res) => {
  const { sid, moisture, temperature = null, measured_at = null } = req.body || {};
  if (!sid || typeof moisture !== 'number') {
    return res.status(400).json({ ok: false, error: 'sid and numeric moisture required' });
  }
  await pool.execute(
    `INSERT INTO moisture (sid, moisture, temperature, measured_at) VALUES (?, ?, ?, ?)`,
    [Number(sid), moisture, temperature, measured_at ? new Date(measured_at) : new Date()]
  );
  broadcast({ type: 'moisture_ingest', sid: Number(sid), moisture, temperature });
  res.json({ ok: true });
});


// Real-time Events (SSE)
let clients = [];

app.get('/api/soil-sensors/events', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  // ถ้าไม่ได้ใช้ Vite proxy และเปิดตรงจาก 5173 ให้เปิด CORS ตรงนี้ด้วย
  // res.setHeader('Access-Control-Allow-Origin', 'http://127.0.0.1:5173');

  res.flushHeaders?.(); // บางเวอร์ชันของ express มี

  // ส่ง hello ทันทีเพื่อพิสูจน์ว่ามาแล้ว
  res.write(`event: hello\ndata: ${JSON.stringify({ type: 'hello', t: Date.now() })}\n\n`);

  clients.push(res);
  req.on('close', () => {
    clients = clients.filter(c => c !== res);
    res.end();
  });
});

function broadcastEvent(obj) {
  const data = `data: ${JSON.stringify(obj)}\n\n`; // ต้องมี \n\n ปิด event
  clients.forEach(c => c.write(data));
}

app.post('/api/dev/push', (req, res) => {
  broadcastEvent({ type: 'dev', ...req.body, t: Date.now() });
  res.json({ ok: true });
});

// ---- WS hello ----
// wss.on('connection', (ws) => {
//   ws.send(JSON.stringify({ type: 'hello', message: 'connected' }));
// });



// Health
app.get('/health', (req, res) => {
  res.json({
    server: 'ok',
    time: nowIso(),
    mqtt: {
      url: currentUrl,
      connected: !!(mqttClient && mqttClient.connected),
      reconnecting: !!(mqttClient && mqttClient.reconnecting),
      attemptsForCurrentUrl,
      maxAttemptsPerUrl: MAX_ATTEMPTS_PER_URL
    },
    subscriptions: SUB_TOPICS,
    buffer: { size: messages.length, max: MSG_BUFFER_MAX },
    auth: HAS_AUTH ? 'on' : 'off'
  });
});

// Recent messages
app.get('/messages', (req, res) => {
  const limit = Math.max(1, Math.min(parseInt(req.query.limit || '100', 10), MSG_BUFFER_MAX));
  const start = Math.max(messages.length - limit, 0);
  res.json(messages.slice(start));
});

// SSE stream
app.get('/stream', (req, res) => {
  res.set({ 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive' });
  res.flushHeaders();
  res.write(`event: hello\n`);
  res.write(`data: ${JSON.stringify({ ts: nowIso(), note: 'SSE connected' })}\n\n`);
  sseClients.add(res);
  req.on('close', () => { sseClients.delete(res); try { res.end(); } catch {} });
});

// Publish (for testing)
app.post('/publish', (req, res) => {
  const { topic, payload, qos = 0, retain = false } = req.body || {};
  if (!topic || typeof topic !== 'string') return res.status(400).json({ error: 'topic is required (string)' });
  if (!mqttClient || !mqttClient.connected) return res.status(503).json({ error: 'MQTT not connected' });
  const payloadStr = (typeof payload === 'string') ? payload : JSON.stringify(payload ?? {});
  mqttClient.publish(topic, payloadStr, { qos, retain }, (err) => {
    if (err) return res.status(500).json({ error: err.message });
    res.json({ ok: true, topic, qos, retain, bytes: Buffer.byteLength(payloadStr, 'utf8') });
  });
});




// Start HTTP server + MQTT
const server = app.listen(PORT, '0.0.0.0', () => {
  console.log(JSON.stringify({ port: PORT, host: '0.0.0.0', msg: 'API server listening' }));
});
connectMqtt(currentUrl);

// Graceful shutdown
function shutdown() {
  console.log('Shutting down...');
  try { server.close(); } catch {}
  try { if (mqttClient) mqttClient.end(true); } catch {}
  process.exit(0);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
