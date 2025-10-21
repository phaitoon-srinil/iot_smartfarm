// server.js (patched)
// - add MySQL pool (mysql2/promise)
// - implement MQTT -> MySQL for moisture/climate/irrigation
// - add broadcast() using existing SSE buffer


const path = require('path');
require('dotenv').config({
  path: path.resolve(__dirname, '../.env'),
  override: true,        // <-- ให้ .env ทับ ENV ที่ตั้งไว้ใน shell
});

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
// const SUB_TOPICS = (process.env.MQTT_TOPIC || process.env.MQTT_SUB_TOPICS || '/buu/iot/+/moisture/state')
//   .split(',').map(s => s.trim()).filter(Boolean);

// topic เดียวหรือหลายอันคั่นด้วย comma
const SUB_TOPICS = (process.env.MQTT_TOPIC || process.env.MQTT_SUB_TOPICS ||
  // moisture + valve states ทั้งสองรูปแบบ
  '/buu/iot/+/moisture/state,' +
  '/buu/iot/+/+/control/state/#' +          // จับ .../state/valveN และ .../state/vN (ถ้ามี)
  '/buu/iot/+/weather/state/#'
  // '/buu/iot/+/house+/state/#'        // จับ .../valve/state/vN
).split(',').map(s => s.trim()).filter(Boolean);


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

// ฟังก์ชันช่วยแปลงเวลา UTC → เวลาประเทศไทย
function toThailandTime(isoString) {
  const utcDate = new Date(isoString);
  // เพิ่ม 7 ชั่วโมง (7 * 60 * 60 * 1000 ms)
  return new Date(utcDate.getTime() + 7 * 60 * 60 * 1000);
  // return new Date(utcDate.getTime() + 0 * 60 * 60 * 1000);
}


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
  const USER = safeEnvStr(process.env.MQTT_USERNAME);
  const PASS = safeEnvStr(process.env.MQTT_PASSWORD);

  const clientId = `express-mqtt-api_${Math.random().toString(16).slice(2)}`;
  const options = {
    clientId,
    clean: true,
    reconnectPeriod: RECONNECT_PERIOD,
    connectTimeout: CONNECT_TIMEOUT,
    keepalive: KEEPALIVE,
    // ลองทั้ง MQTT v3.1.1 และ fallback v5 หากจำเป็น
    protocolVersion: 4,
    resubscribe: true,
    will: {
      topic: '/system/express-mqtt-api/status',
      payload: JSON.stringify({ clientId, status: 'offline', ts: nowIso() }),
      qos: 0, retain: false
    }
  };

  if (USER && PASS) {
    options.username = USER;
    // ใช้ Buffer เพื่อกัน encoding edge case
    options.password = Buffer.from(PASS, 'utf8');
    console.log('[MQTT] auth: on (len=%d)', PASS.length);
  } else {
    console.log('[MQTT] auth: off');
  }

  if (url.startsWith('mqtts://') || url.startsWith('wss://')) {
    options.rejectUnauthorized = !MQTT_TLS_INSECURE;
  }
  return options;
}




// ====== Helpers (mapping / validation) ======
// ====== Valve binding helpers ======
function parseUserAndHouse(topic) {
  const m = String(topic||'').match(/^\/buu\/iot\/([^/]+)\/house(\d+)\//i);
  return m ? { user: m[1], house: Number(m[2]) } : null;
}
// function parseValveIndex(topic) {
//   // รองรับ .../state/valveN และ .../valve/state/vN
//   const t = String(topic||'');
//   const mA = t.match(/\/state\/valve(\d+)(?:\/|$)/i);
//   const mB = t.match(/\/valve\/state\/v(\d+)(?:\/|$)/i);
//   const num = mA ? Number(mA[1]) : (mB ? Number(mB[1]) : NaN);
//   return Number.isFinite(num) ? num : null;
// }

function parseValveIndex(topic) {
  const t = String(topic || '');
  const mA = t.match(/\/state\/v(\d+)(?:\/|$)/i);          // ✅ ใหม่
  const mB = t.match(/\/state\/valve(\d+)(?:\/|$)/i);      // เดิม
  const mC = t.match(/\/valve\/state\/v(\d+)(?:\/|$)/i);   // เดิม
  const num = mA ? mA[1] : (mB ? mB[1] : (mC ? mC[1] : null));
  return num ? Number(num) : null;
}

function buildBindingTopic(user, house, vIndex) {
  // binding topic มาตรฐาน: .../houseX/state/vY (ไม่มีคำว่า 'valve' ใน path)
  return `/buu/iot/${user}/house${house}/control/state/v${vIndex}`;
}
async function lookupVidForValve(pool, topic) {
  const uh = parseUserAndHouse(topic);
  const vIndex = parseValveIndex(topic);
  if (!uh || vIndex == null) return null;
  const bindingTopic = buildBindingTopic(uh.user, uh.house, vIndex);
  const field = `v${vIndex}`;
  console.log("bindingTopic:",bindingTopic," field:",field);

  const [[row]] = await pool.query(
    `SELECT vid FROM mqtt_valve_binding WHERE topic=? AND field=? LIMIT 1`,
    [bindingTopic, field]
  );
  return row?.vid ?? null;
}


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
        // (r.measured_at instanceof Date ? r.measured_at : new Date(r.measured_at)),
        r.measured_at,
        r.wid
      ]);
    }
    await conn.commit();
    return rows.length;
  } catch (e) {
    await conn.rollback(); throw e;
  } finally { conn.release(); }
}

async function insertHourlyWeatherRows(pool, rows) {
  if (!rows.length) return 0;
  const sql = `
    INSERT INTO hourly_weather
      (temperature, humidity, wind_speed, solar_radiation, eto, measured_at, wid)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `;
  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();
    for (const r of rows) {
      await conn.execute(sql, [
        toNumOrNull(r.temperature),
        toNumOrNull(r.humidity),
        toNumOrNull(r.wind_speed),
        toNumOrNull(r.solar_radiation),
        toNumOrNull(r.eto),
        r.measured_at,   // string 'YYYY-MM-DD HH:MM:SS' หรือ Date -> แนะนำแปลงเป็นสตริงก่อน
        r.wid
      ]);
    }
    await conn.commit();
    return rows.length;
  } catch (e) {
    await conn.rollback(); throw e;
  } finally { conn.release(); }
}

async function insertHourlyIrrigationRows(pool, rows) {
  if (!rows.length) return 0;
  const sql = `
    INSERT INTO hourly_irrigation
      (eto, kc, etc, rain, pe, net, pe_factor, vpd, measured_at, wid)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `;
  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();
    for (const r of rows) {
      await conn.execute(sql, [
        toNumOrNull(r.eto),
        toNumOrNull(r.kc),
        toNumOrNull(r.etc),
        toNumOrNull(r.rain),
        toNumOrNull(r.pe),
        toNumOrNull(r.net),
        toNumOrNull(r.pe_factor),
        toNumOrNull(r.vpd),
        r.measured_at,   // ใช้สตริง 'YYYY-MM-DD HH:MM:SS' ที่เตรียมไว้แล้ว
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


// ====== Insert/Watering persistence ======
async function startWateringIfNotOpenYet(pool, vid, when) {
  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();
    const [[row]] = await conn.query(
      `SELECT id FROM watering WHERE vid=? AND stop_at IS NULL ORDER BY id DESC LIMIT 1`,
      [vid]
    );
    if (!row) {
      await conn.query(
        `INSERT INTO watering (start_at, vid) VALUES (?, ?)`,
        [toMySQLDateTime(when), vid]
      );
    }
    await conn.commit();
  } catch (e) { await conn.rollback(); throw e; } finally { conn.release(); }
}

async function stopLatestWatering(pool, vid, when) {
  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();
    const [[row]] = await conn.query(
      `SELECT id FROM watering WHERE vid=? AND stop_at IS NULL ORDER BY id DESC LIMIT 1`,
      [vid]
    );
    if (row) {
      await conn.query(
        `UPDATE watering SET stop_at=? WHERE id=?`,
        [toMySQLDateTime(when), row.id]
      );
    }
    await conn.commit();
  } catch (e) { await conn.rollback(); throw e; } finally { conn.release(); }
}


// ========== CONFIG ==========
const PAYLOAD_TZ_OFFSET_MIN = Number(process.env.PAYLOAD_TZ_OFFSET_MIN ?? 0); // +7h = 420 นาที
// มีโซนเวลามาในสตริงไหม? (เช่น ...Z, +07:00, -08:00, +0700)
function hasExplicitTZ(str) {
  return /Z$|[+-]\d{2}:?\d{2}$/.test(String(str||'').trim());
}

// ========== TIME UTILS ==========
function toMySQLDateTime(d) {
  const pad = n => String(n).toString().padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ` +
         `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

function parseFlexibleDate(s) {
  if (!s) return null;
  const str = String(s).trim();

  const d0 = new Date(str);
  if (!isNaN(d0)) return d0;

  const m = str.match(/^(\d{2})\/(\d{2})\/(\d{2})\s+(\d{2}):(\d{2})(?::(\d{2}))?$/);
  if (m) {
    let [, yy, mo, dd, hh, mm, ss='00'] = m;
    const year = 2000 + Number(yy);
    return new Date(year, Number(mo)-1, Number(dd), Number(hh), Number(mm), Number(ss));
  }

  const n = Number(str);
  if (!Number.isNaN(n)) return new Date(n > 1e12 ? n : n*1000);

  return null;
}

// เลื่อนเวลาเป็นนาที (+420 = +7 ชม.)
function shiftMinutes(d, minutes) {
  return new Date(d.getTime() + minutes * 60 * 1000);
}

// ========== BINDING LOOKUP ==========
// ค้นหา wid จากการผูก topic → wid
// helper regex: /weather/state/{id}
function parseFieldFromWeatherTopic(topic) {
  const t = String(topic || '');
  // ดึงทุกอย่างหลัง /weather/state/ จนถึงสิ้นสุดสตริงหรือ /
  const m = t.match(/\/weather\/state\/([^/]+)(?:\/|$)/);
  return m ? m[1] : null;
}

// helper regex: /weather/state/{id}
function parseBeforeIdFromWeatherTopic(topic) {
  const t = String(topic || '');
  // จับทุกอย่างก่อน /{id} โดย {id} = ส่วนสุดท้ายหลัง "/"
  const m = t.match(/^(.*)\/[^/]+$/);
  return m ? m[1] : null;
}

async function lookupWidForTopic(db, fieldTopic, fieldId) {
  // 1) payload.wid มาก่อน
  // if (payloadWid !== undefined && payloadWid !== null && payloadWid !== '') {
  //   const widNum = Number(payloadWid);
  //   return Number.isFinite(widNum) ? widNum : payloadWid; // รองรับทั้งเลขและ string
  // }

  // // 2) หา {id} จาก topic /weather/state/{id}
  // const fieldId = parseFieldFromWeatherTopic(topic);
  // const fieldTopic = parseBeforeIdFromWeatherTopic(topic);

  if (fieldId) {
    // 3) ลองหาใน binding ด้วย field == {id}
    const [rows] = await db.query(
      `SELECT wid FROM mqtt_weather_binding WHERE topic = ? AND field = ? LIMIT 1`,
      [fieldTopic, fieldId]
    );
    // if (rows?.length) {
    //   return rows[0].wid;
    // }
    // // ถ้าไม่มี binding ให้คืนค่า fieldId กลับ (string หรือ number ก็ได้)
    // return fieldId;
    console.log("Parse topic:",fieldTopic," Id:",fieldId, "wid:",rows?.[0]?.wid ?? null);
    return rows?.[0]?.wid ?? null;
  }

  // // 4) หาใน binding ด้วย topic ตรง ๆ
  // const [rows] = await db.query(
  //   `SELECT wid FROM mqtt_weather_binding WHERE topic = ? LIMIT 1`,
  //   [String(topic)]
  // );
  // if (rows?.length) {
  //   return rows[0].wid;
  // }

  // // 5) fallback DEFAULT_WID
  // return DEFAULT_WID;
}

async function lookupSidForField(db, topic, field) {
  // ปกติ topic ใน binding มี '/' นำหน้า ให้ normalize แล้วเทียบตรง ๆ
  const t = String(topic || '').trim();
  const f = String(field || '').trim();     // 'sm1', 'sm2', ...
  const [rows] = await db.query(
    `SELECT sid FROM mqtt_sensor_binding WHERE topic = ? AND field = ? LIMIT 1`,
    [t, f]
  );
  console.log("topic:",t," field:",f, "sid:",rows?.[0]?.sid ?? null);
  return rows?.[0]?.sid ?? null;
}


// ====== MQTT Connect / Handlers ======

function safeEnvStr(v) {
  return (v == null) ? '' : String(v).trim();
}

function urlWithAuth(baseUrl, user, pass) {
  if (!user) return baseUrl;
  const u = encodeURIComponent(user);
  const p = encodeURIComponent(pass || '');
  // ถ้า base เป็น mqtt://host:1883 -> แทรก auth ก่อน host
  return baseUrl.replace(/^(\w+:\/\/)(.*)$/i, (_, proto, rest) => `${proto}${u}:${p}@${rest}`);
}

function connectMqtt(url) {
  if (!url) { console.error('[MQTT] No URL to connect.'); return; }
  if (mqttClient) { try { mqttClient.end(true); } catch {} mqttClient = null; }
  everConnected = false; attemptsForCurrentUrl = 0;

  // ถ้าเปิด auth ให้ลองฉีด credential เข้า URL ด้วย (สำรอง)
  const USER = safeEnvStr(process.env.MQTT_USERNAME);
  const PASS = safeEnvStr(process.env.MQTT_PASSWORD);
  const candidateUrl = (USER ? urlWithAuth(url, USER, PASS) : url);

  const options = makeMqttOptions(url);
  console.log(`[MQTT] connecting ${candidateUrl} ...`);
  console.log('[ENV] USER =', JSON.stringify(process.env.MQTT_USERNAME||''));
  console.log('[ENV] PASS =', JSON.stringify(process.env.MQTT_PASSWORD||''));


  mqttClient = mqtt.connect(candidateUrl, options);


  mqttClient.on('connect', () => {
    everConnected = true; attemptsForCurrentUrl = 0;
    console.log(`[MQTT] connected: ${url}`);
    console.log('USER hex =', Buffer.from(process.env.MQTT_USERNAME||'').toString('hex'));
    console.log('PASS hex =', Buffer.from(process.env.MQTT_PASSWORD||'').toString('hex'));

    SUB_TOPICS.forEach(t => {
      mqttClient.subscribe(t, { qos: 1 }, (err) => {
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
  console.log('[MQTT] RECV', { topic, retain: !!packet?.retain, payload: payloadText, len: payloadBuf.length});

  
  // 1) ต้องเป็น JSON เท่านั้น
  let msg = null;
  // ---- FAST PATH: valve state (non-JSON text) ----
  // if (/^\/buu\/iot\/[^/]+\/house\d+\/(?:state\/valve\d+|valve\/state\/v\d+)(?:\/|$)/i.test(topic)) {
  // if (/^\/buu\/iot\/[^/]+\/house\d+\/(?:state\/(?:v\d+|valve\d+)|valve\/state\/v\d+)(?:\/|$)/i.test(topic)) {

  if (/^\/buu\/iot\/[^/]+\/house\d+\/control\/state\/v\d+(?:\/|$)/i.test(topic)) {

    const text = payloadBuf.toString('utf8').trim();

    // ดึงเวลา (ถ้ามี) และสถานะ ON/OFF
    // รองรับทั้ง "VALVE1_ON" และ "YYYY-MM-DD HH:MM:SS VALVE1_ON"
    // const m = text.match(/^(?:(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+)?VALVE(\d+)_(ON|OFF)\b/i);
    // ใหม่: รองรับทั้ง YYYY-MM-DD HH:MM:SS และ YY/MM/DD HH:MM:SS
    const m = text.match(
      /^(?:(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}|\d{2}\/\d{2}\/\d{2}\s+\d{2}:\d{2}:\d{2})\s+)?VALVE(\d+)_(ON|OFF)\b/i
    );

    // console.log("m: ",m);

    if (!m) { console.warn('[MQTT] valve-state: payload not recognized:', text); return; }

    const whenStr = m[1] || '';                   // อาจว่างได้
    const vnum    = Number(m[2]);                 // 1..n
    const state   = m[3].toUpperCase();           // ON|OFF


    // เวลา: ถ้าไม่เจอรูปแบบเต็ม ลอง parser เดิม; ถ้ายังไม่ได้ ใช้ now()
    // let when = whenStr ? new Date(whenStr) : null;
    // if (!when || isNaN(when)) {
    //   // เผื่อ payload แบบ "25/10/04 11:14:18 VALVE1_ON"
    //   const head17 = text.slice(0, 17); // "YY/MM/DD HH:MM:SS"
    //   when = parseFlexibleDate(head17) || new Date();                  // มีในไฟล์เดิม
    //   if (!hasExplicitTZ(head17)) when = shiftMinutes(when, PAYLOAD_TZ_OFFSET_MIN); // ชดเชยโซนเวลา
    // }

    let when = null;
    if (whenStr) {
      // ลอง parse โดยตรงทั้ง 2 รูปแบบ (ISO และ YY/MM/DD)
      when = parseFlexibleDate(whenStr) || new Date(whenStr);
    }
    if (!when || isNaN(when)) {
      // เผื่อ payload ไม่มี timestamp ข้างหน้า ใช้ now()
      when = new Date();
    }
    // หากสตริงเวลาไม่มี timezone ให้ชดเชย OFFSET (+420 นาที = UTC+7) ตามค่า .env
    if (whenStr && !hasExplicitTZ(whenStr)) {
      when = shiftMinutes(when, PAYLOAD_TZ_OFFSET_MIN);
    }

    // map → vid ผ่าน binding
    const vid = await lookupVidForValve(pool, topic);

    console.log("valve state:",state," when:",whenStr," vid:",vid);

    if (!vid) { console.warn('[MQTT] valve-state: cannot resolve vid for', topic); return; }

    // เขียนลง watering: ON → insert start_at; OFF → update stop_at
    if (state === 'ON') {
      await startWateringIfNotOpenYet(pool, Number(vid), when);
      console.log(`[MQTT] watering start vid=${vid} at ${toMySQLDateTime(when)}`);
    } else {
      await stopLatestWatering(pool, Number(vid), when);
      console.log(`[MQTT] watering stop  vid=${vid} at ${toMySQLDateTime(when)}`);
    }
    return; // จบ fast-path
  }


  try {
    msg = JSON.parse(payloadText);
  } catch {
    console.warn('[MQTT] Non-JSON payload => ignored');
    return;
  }

  try {
    // ================= CASE 1: sensor[] =================
    const msg = JSON.parse(payloadText);
    // if (!Array.isArray(msg?.sensor) || !msg.sensor.length) return;
    if (Array.isArray(msg?.sensor) && msg.sensor.length > 0) {

      const type = String(msg.sensor[0]?.type || '').toLowerCase();

      // ---------- SOIL MOISTURE ----------
      if (type === 'moisture') {
        const rows = [];

        for (const s of msg.sensor) {
          // 1) parse เวลา + ชดเชยโซนเวลา (ค่าเริ่มต้น +420 นาที = +7 ชม.)
          const rawWhen = s.measured_at || s.date || s.datetime || s.ts || s.time;
          let dt = parseFlexibleDate(rawWhen) || new Date();

          // ชดเชย “เฉพาะ” กรณีที่ไม่มีโซนเวลาในสตริง และ offset ≠ 0
          if (!hasExplicitTZ(rawWhen) && PAYLOAD_TZ_OFFSET_MIN !== 0) {
            dt = new Date(dt.getTime() + PAYLOAD_TZ_OFFSET_MIN * 60 * 1000);
          }        
          const measuredAt = toMySQLDateTime(dt);

          // 2) อุณหภูมิ (ถ้ามี)
          const genericTemp =
            (typeof s.temperature === 'number') ? s.temperature :
            (typeof s.temp === 'number')        ? s.temp        : null;
 
          // 3) ไล่ key smN → แม็ป sid ด้วย (topic, field)
          for (const [k, v] of Object.entries(s)) {
            const m = /^sm(\d+)$/.exec(k);
            if (!m) continue;
            const idx = Number(m[1]);
            const moistureVal = Number(v);
            if (!Number.isFinite(moistureVal)) continue;

            // หา sid จาก binding: (topic, 'sm1'|'sm2'|...)
            const field = `sm${idx}`;
            const sid = await lookupSidForField(pool, topic, field);

            if (!sid) {
              console.warn(`[INGEST] no binding for ${field} @ ${topic} -> skip`);
              continue;
            }

            // temp เฉพาะช่อง (stN/tN) > temp รวม
            let temp = genericTemp;
            if (typeof s[`st${idx}`] === 'number')      temp = s[`st${idx}`];
            else if (typeof s[`t${idx}`]  === 'number') temp = s[`t${idx}`];

            // ph เฉพาะช่อง (phN/tN) > temp รวม
            let ph = null;
            if (typeof s[`ph${idx}`] === 'number')      ph = s[`ph${idx}`];
 
            let ec = null;
            if (typeof s[`ec${idx}`] === 'number')      ec = s[`ec${idx}`];
           let n = null;
            if (typeof s[`n${idx}`] === 'number')      n = s[`n${idx}`];

            let p = null;
            if (typeof s[`p${idx}`] === 'number')      p = s[`p${idx}`];

           let potassium = 0;
            if (typeof s[`k${idx}`] === 'number')      potassium = s[`k${idx}`];

            
            rows.push({
              sid,
              name: field,
              moisture: moistureVal,
              temperature: (typeof temp === 'number') ? temp : null,
              ph: (typeof ph === 'number')?ph:null,
              ec: (typeof ec === 'number')?ec:null,
              n: (typeof n === 'number')?n:null,
              p: (typeof p === 'number')?p:null,
              k: (typeof potassium === 'number')?potassium:null,
              measured_at: measuredAt,   // เก็บเป็นสตริง MySQL พร้อมแล้ว
            });
          }
        }

        if (!rows.length) return;

        // 4) insert ทีละแถว
        for (const r of rows) {
          await pool.query(
            `INSERT INTO moisture (moisture, temperature, ph, ec_us_cm, n, p, k, measured_at, sid)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            [r.moisture, r.temperature, r.ph, r.ec, r.n, r.p, r.k, r.measured_at, r.sid]
          );
        }
        console.log(`[MQTT] moisture inserted: ${rows.length} row(s)`);

        // 5) broadcast SSE
        for (const r of rows) {
          broadcastEvent({
            type: 'moisture',
            sid: r.sid,
            name: r.name,
            moisture: r.moisture,
            temperature: r.temperature ?? null,
            measured_at: new Date(r.measured_at).toISOString(),
          });
        }
        return;
      }

      // ---------- LIVE WEATHER / CLIMATE ----------
      if (type === 'weather') {
        const rows = [];

        for (const s of msg.sensor) {
          // เวลา: ถ้า payload ไม่มี ให้ใช้เวลาปัจจุบัน
          const rawWhen = s.measured_at || s.date || s.datetime || s.ts || s.time;
          let dt = parseFlexibleDate(rawWhen) || new Date();

          // ชดเชยโซนเวลาเฉพาะกรณีไม่มี TZ ในสตริง และตั้งค่า OFFSET ไว้
          if (!hasExplicitTZ(rawWhen) && PAYLOAD_TZ_OFFSET_MIN !== 0) {
            dt = new Date(dt.getTime() + PAYLOAD_TZ_OFFSET_MIN * 60 * 1000);
          }      
          const measuredAt = toMySQLDateTime(dt);
          console.log("measure_at:",measuredAt);



          // หา wid: payload > topic (/weather/state) > binding(field) > DEFAULT_WID
          let wid = await lookupWidForTopic(pool, topic, s.name);
          if (!Number.isFinite(Number(wid))) {
            // หากยังไม่ใช่ตัวเลข (กรณี payload ใส่เป็น string ที่ไม่มีใน binding)
            wid = DEFAULT_WID;
          } else {
            wid = Number(wid);
          }

          // กันพลาด: wid ต้องมีจริงใน weatherStation
          if (!await ensureWidExists(wid)) {
            console.warn(`[MQTT] weather: wid ${wid} not found -> skip item`);
            continue;
          }

          // map ชื่อคีย์จาก ESP32 -> คอลัมน์ใน climate
          rows.push({
            name:    s.name,
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
            measured_at:     measuredAt,   // Date object (จะถูกแปลงเป็น 'YYYY-MM-DD HH:mm:SS' ใน insert helper)
            wid
          });
        }

        if (!rows.length) return;

        const n = await insertClimateRows(pool, rows);
        console.log(`[MQTT] climate inserted: ${n} row(s)`);

        // Broadcast แบบเรียลไทม์ (SSE/WS)
        for (const r of rows) {
          broadcastEvent({
            type: 'weather',
            wid: r.wid,
            name: r.name,
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
            measured_at: (r.measured_at instanceof Date ? r.measured_at : new Date(r.measured_at)).toISOString(),
          });
        }

        return;
      }

      // ---------- HOURLY WEATHER ----------
      if (type === 'hourly') {
        const rows = [];

        for (const s of msg.sensor) {
          // เวลา
          const rawWhen = s.measured_at || s.date || s.datetime || s.ts || s.time;
          let dt = parseFlexibleDate(rawWhen) || new Date();
          if (!hasExplicitTZ(rawWhen) && PAYLOAD_TZ_OFFSET_MIN !== 0) {
            dt = new Date(dt.getTime() + PAYLOAD_TZ_OFFSET_MIN * 60 * 1000);
          }
          const measuredAt = toMySQLDateTime(dt);

          // หา wid: payload > topic (/weather/state/{id}) > binding(field) > DEFAULT_WID
          let widAny = await lookupWidForTopic(pool, topic, s.name);
          let wid = Number(widAny);
          if (!Number.isFinite(wid)) wid = DEFAULT_WID;

          if (!await ensureWidExists(wid)) {
            console.warn(`[MQTT] hourly: wid ${wid} not found -> skip`);
            continue;
          }

          // map key จาก ESP32 -> คอลัมน์ตาราง
          rows.push({
            h_temperature:     s.h_temperature,
            h_humidity:        s.h_humidity,
            h_wind_speed:      s.h_wind_speed,
            h_solar_radiation: s.h_solar_radiation,
            h_eto:             s.h_ETo,
            measured_at:     measuredAt,
            wid
          });
        }

        if (!rows.length) return;

        const n = await insertHourlyWeatherRows(pool, rows);
        console.log(`[MQTT] hourly_weather inserted: ${n} row(s)`);

        // (ถ้าต้องการ) broadcast ให้ frontend
        for (const r of rows) {
          broadcastEvent({
            type: 'hourly',
            wid: r.wid,
            h_temperature: r.h_temperature,
            h_humidity: r.h_humidity,
            h_wind_speed: r.h_wind_speed,
            h_solar_radiation: r.h_solar_radiation,
            h_eto: r.h_eto,
            measured_at: new Date(r.measured_at).toISOString(),
          });
        }


        return;
      }

    }

    if (Array.isArray(msg?.irrigation) && msg.irrigation.length > 0) {
      const type = String(msg.irrigation[0]?.type || '').toLowerCase();

      //---------- HOURLY IRRIGATION ------------------
      if (type === 'hourly') {
        const rows = [];

        for (const it of msg.irrigation) {
          // 1) เวลา: ใช้ it.date เป็นหลัก; fallback ไป measured_at/date/datetime/ts/time
          const rawWhen = it.measured_at || it.date || it.datetime || it.ts || it.time;
          let dt = parseFlexibleDate(rawWhen) || new Date();
          if (!hasExplicitTZ(rawWhen) && PAYLOAD_TZ_OFFSET_MIN !== 0) {
            dt = new Date(dt.getTime() + PAYLOAD_TZ_OFFSET_MIN * 60 * 1000);
          }
          const measuredAt = toMySQLDateTime(dt);

          // 2) หา wid ตามลำดับ: payload.wid → topic (/weather/state/{id}) → binding(field) → DEFAULT_WID
          let widAny = await lookupWidForTopic(pool, topic, it.name);
          let wid = Number(widAny);
          if (!Number.isFinite(wid)) wid = DEFAULT_WID;

          // 3) ตรวจว่ามี weatherStation จริง
          if (!await ensureWidExists(wid)) {
            console.warn(`[MQTT] hourly_irrigation: wid ${wid} not found -> skip`);
            continue;
          }

          // 4) แมพคีย์จาก ESP32 → คอลัมน์ DB
          //    ESP32 keys: ETo, Kc, ETc, Rain, Pe, Net, Pe_factor, Irr
          rows.push({
            eto:        it.ETo,
            kc:         it.Kc,
            etc:        it.ETc,
            rain:       it.Rain,
            pe:         it.Pe,
            net:        it.Net,
            pe_factor:  it.Pe_factor,
            vpd: it.vpd,
            measured_at: measuredAt,
            wid
          });
        }

        if (!rows.length) return;

        const n = await insertHourlyIrrigationRows(pool, rows);
        console.log(`[MQTT] hourly_irrigation inserted: ${n} row(s)`);

        // (ไม่บังคับ) broadcast เพื่อแจ้ง frontend
        for (const r of rows) {
          broadcast({
            type: 'hourly_irrigation',
            wid: r.wid,
            eto: r.eto,
            kc: r.kc,
            etc: r.etc,
            rain: r.rain,
            pe: r.pe,
            net: r.net,
            pe_factor: r.pe_factor,
            irrigation: r.irrigation,
            measured_at: new Date(r.measured_at).toISOString(),
          });
        }
        return;
      }
    }



    // ================= CASE 2: irrigation[] =================
    // (ปลดคอมเมนต์เมื่อคุณพร้อมใช้ topic/รูปแบบนี้)
    // if (Array.isArray(msg?.irrigation) && msg.irrigation.length > 0) {
    //   const type = String(msg.irrigation[0]?.type || '').toLowerCase();
    //   if (type === 'irrigation') {
    //     const rows = [];
    //     for (const it of msg.irrigation) {
    //       const state = toStateEnum(it.state);
    //       const when  = parseFlexibleDate(it.measured_at || it.date || it.date_time) || new Date();
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

    // console.warn('[MQTT] unsupported payload shape => ignored');
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


// ================= Historical Moisture =================
// GET /api/history/moisture/aggregate
// query:
//   start, end          : ISO (UTC) เช่น '2025-10-10T00:00:00Z'
//   granularity         : '15m' | 'hour' | 'day'   (default = 'hour')
//   stat                : 'avg' | 'min' | 'max'    (default = 'avg')
//   pid (จำเป็น), zid (อาจว่าง = ทุกโซนใน plot)
//   sids (CSV optional) : เช่น "1,2,3" ถ้าไม่ส่ง -> ใช้ทุก sid ตาม scope pid/zid
app.get('/api/history/moisture/aggregate', async (req, res) => {
  try {
    const { start, end, granularity='hour', stat='avg', pid, zid, sids } = req.query;

    if (!start || !end || !pid) {
      return res.status(400).json({ error: 'missing start/end/pid' });
    }

    const g = String(granularity).toLowerCase();
    const statFn = ({avg:'AVG', min:'MIN', max:'MAX'}[String(stat).toLowerCase()] || 'AVG');

    // ความกว้าง bucket (วินาที)
    const bucketSec =
      g === '15m' ? 15*60 :
      g === 'day' ? 24*60*60 :
      60*60; // 'hour' (default)

    // ทำลิสต์ sid filter
    const sidList = (String(sids||'').trim())
      ? String(sids).split(',').map(s => s.trim()).filter(Boolean).map(n => Number(n)).filter(Number.isFinite)
      : [];

    // สร้างเงื่อนไข scope: plot / zone / (sid ถ้าเลือก)
    // หมายเหตุ: measured_at เก็บเป็น UTC แล้ว (server ทำ timezone='Z' ใน pool)
    // Bucket ด้วย UNIX_TIMESTAMP()
    let sql = `
      SELECT
        FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(m.measured_at)/?)*?)           AS bucket_at,
        m.sid                                                              AS sid,
        ${statFn}(m.moisture)                                              AS moisture
      FROM moisture m
      JOIN soilSensor s ON s.sid = m.sid
      JOIN zone z       ON z.zid = s.zid
      JOIN plot p       ON p.pid = z.pid
      WHERE m.measured_at BETWEEN ? AND ?
        AND p.pid = ?
    `;
    const params = [bucketSec, bucketSec, new Date(start), new Date(end), Number(pid)];

    if (zid) {
      sql += ` AND z.zid = ? `;
      params.push(Number(zid));
    }
    if (sidList.length) {
      sql += ` AND m.sid IN (${sidList.map(() => '?').join(',')}) `;
      params.push(...sidList);
    }

    sql += `
      GROUP BY bucket_at, m.sid
      ORDER BY bucket_at ASC, m.sid ASC
    `;

    const [rows] = await pool.query(sql, params);
    // โครงคืนค่า: [{bucket_at: '2025-10-10 01:00:00', sid: 3, moisture: 41.2}, ...]
    res.json({ rows, bucketSec, stat: statFn });
  } catch (e) {
    console.error('[history.moisture]', e);
    res.status(500).json({ error: 'internal error' });
  }
});



// ================= Historical Weather =================
// GET /api/history/weather/aggregate
// query:
//   start, end        : ISO UTC เช่น '2025-10-10T00:00:00Z' (จำเป็น)
//   granularity       : '15m' | 'hour' | 'day'   (default 'hour')
//   stat              : 'avg' | 'min' | 'max'    (default 'avg')
//   pid               : plot id (จำเป็น), zid (อาจว่าง)
//   wids              : CSV ของ wid (อาจว่าง → ใช้ทุกสถานีใน scope pid/zid)
//   metric            : 'temperature' | 'humidity' | 'vpd'
//                       | 'temp_hum' | 'both'
//                       | 'temp_hum_vpd' | 'all'
app.get('/api/history/weather/aggregate', async (req, res) => {
  try {
    const {
      start, end,
      granularity = 'hour',
      stat = 'avg',
      pid, zid,
      wids,
      metric = 'temperature'
    } = req.query;

    if (!start || !end || !pid) {
      return res.status(400).json({ error: 'missing start/end/pid' });
    }

    const g = String(granularity).toLowerCase();
    const statFn = ({ avg: 'AVG', min: 'MIN', max: 'MAX' }[String(stat).toLowerCase()] || 'AVG');

    const metricRaw = String(metric).toLowerCase();
    const bothMode  = (metricRaw === 'temp_hum' || metricRaw === 'both');
    const allMode   = (metricRaw === 'temp_hum_vpd' || metricRaw === 'all'); // << ใหม่

    // ความกว้าง bucket (วินาที)
    const bucketSec =
      g === '15m' ? 15 * 60 :
      g === 'day' ? 24 * 60 * 60 :
      60 * 60; // 'hour' (default)

    const widList = (String(wids || '').trim())
      ? String(wids).split(',').map(s => s.trim()).filter(Boolean).map(Number).filter(Number.isFinite)
      : [];

    // ---------- โหมดรวม 3 ค่า: temperature + humidity + vpd ----------
    if (allMode) {
      let sql = `
        SELECT
          FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(c.measured_at)/?)*?) AS bucket_at,
          c.wid                                                   AS wid,
          ${statFn}(c.temperature)                                 AS temperature,
          ${statFn}(c.humidity)                                    AS humidity,
          ${statFn}(c.vpd)                                         AS vpd
        FROM climate c
        JOIN weatherStation w ON w.wid = c.wid
        JOIN zone z           ON z.zid = w.zid
        JOIN plot p           ON p.pid = z.pid
        WHERE c.measured_at BETWEEN ? AND ?
          AND p.pid = ?
      `;
      // const params = [bucketSec, bucketSec, new Date(start), new Date(end), Number(pid)];
      const params = [
        bucketSec, bucketSec,
        toThailandTime(start),
        toThailandTime(end),
        Number(pid)
      ];

      if (zid) { sql += ` AND z.zid = ? `; params.push(Number(zid)); }
      if (widList.length) {
        sql += ` AND c.wid IN (${widList.map(() => '?').join(',')}) `;
        params.push(...widList);
      }
      sql += ` GROUP BY bucket_at, c.wid ORDER BY bucket_at ASC, c.wid ASC `;

console.log("Start:",start," End:",end);
const paramsForLog = [...params];  // shallow copy
const finalSQL = sql.replace(/\?/g, () => {
  const val = paramsForLog.shift();
  return typeof val === 'string'
    ? `'${val}'`
    : (val instanceof Date
        ? `'${val.toISOString().slice(0, 19).replace('T', ' ')}'`
        : val);
});
console.log("[SQL]", finalSQL);


      const [rows] = await pool.query(sql, params);
      return res.json({ rows, bucketSec, stat: statFn, metric: 'temp_hum_vpd' });
    }

    // ---------- โหมดรวม 2 ค่า: temperature + humidity ----------
    if (bothMode) {
      let sql = `
        SELECT
          FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(c.measured_at)/?)*?) AS bucket_at,
          c.wid                                                   AS wid,
          ${statFn}(c.temperature)                                 AS temperature,
          ${statFn}(c.humidity)                                    AS humidity
        FROM climate c
        JOIN weatherStation w ON w.wid = c.wid
        JOIN zone z           ON z.zid = w.zid
        JOIN plot p           ON p.pid = z.pid
        WHERE c.measured_at BETWEEN ? AND ?
          AND p.pid = ?
      `;
      // const params = [bucketSec, bucketSec, new Date(start), new Date(end), Number(pid)];
      const params = [
        bucketSec, bucketSec,
        toThailandTime(start),
        toThailandTime(end),
        Number(pid)
      ];      
      if (zid) { sql += ` AND z.zid = ? `; params.push(Number(zid)); }
      if (widList.length) {
        sql += ` AND c.wid IN (${widList.map(() => '?').join(',')}) `;
        params.push(...widList);
      }
      sql += ` GROUP BY bucket_at, c.wid ORDER BY bucket_at ASC, c.wid ASC `;

      const [rows] = await pool.query(sql, params);
      return res.json({ rows, bucketSec, stat: statFn, metric: 'temp_hum' });
    }

    // ---------- โหมดเดี่ยว: temperature / humidity / vpd ----------
    const colMap = { temperature: 'temperature', humidity: 'humidity', vpd: 'vpd' };
    const col = colMap[metricRaw] || 'temperature';

    let sql = `
      SELECT
        FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(c.measured_at)/?)*?) AS bucket_at,
        c.wid                                                   AS wid,
        ${statFn}(c.${col})                                      AS value
      FROM climate c
      JOIN weatherStation w ON w.wid = c.wid
      JOIN zone z           ON z.zid = w.zid
      JOIN plot p           ON p.pid = z.pid
      WHERE c.measured_at BETWEEN ? AND ?
        AND p.pid = ?
    `;
    // const params = [bucketSec, bucketSec, new Date(start), new Date(end), Number(pid)];
    const params = [
      bucketSec, bucketSec,
      toThailandTime(start),
      toThailandTime(end),
      Number(pid)
    ];    
    if (zid) { sql += ` AND z.zid = ? `; params.push(Number(zid)); }
    if (widList.length) {
      sql += ` AND c.wid IN (${widList.map(() => '?').join(',')}) `;
      params.push(...widList);
    }
    sql += ` GROUP BY bucket_at, c.wid ORDER BY bucket_at ASC, c.wid ASC `;

    const [rows] = await pool.query(sql, params);
    res.json({ rows, bucketSec, stat: statFn, metric: col });
  } catch (e) {
    console.error('[history.weather]', e);
    res.status(500).json({ error: 'internal error' });
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
// app.get('/api/filters/hierarchy', async (req, res) => {
//   const [rows] = await pool.query(`
//     SELECT 
//       u.user_id              AS uid,
//       u.name                 AS user_name,
//       p.pid                  AS pid,
//       p.name                 AS plot_name,
//       z.zid                  AS zid,
//       z.name                 AS zone_name,
//       s.sid                  AS sid,
//       s.name                 AS sensor_name,
//       s.type                 AS sensor_type
//     FROM app_user u
//     JOIN plot p ON p.user_id = u.user_id
//     JOIN zone z ON z.pid = p.pid
//     JOIN soilSensor s ON s.zid = z.zid
//     ORDER BY u.user_id, p.pid, z.zid, s.sid
//   `);

//   const users = [];
//   const byUid = new Map(), byPid = new Map(), byZid = new Map();

//   for (const r of rows) {
//     if (!byUid.has(r.uid)) {
//       const u = { uid: r.uid, name: r.user_name, plots: [] };
//       byUid.set(r.uid, u); users.push(u);
//     }
//     const u = byUid.get(r.uid);

//     if (!byPid.has(r.pid)) {
//       const p = { pid: r.pid, name: r.plot_name, zones: [] };
//       byPid.set(r.pid, p); u.plots.push(p);
//     }
//     const p = byPid.get(r.pid);

//     if (!byZid.has(r.zid)) {
//       const z = { zid: r.zid, name: r.zone_name, sensors: [] };
//       byZid.set(r.zid, z); p.zones.push(z);
//     }
//     const z = byZid.get(r.zid);

//     z.sensors.push({ sid: r.sid, name: r.sensor_name, type: r.sensor_type });
//   }

//   res.json({ users });
// });

// โครงสร้างที่คืนจะเป็น: users[] -> plots[] -> zones[] -> sensors[] + weatherStations[]
app.get('/api/filters/hierarchy', async (req, res) => {
  // 1) ดึงลำดับชั้น + sensors (ใช้ LEFT JOIN เพื่อไม่ตัดโซนที่ยังไม่มี sensor)
  const [rows] = await pool.query(`
    SELECT 
      u.user_id            AS uid,
      u.name               AS user_name,
      p.pid                AS pid,
      p.name               AS plot_name,
      z.zid                AS zid,
      z.name               AS zone_name,
      s.sid                AS sid,
      s.name               AS sensor_name,
      s.type               AS sensor_type
    FROM app_user u
    JOIN plot p       ON p.user_id = u.user_id
    JOIN zone z       ON z.pid     = p.pid
    LEFT JOIN soilSensor s ON s.zid = z.zid
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
      const z = { zid: r.zid, name: r.zone_name, sensors: [], weatherStations: [] };
      byZid.set(r.zid, z); p.zones.push(z);
    }
    const z = byZid.get(r.zid);

    if (r.sid) {
      z.sensors.push({ sid: r.sid, name: r.sensor_name, type: r.sensor_type });
    }
  }

  // 2) ดึง weather stations ต่อโซน แล้วแนบเข้ากับแต่ละ zone
  const [ws] = await pool.query(`
    SELECT 
      u.user_id  AS uid, 
      p.pid, 
      z.zid, 
      w.wid, 
      w.name     AS ws_name, 
      w.modbus_id
    FROM app_user u
    JOIN plot p          ON p.user_id = u.user_id
    JOIN zone z          ON z.pid     = p.pid
    JOIN weatherStation w ON w.zid    = z.zid
    ORDER BY u.user_id, p.pid, z.zid, w.wid
  `);

  for (const r of ws) {
    const z = byZid.get(r.zid);
    if (z) {
      z.weatherStations.push({
        wid: r.wid,
        name: r.ws_name,
        modbus_id: r.modbus_id
      });
    }
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

// GET /api/soil-sensors/history?start=ISO&end=ISO&sid=optional&limit=optional
app.get('/api/soil-sensors/history', async (req, res) => {
  try {
    const { start, end, sid, limit } = req.query;

    // ตรวจสอบช่วงเวลา
    if (!start || !end) {
      return res.status(400).json({ ok: false, error: 'start and end are required (ISO 8601)' });
    }
    const startDate = new Date(start);
    const endDate = new Date(end);
    if (isNaN(startDate) || isNaN(endDate)) {
      return res.status(400).json({ ok: false, error: 'start/end must be valid ISO date strings' });
    }

    // ป้องกัน OOM: จำกัดจำนวนแถวสูงสุด (เช่น 50k)
    const maxCap = 50000;
    const userLimit = Number(limit);
    const cap = Number.isFinite(userLimit) && userLimit > 0 ? Math.min(userLimit, maxCap) : maxCap;

    const where = ['m.measured_at BETWEEN ? AND ?'];
    const params = [startDate, endDate];

    if (sid) {
      where.push('m.sid = ?');
      params.push(Number(sid));
    }

    const sql = `
      SELECT m.sid, m.moisture, m.temperature, m.measured_at
      FROM moisture m
      WHERE ${where.join(' AND ')}
      ORDER BY m.measured_at ASC
      LIMIT ?
    `;
    params.push(cap);

    const [rows] = await pool.query(sql, params);
    // คืนเป็น JSON array ตรงๆ ให้ frontend
    return res.json(rows);
  } catch (e) {
    console.error('[API] /api/soil-sensors/history error:', e?.message || e);
    return res.status(500).json({ ok: false, error: 'internal error' });
  }
});

// GET /api/weather/history?start=ISO&end=ISO&wid=optional&limit=optional
app.get('/api/weather/history', async (req, res) => {
  try {
    const { start, end, wid, limit } = req.query;

    if (!start || !end) {
      return res.status(400).json({ ok: false, error: 'start and end are required (ISO 8601)' });
    }
    const startDate = new Date(start);
    const endDate = new Date(end);
    if (isNaN(startDate) || isNaN(endDate)) {
      return res.status(400).json({ ok: false, error: 'start/end must be valid ISO date strings' });
    }

    const maxCap = 50000;
    const userLimit = Number(limit);
    const cap = Number.isFinite(userLimit) && userLimit > 0 ? Math.min(userLimit, maxCap) : maxCap;

    const where = ['c.measured_at BETWEEN ? AND ?'];
    const params = [startDate, endDate];

    if (wid) {
      where.push('c.wid = ?');
      params.push(Number(wid));
    }

    const sql = `
      SELECT c.wid, c.temperature, c.humidity, c.measured_at
      FROM climate c
      WHERE ${where.join(' AND ')}
      ORDER BY c.measured_at ASC
      LIMIT ?
    `;
    params.push(cap);

    const [rows] = await pool.query(sql, params);
    return res.json(rows);
  } catch (e) {
    console.error('[API] /api/weather/history error:', e?.message || e);
    return res.status(500).json({ ok: false, error: 'internal error' });
  }
});


// GET /api/weather/history?start=ISO&end=ISO&wid=optional
app.get('/api/weather/history', async (req, res) => {
  const { start, end, wid } = req.query;
  // SELECT wid, temperature, humidity, measured_at
  // FROM climate
  // WHERE measured_at BETWEEN ? AND ?
  // [AND wid = ?]
  // ORDER BY measured_at ASC
});

// ===== Utils เล็กๆ =====
function parseCSVInt(s) {
  if (!s) return [];
  return String(s)
    .split(',')
    .map(x => x.trim())
    .filter(x => x !== '')
    .map(x => Number(x))
    .filter(Number.isFinite);
}
function pickBucketSec(bucket) {
  // รองรับ 15 นาที, ชั่วโมง, วัน
  const map = { '15m': 900, 'hour': 3600, 'day': 86400 };
  return map[bucket] || 3600; // default: hour
}

// ===== 1) Soil moisture aggregated =====
//
// GET /api/soil-sensors/history/aggregate
//   ?start=ISO&end=ISO
//   &sid=2,5,7             // optional: หลาย sid คั่นด้วย ,
//   &bucket=15m|hour|day    // optional (default hour)
//   &stat=avg|min|max       // optional (default avg)
//   &limit=50000            // optional cap
//
// Response: [{sid, bucket_start, avg_moisture, min_moisture, max_moisture, avg_temperature}]
app.get('/api/soil-sensors/history/aggregate', async (req, res) => {
  try {
    const { start, end, sid, bucket, stat, limit } = req.query;
    if (!start || !end) {
      return res.status(400).json({ ok: false, error: 'start and end are required (ISO 8601)' });
    }
    const startDate = new Date(start);
    const endDate = new Date(end);
    if (isNaN(startDate) || isNaN(endDate)) {
      return res.status(400).json({ ok: false, error: 'start/end must be valid ISO date strings' });
    }

    const sec = pickBucketSec(bucket);
    const sids = parseCSVInt(sid);
    const maxCap = 50000;
    const cap = Math.min(Number(limit) || maxCap, maxCap);

    const where = ['m.measured_at BETWEEN ? AND ?'];
    const params = [startDate, endDate];

    if (sids.length) {
      where.push(`m.sid IN (${sids.map(() => '?').join(',')})`);
      params.push(...sids);
    }

    // สรุปทั้ง avg/min/max คืนไปให้ครบ เพื่อให้ frontend เลือกแสดงได้
    const sql = `
      SELECT
        m.sid,
        FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(m.measured_at)/?)*?) AS bucket_start,
        AVG(m.moisture)  AS avg_moisture,
        MIN(m.moisture)  AS min_moisture,
        MAX(m.moisture)  AS max_moisture,
        AVG(m.temperature) AS avg_temperature
      FROM moisture m
      WHERE ${where.join(' AND ')}
      GROUP BY m.sid, bucket_start
      ORDER BY bucket_start ASC, m.sid ASC
      LIMIT ?
    `;

    const [rows] = await pool.query(sql, [sec, sec, ...params, cap]);
    // (ออปชัน) หากผู้ใช้เจาะจง stat=avg|min|max จะลด payload ฝั่งเน็ตลง
    if (stat === 'min') {
      rows.forEach(r => { delete r.avg_moisture; delete r.max_moisture; });
    } else if (stat === 'max') {
      rows.forEach(r => { delete r.avg_moisture; delete r.min_moisture; });
    } else {
      // default avg: เก็บ avg แล้วตัด min/max ทิ้งก็ได้
      rows.forEach(r => { delete r.min_moisture; delete r.max_moisture; });
    }
    return res.json(rows);
  } catch (e) {
    console.error('[API] /api/soil-sensors/history/aggregate error:', e?.message || e);
    return res.status(500).json({ ok: false, error: 'internal error' });
  }
});

// ===== 2) Weather aggregated =====
//
// GET /api/weather/history/aggregate
//   ?start=ISO&end=ISO
//   &wid=1,2                // optional หลายสถานี
//   &bucket=15m|hour|day
//   &stat=avg|min|max       // ใช้กับ temperature/humidity (ส่ง avg/min/max กลับคล้ายกัน)
//   &limit=50000
//
// Response: [{wid, bucket_start, avg_temperature, min_temperature, max_temperature, avg_humidity, min_humidity, max_humidity}]
app.get('/api/weather/history/aggregate', async (req, res) => {
  try {
    const { start, end, wid, bucket, stat, limit } = req.query;
    if (!start || !end) {
      return res.status(400).json({ ok: false, error: 'start and end are required (ISO 8601)' });
    }
    const startDate = new Date(start);
    const endDate = new Date(end);
    if (isNaN(startDate) || isNaN(endDate)) {
      return res.status(400).json({ ok: false, error: 'start/end must be valid ISO date strings' });
    }

    const sec = pickBucketSec(bucket);
    const wids = parseCSVInt(wid);
    const maxCap = 50000;
    const cap = Math.min(Number(limit) || maxCap, maxCap);

    const where = ['c.measured_at BETWEEN ? AND ?'];
    const params = [startDate, endDate];

    if (wids.length) {
      where.push(`c.wid IN (${wids.map(() => '?').join(',')})`);
      params.push(...wids);
    }

    const sql = `
      SELECT
        c.wid,
        FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(c.measured_at)/?)*?) AS bucket_start,
        AVG(c.temperature) AS avg_temperature,
        MIN(c.temperature) AS min_temperature,
        MAX(c.temperature) AS max_temperature,
        AVG(c.humidity)    AS avg_humidity,
        MIN(c.humidity)    AS min_humidity,
        MAX(c.humidity)    AS max_humidity
      FROM climate c
      WHERE ${where.join(' AND ')}
      GROUP BY c.wid, bucket_start
      ORDER BY bucket_start ASC, c.wid ASC
      LIMIT ?
    `;

    const [rows] = await pool.query(sql, [sec, sec, ...params, cap]);
    if (stat === 'min') {
      rows.forEach(r => { delete r.avg_temperature; delete r.max_temperature; delete r.avg_humidity; delete r.max_humidity; });
    } else if (stat === 'max') {
      rows.forEach(r => { delete r.avg_temperature; delete r.min_temperature; delete r.avg_humidity; delete r.min_humidity; });
    } else {
      rows.forEach(r => { delete r.min_temperature; delete r.max_temperature; delete r.min_humidity; delete r.max_humidity; });
    }
    return res.json(rows);
  } catch (e) {
    console.error('[API] /api/weather/history/aggregate error:', e?.message || e);
    return res.status(500).json({ ok: false, error: 'internal error' });
  }
});




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
