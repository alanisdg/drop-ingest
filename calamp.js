import dgram from "dgram";
import { MongoClient } from "mongodb";
import dotenv from "dotenv";
import { createClient } from "redis";
import path from "path";

const __dirname = path.dirname(new URL(import.meta.url).pathname);
dotenv.config();

const STORE_RAW_PACKET_HEX = process.env.STORE_RAW_PACKET_HEX === "true";
const UDP_PORT = process.env.UDP_PORT ? Number(process.env.UDP_PORT) : 5001;
const WEEKLY_COLLECTION_PREFIX = process.env.WEEKLY_COLLECTION_PREFIX || "drops_week_";
const DAILY_COLLECTION_PREFIX = process.env.DAILY_COLLECTION_PREFIX || "drops_day_";
const BATCH_SIZE = process.env.MONGO_BATCH_SIZE ? Number(process.env.MONGO_BATCH_SIZE) : 200;
const insertBuffer = [];
let flushInProgress = false;
const WATCH_IMEI = process.env.WATCH_IMEI || "357041063338902";

const mongoClient = new MongoClient(
  `mongodb://${process.env.DB_MONGO_HOST_NODE}:${process.env.DB_MONGO_PORT}`
);
await mongoClient.connect();
const mongoDb = mongoClient.db(process.env.DB_MONGO_DATABASE);

const redis = createClient({
  socket: {
    host: process.env.REDIS_HOST || "10.124.0.6",
    port: process.env.REDIS_PORT ? Number(process.env.REDIS_PORT) : 6379,
  },
});
redis.on("error", (err) => {
  console.error("❌ Redis error:", err?.message || err);
});
await redis.connect();

function pad2(n) {
  return String(n).padStart(2, "0");
}

function startOfIsoWeekYear(date) {
  const d = new Date(Date.UTC(date.getUTCFullYear(), 0, 4));
  const day = d.getUTCDay() || 7;
  d.setUTCDate(d.getUTCDate() - (day - 1));
  return d;
}

function getIsoWeek(date) {
  const tmp = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
  const day = tmp.getUTCDay() || 7;
  tmp.setUTCDate(tmp.getUTCDate() + 4 - day);
  const yearStart = startOfIsoWeekYear(tmp);
  const diffDays = Math.floor((tmp - yearStart) / 86400000);
  return Math.floor(diffDays / 7) + 1;
}

function getIsoWeekYear(date) {
  const tmp = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
  const day = tmp.getUTCDay() || 7;
  tmp.setUTCDate(tmp.getUTCDate() + 4 - day);
  return tmp.getUTCFullYear();
}

function getWeeklyCollectionName(date) {
  const isoYear = getIsoWeekYear(date);
  const isoWeek = getIsoWeek(date);
  return `${WEEKLY_COLLECTION_PREFIX}${isoYear}_w${pad2(isoWeek)}`;
}

function getDailyCollectionName(date) {
  const y = date.getUTCFullYear();
  const m = pad2(date.getUTCMonth() + 1);
  const d = pad2(date.getUTCDate());
  return `${DAILY_COLLECTION_PREFIX}${y}_${m}_${d}`;
}

function get_decimal(hex) {
  return parseInt(hex, 16);
}

function parse_LatLng(v) {
  const d = parseInt(v, 16);
  return (d < parseInt("7FFFFFFF", 16))
    ? (d / 10000000)
    : 0 - ((parseInt("FFFFFFFF", 16) - d) / 10000000);
}

function mapCalampEventToUnified(eventCode) {
  if (Number(eventCode) === 20) return 20;
  if (Number(eventCode) === 21) return 21;
  if (Number(eventCode) === 11317) return 9100;
  if (!eventCode) return 31;
  return Number(eventCode) || 0;
}

function getUnifiedEventName(code) {
  const c = Number(code);
  if (c === 21) return "engine_on";
  if (c === 20) return "engine_off";
  if (c === 31) return "periodic";
  if (c === 9100) return "dualcam_event";
  return "unknown";
}

function isWithinAmericas(lat, lng) {
  return Number.isFinite(lat) && Number.isFinite(lng)
    && lat >= -60 && lat <= 85
    && lng >= -170 && lng <= -20;
}

function hasValidCalampPacket(packet) {
  return Boolean(
    packet &&
    packet.imei &&
    Number.isFinite(packet.lat) &&
    Number.isFinite(packet.lng) &&
    packet.lat !== 0 &&
    packet.lng !== 0 &&
    isWithinAmericas(packet.lat, packet.lng)
  );
}

function hasValidPacketTimestamp(value) {
  const date = value instanceof Date ? value : new Date(value);
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    return false;
  }

  const minYear = Number(process.env.MIN_VALID_PACKET_YEAR ?? 2020);
  const maxFutureSkewMinutes = Number(process.env.MAX_FUTURE_PACKET_SKEW_MINUTES ?? 10);
  const minDate = new Date(Date.UTC(minYear, 0, 1, 0, 0, 0, 0));
  const maxDate = new Date(Date.now() + (maxFutureSkewMinutes * 60 * 1000));

  return date >= minDate && date <= maxDate;
}

async function getDeviceFromImei(imei) {
  const data = await redis.get(`imei:${imei}`);
  if (!data) {
    console.error(`❌ IMEI not found in Redis: ${imei}`);
    return null;
  }
  return JSON.parse(data);
}

function extractCalampImei(hex) {
  return hex?.substring(4, 19) || null;
}

function parseCalamp(hex) {
  const eventCode = get_decimal(hex.substring(106, 108));
  const unifiedEventCode = mapCalampEventToUnified(eventCode);
  const powerSupply = Number((get_decimal(hex.substring(112, 120)) / 1000).toFixed(3));
  const batteryVolts = Number((get_decimal(hex.substring(120, 128)) / 1000).toFixed(3));
  const vMin = Number(process.env.BATTERY_VMIN ?? 3.0);
  const vMax = Number(process.env.BATTERY_VMAX ?? 4.2);
  const span = vMax - vMin;
  const batteryPercent = Number.isFinite(span) && span > 0
    ? Number(Math.max(0, Math.min(100, (((batteryVolts - vMin) / span) * 100))).toFixed(2))
    : null;

  const packet = {
    protocol: "calamp",
    imei: hex.substring(4, 19),
    update_time: new Date(get_decimal(hex.substring(32, 40)) * 1000),
    lat: parse_LatLng(hex.substring(48, 56)),
    lng: parse_LatLng(hex.substring(56, 64)),
    speed: (get_decimal(hex.substring(72, 80)) * 0.036).toFixed(1),
    heading: get_decimal(hex.substring(81, 84)),
    satelites: get_decimal(hex.substring(84, 86)),
    rssi: Number(hex.substring(92, 96), 16) || null,
    event_code: eventCode,
    event_name: eventCode === 0 ? "Periodic / Sin evento" : null,
    event_id: eventCode,
    unified_event_code: unifiedEventCode,
    unified_event_name: getUnifiedEventName(unifiedEventCode),
    powerSupply,
    powerBat: batteryPercent,
    odometer: get_decimal(hex.substring(136, 144)),
    odometroTotal: get_decimal(hex.substring(136, 144)),
    odometroReporte: get_decimal(hex.substring(144, 152)),
    distance_m_between_msgs: get_decimal(hex.substring(144, 152)),
  };

  if (hex.substring(32, 40) === "0000000c") {
    packet.update_time = new Date(get_decimal(hex.substring(40, 48)) * 1000);
  }

  return packet;
}

async function normalizeCalampPacket(packet, rawPacketHex) {
  const device = await getDeviceFromImei(String(packet.imei));
  const normalized = {
    imei: String(packet.imei),
    device_id: device?.device_id ?? null,
    customer_id: device?.customer_id ?? null,
    lat: packet.lat,
    lng: packet.lng,
    speed: packet.speed,
    heading: Number(packet.heading) || 0,
    satelites: Number(packet.satelites) || 0,
    rssi: packet.rssi,
    odometer: Number(packet.odometer) || 0,
    powerSupply: packet.powerSupply,
    powerBat: packet.powerBat,
    event_code: Number(packet.event_code) || 0,
    event_name: packet.event_name,
    event_id: Number(packet.event_id) || 0,
    event_value: null,
    event_value_text: null,
    unified_event_code: packet.unified_event_code,
    unified_event_name: packet.unified_event_name,
    stoped: Number(packet.speed) <= 0 ? 1 : 0,
    update_time: packet.update_time instanceof Date ? packet.update_time : new Date(packet.update_time),
    odometroTotal: Number(packet.odometroTotal) || 0,
    odometroReporte: Number(packet.odometroReporte) || 0,
    distance_m_between_msgs: Number(packet.distance_m_between_msgs) || 0,
  };

  if (STORE_RAW_PACKET_HEX && rawPacketHex) {
    normalized.raw_packet_hex = rawPacketHex;
  }

  return normalized;
}

function groupDocsByCollections(docs) {
  const weeklyGroups = new Map();
  const dailyGroups = new Map();

  for (const doc of docs) {
    const baseDate = doc?.update_time instanceof Date && !Number.isNaN(doc.update_time.getTime())
      ? doc.update_time
      : new Date();

    const weeklyName = getWeeklyCollectionName(baseDate);
    const dailyName = getDailyCollectionName(baseDate);

    if (!weeklyGroups.has(weeklyName)) weeklyGroups.set(weeklyName, []);
    weeklyGroups.get(weeklyName).push(doc);

    if (!dailyGroups.has(dailyName)) dailyGroups.set(dailyName, []);
    dailyGroups.get(dailyName).push(doc);
  }

  return { weeklyGroups, dailyGroups };
}

function serializeDocForRedis(doc) {
  return JSON.parse(JSON.stringify(doc));
}

async function publishDocsToRedisStream(docs) {
  if (!Array.isArray(docs) || !docs.length) return;

  for (const doc of docs) {
    const payload = serializeDocForRedis(doc);
    await redis.xAdd(
      "gps_stream",
      "*",
      {
        data: JSON.stringify(payload),
      },
      {
        TRIM: {
          strategy: "MAXLEN",
          strategyModifier: "~",
          threshold: 1000,
        },
      }
    );
  }
}

async function insertDocsToMongo(docs) {
  if (!Array.isArray(docs) || !docs.length) {
    return { insertedCount: 0, publishedCount: 0 };
  }

  const { weeklyGroups, dailyGroups } = groupDocsByCollections(docs);

  try {
    for (const [colName, groupDocs] of weeklyGroups.entries()) {
      await mongoDb.collection(colName).insertMany(groupDocs, { ordered: true });
    }
    for (const [colName, groupDocs] of dailyGroups.entries()) {
      await mongoDb.collection(colName).insertMany(groupDocs, { ordered: true });
    }

    await publishDocsToRedisStream(docs);
    return { insertedCount: docs.length, publishedCount: docs.length };
  } catch (e) {
    console.error(`❌ Mongo/Redis pipeline ERROR | count=${docs.length} | err=${e?.message || e}`);
    throw e;
  }
}

async function flushInsertBuffer() {
  if (flushInProgress || !insertBuffer.length) return;

  flushInProgress = true;
  try {
    while (insertBuffer.length) {
      const docs = insertBuffer.splice(0, BATCH_SIZE);
      await insertDocsToMongo(docs);
    }
  } finally {
    flushInProgress = false;
  }
}

const server = dgram.createSocket("udp4");

server.on("message", async (datagram, rinfo) => {
  try {
    const rawHex = datagram.toString("hex");
    const incomingImei = extractCalampImei(rawHex);

    const packet = parseCalamp(rawHex);
    const device = await getDeviceFromImei(String(packet?.imei));
    const isOutsideAmericas = Number.isFinite(packet?.lat) && Number.isFinite(packet?.lng)
      && !isWithinAmericas(packet.lat, packet.lng);

    if (device?.device_id === 498 && isOutsideAmericas) {
      console.warn("🛰️ device_id 498 outside Americas | rawHex:", rawHex);
      console.warn("🛰️ device_id 498 outside Americas | parsed drop:", JSON.stringify({
        from: `${rinfo.address}:${rinfo.port}`,
        imei: packet?.imei,
        device_id: device?.device_id ?? null,
        customer_id: device?.customer_id ?? null,
        lat: packet?.lat,
        lng: packet?.lng,
        update_time: packet?.update_time instanceof Date && !Number.isNaN(packet.update_time.getTime())
          ? packet.update_time.toISOString()
          : packet?.update_time,
        event_code: packet?.event_code,
        unified_event_code: packet?.unified_event_code,
        speed: packet?.speed,
        heading: packet?.heading,
        satelites: packet?.satelites,
        rssi: packet?.rssi,
        odometer: packet?.odometer,
      }));
    }

    if (!hasValidCalampPacket(packet)) {
      console.warn("⚠️ CalAmp ignorado (sin IMEI o GPS inválido)", {
        from: `${rinfo.address}:${rinfo.port}`,
        imei: packet?.imei,
        incomingImei,
        device_id: device?.device_id ?? null,
      });
      return;
    }

    if (!hasValidPacketTimestamp(packet?.update_time)) {
      console.warn("⚠️ CalAmp ignorado (timestamp inválido)", {
        from: `${rinfo.address}:${rinfo.port}`,
        imei: packet?.imei,
        incomingImei,
        update_time: packet?.update_time instanceof Date && !Number.isNaN(packet.update_time.getTime())
          ? packet.update_time.toISOString()
          : packet?.update_time,
      });
      return;
    }

    const normalized = await normalizeCalampPacket(packet, rawHex);
    insertBuffer.push(normalized);

    if (insertBuffer.length >= BATCH_SIZE) {
      await flushInsertBuffer();
    }
  } catch (error) {
    console.error("❌ Error procesando mensaje UDP CalAmp:", error);
  }
});

server.on("listening", () => {
  console.log(`🚀 CalAmp UDP listening on ${UDP_PORT}`);
});

server.bind(UDP_PORT, "0.0.0.0");

setInterval(async () => {
  try {
    await flushInsertBuffer();
  } catch (e) {
    console.error(`❌ flushInsertBuffer failed: ${e?.message || e}`);
  }
}, 1000);