// Teltonika TCP Server (Codec8/8E)
import net from "node:net";
import Parser from "/opt/ingest-shared/node_modules/teltonika-parser-extended/index.js";
import binutils from "/opt/ingest-shared/node_modules/binutils64/binutils.js";
import { MongoClient } from "/opt/ingest-shared/node_modules/mongodb/lib/index.js";
import dotenv from "/opt/ingest-shared/node_modules/dotenv/lib/main.js";
import { createClient } from "/opt/ingest-shared/node_modules/redis/dist/index.js";
import path from "path";

// IMEI para debug selectivo
let DEBUG_IMEI = process.env.DEBUG_IMEI || null;
const logForImei = (imeiValue, ...args) => {
  if (DEBUG_IMEI && String(imeiValue) === String(DEBUG_IMEI)) {
  }
};

const __dirname = path.dirname(new URL(import.meta.url).pathname);
dotenv.config({ path: path.resolve(__dirname, "../.env") });

// In staging we may want to fully process packets but never send ACKs back (avoid loops).
// Default: ACKs enabled.
const ACK_ENABLED = process.env.ACK_ENABLED !== "false";

// Guardar hex completo de paquetes solo cuando se habilita explícitamente.
const STORE_RAW_PACKET_HEX = process.env.STORE_RAW_PACKET_HEX === "true";
const RAW_PACKET_HEX_DEVICE_IDS = new Set(
  String(process.env.RAW_PACKET_HEX_DEVICE_IDS || "4251")
    .split(",")
    .map((v) => Number(String(v).trim()))
    .filter((v) => Number.isFinite(v))
);
const DEBUG_FILTERS = process.env.DEBUG_FILTERS === "true";
const IGNORED_RAW_EVENTS = new Set(
  String(process.env.IGNORED_RAW_EVENTS || "497,498,499,11317")
    .split(",")
    .map((v) => Number(String(v).trim()))
    .filter((v) => Number.isFinite(v))
);

// Nota: TCP mirroring / forwarding removido (ingestor mínimo)

function maybeWrite(socket, payload, label = "socket.write") {
  if (!ACK_ENABLED) {
    return;
  }
  try {
    socket.write(payload);
  } catch (e) {
    console.error(`❌ Failed ${label}:`, e);
  }
}

/* ------------------------------------ */
/* MONGO                                */
/* ------------------------------------ */
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

async function getDeviceFromImei(imei) {
  const data = await redis.get(`imei:${imei}`);

  if (!data) {
    console.error(`❌ IMEI not found in Redis: ${imei}`);
    return null;
  }

  return JSON.parse(data);
}

function getLastOdometerKey(imei) {
  return `teltonika:last_odometer:${imei}`;
}

async function computeAndStoreOdometerDelta(imei, odometer, updateTime) {
  const current = toNumber(odometer);
  if (current === null) {
    return 0;
  }

  const key = getLastOdometerKey(String(imei));
  const previousRaw = await redis.get(key);
  let delta = 0;

  if (previousRaw) {
    try {
      const previous = JSON.parse(previousRaw);
      const previousOdometer = toNumber(previous?.odometer);
      if (previousOdometer !== null) {
        const computed = current - previousOdometer;
        delta = computed > 0 ? computed : 0;
      }
    } catch (e) {
      console.error(`❌ Invalid previous odometer cache for ${imei}:`, e?.message || e);
    }
  }

  await redis.set(key, JSON.stringify({
    odometer: current,
    update_time: updateTime instanceof Date && !Number.isNaN(updateTime.getTime())
      ? updateTime.toISOString()
      : updateTime ?? null,
  }));

  return delta;
}

const WEEKLY_COLLECTION_PREFIX = process.env.WEEKLY_COLLECTION_PREFIX || "drops_week_";
const DAILY_COLLECTION_PREFIX = process.env.DAILY_COLLECTION_PREFIX || "drops_day_";

/* ------------------------------------ */
/* TCP SERVER                           */
/* ------------------------------------ */

const PORT = process.env.TCP_PORT ? Number(process.env.TCP_PORT) : 5003;
const BATCH_SIZE = process.env.MONGO_BATCH_SIZE ? Number(process.env.MONGO_BATCH_SIZE) : 200;
const insertBuffer = [];
let flushInProgress = false;

function pad2(n) {
  return String(n).padStart(2, "0");
}

// ISO week helpers (UTC-based)
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

function isBinaryLikeString(str) {
  // caracteres de control o bytes no imprimibles
  return /[\x00-\x08\x0E-\x1F\x7F-\xFF]/.test(str);
}

function hasValidGPS(rec) {
  const lat = rec?.lat ?? rec?.latitude ?? rec?.gps?.latitude;
  const lng = rec?.lng ?? rec?.longitude ?? rec?.gps?.longitude;

  return Boolean(
    Number.isFinite(lat) &&
      Number.isFinite(lng) &&
      lat !== 0 &&
      lng !== 0
  );
}

function normalizeIoValue(value) {
  if (typeof value === "number") return value;
  if (typeof value === "string") {
    if (isBinaryLikeString(value)) {
      return Buffer.from(value, "latin1").toString("hex");
    }
    return value;
  }
  if (Buffer.isBuffer(value)) {
    return value.toString("hex");
  }
  return value;
}


function toNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function ioMapFromRecord(rec) {
  const ioElements = Array.isArray(rec?.ioElements) ? rec.ioElements : [];
  const map = new Map();
  for (const io of ioElements) {
    map.set(Number(io?.id), normalizeIoValue(io?.value));
  }
  return map;
}

function getIo(map, id) {
  return map.has(Number(id)) ? map.get(Number(id)) : null;
}

function ioMapToPrettyObject(map) {
  return Object.fromEntries(
    [...map.entries()]
      .sort((a, b) => Number(a[0]) - Number(b[0]))
      .map(([id, value]) => [String(id), value])
  );
}

function toVoltageVolts(rawMv) {
  const n = toNumber(rawMv);
  return n === null ? null : Number((n / 1000).toFixed(3));
}

function toBatteryPercentFromMv(rawMv) {
  const n = toNumber(rawMv);
  if (n === null) return null;
  return Number(((n / 43.77256539235412)).toFixed(2));
}

function eventNameFromId(eventId) {
  if (Number(eventId) === 0) return 'Periodic / Sin evento';
  return null;
}

function unifiedEventFromId(eventId) {
  if (Number(eventId) === 0) {
    return { unified_event_code: 31, unified_event_name: 'periodic' };
  }
  return { unified_event_code: null, unified_event_name: null };
}

function stoppedFromSpeed(speed) {
  const n = Number(speed);
  if (!Number.isFinite(n)) return null;
  return n <= 0 ? 1 : 0;
}
function shouldIgnoreTcpRecord({ rawEventCode, gps }) {
  if (IGNORED_RAW_EVENTS.has(Number(rawEventCode))) {
    return { ignore: true, reason: 'ignored_raw_event' };
  }

  const lat = gps?.lat ?? gps?.latitude;
  const lng = gps?.lng ?? gps?.longitude;
  if (!Number.isFinite(lat) || !Number.isFinite(lng) || lat === 0 || lng === 0) {
    return { ignore: true, reason: 'invalid_gps' };
  }

  return { ignore: false };
}

function isIgnoredMongoDoc(doc) {
  return IGNORED_RAW_EVENTS.has(Number(doc?.event_id ?? doc?.event_code));
}


async function normalizeAvlRecord(imei, rec, rawPacketHex) {
  const ioElements = Array.isArray(rec?.ioElements)
    ? rec.ioElements.map((io) => ({
        ...io,
        value: normalizeIoValue(io.value),
      }))
    : [];

  const io = ioMapFromRecord({ ioElements });
  const gps = rec?.gps || {};
  const event_id = rec?.event_id ?? rec?.eventId ?? 0;
  const speedNum = toNumber(rec?.speed ?? gps?.speed);
  const heading = toNumber(rec?.angle ?? gps?.angle);
  const odometer = toNumber(getIo(io, 16));
  const mobileOperatorCode = getIo(io, 241);
  const extVoltage = toVoltageVolts(getIo(io, 66));
  const batteryPercent = toBatteryPercentFromMv(getIo(io, 67));
  const event_name = eventNameFromId(event_id);
  const unified = unifiedEventFromId(event_id);
  const updateTime = rec?.timestamp ? new Date(rec.timestamp) : null;
  const odometroReporte = await computeAndStoreOdometerDelta(imei, odometer, updateTime);

  const device = await getDeviceFromImei(String(imei));
  const eyeCandidates = [...io.entries()]
    .filter(([id]) => Number(id) >= 300)
    .map(([id, value]) => ({ id: Number(id), value }));

  console.log(`📶 Teltonika mobile operator | imei=${imei} device_id=${device?.device_id ?? "null"} operator=${mobileOperatorCode ?? "null"}`);
  console.log(`🧾 Teltonika IO pretty | imei=${imei} device_id=${device?.device_id ?? "null"}
${JSON.stringify(ioMapToPrettyObject(io), null, 2)}`);
  if (eyeCandidates.length) {
    console.log(`👁️ EYE candidate IO | imei=${imei} device_id=${device?.device_id ?? "null"} event_id=${event_id}
${JSON.stringify(eyeCandidates, null, 2)}`);
  }

  if (io.has(331)) {
    console.log(`🧪 BLE raw ioElements (id 331 present) | imei=${imei} device_id=${device?.device_id ?? "null"} event_id=${event_id}
${JSON.stringify(ioElements, null, 2)}`);
    console.log(`🧪 BLE raw record (id 331 present) | imei=${imei} device_id=${device?.device_id ?? "null"} event_id=${event_id}
${JSON.stringify(rec, null, 2)}`);
  }

  const normalized = {
    imei: String(imei),
    device_id: device?.device_id ?? null,
    customer_id: device?.customer_id ?? null,
    lat: rec?.lat ?? rec?.latitude ?? gps?.latitude ?? null,
    lng: rec?.lng ?? rec?.longitude ?? gps?.longitude ?? null,
    speed: speedNum === null ? null : speedNum.toFixed(1),
    heading,
    satelites: toNumber(rec?.satelites ?? rec?.satellites ?? gps?.satellites),
    operator: mobileOperatorCode ?? null,
    rssi: -103,
    odometer,
    powerSupply: extVoltage,
    powerBat: batteryPercent,
    event_code: Number(event_id) || 0,
    event_name,
    event_id: Number(event_id) || 0,
    event_value: null,
    event_value_text: null,
    unified_event_code: unified.unified_event_code,
    unified_event_name: unified.unified_event_name,
    stoped: stoppedFromSpeed(speedNum),
    update_time: updateTime,
    odometroTotal: odometer,
    odometroReporte,
    distance_m_between_msgs: 0,
    ioelements: ioElements,
  };

  if (
    STORE_RAW_PACKET_HEX &&
    rawPacketHex &&
    RAW_PACKET_HEX_DEVICE_IDS.has(Number(device?.device_id))
  ) {
    normalized.raw_packet_hex = rawPacketHex;
  }

  console.log('📝 drop to insert:', JSON.stringify(normalized));
  return normalized;
}

function groupDocsByCollections(docs) {
  const weeklyGroups = new Map();
  const dailyGroups = new Map();

  for (const doc of docs) {
    const baseDate = doc?.update_time instanceof Date && !Number.isNaN(doc.update_time.getTime())
      ? doc.update_time
      : (doc?.received_at instanceof Date ? doc.received_at : new Date());

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

function buildRedisPayload(doc) {
  const payload = serializeDocForRedis(doc);

  if (Number(doc?.device_id) === 1493) {
    payload.ioelements = Array.isArray(doc?.ioelements) ? doc.ioelements : [];
  }

  return payload;
}

async function publishDocsToRedisStream(docs) {
  if (!Array.isArray(docs) || !docs.length) {
    return;
  }

  for (const doc of docs) {
    const payload = buildRedisPayload(doc);
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
    return { insertedCount: 0, skippedCount: 0, publishedCount: 0 };
  }

  const filteredDocs = docs.filter((doc) => !isIgnoredMongoDoc(doc));
  const skippedCount = docs.length - filteredDocs.length;

  if (!filteredDocs.length) {
    return { insertedCount: 0, skippedCount, publishedCount: 0 };
  }

  const { weeklyGroups, dailyGroups } = groupDocsByCollections(filteredDocs);

  try {
    for (const [colName, groupDocs] of weeklyGroups.entries()) {
      console.log(`🗓️ collection -> ${colName} count=${groupDocs.length}`);
      await mongoDb.collection(colName).insertMany(groupDocs, { ordered: true });
    }
    for (const [colName, groupDocs] of dailyGroups.entries()) {
      console.log(`📆 collection -> ${colName} count=${groupDocs.length}`);
      await mongoDb.collection(colName).insertMany(groupDocs, { ordered: true });
    }

    await publishDocsToRedisStream(filteredDocs);

    return { insertedCount: filteredDocs.length, skippedCount, publishedCount: filteredDocs.length };
  } catch (e) {
    console.error(`❌ Mongo/Redis pipeline ERROR | count=${filteredDocs.length} | err=${e?.message || e}`);
    throw e;
  }
}

async function flushInsertBuffer() {
  if (flushInProgress || !insertBuffer.length) {
    return;
  }

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

async function handleIncomingPacket(data, socket, state) {
  let imei = state.imei;

  logForImei(imei, "📦 TCP bytes:", data.length);

  try {
    const parser = new Parser(data);

    if (parser.isImei) {
      state.imei = parser.imei;
      imei = state.imei;

      if (!state.imei || !/^\d{15}$/.test(String(state.imei))) {
        console.error("❌ IMEI inválido (no es 15 dígitos), cerrando socket");
        socket?.destroy();
        return;
      }

      logForImei(state.imei, "✔ IMEI:", state.imei);
      maybeWrite(socket, Buffer.from([0x01]), "IMEI ACK");
      return;
    }

    const avl = parser.getAvl();
    if (!avl || !avl.records || !Array.isArray(avl.records)) {
      console.log("📨 Teltonika non-AVL / possible GPRS response | imei=", imei ?? null, "hex=", data.toString("hex"));
      console.log("📨 Teltonika non-AVL / parser object:", JSON.stringify(avl ?? null, null, 2));
      return;
    }

    if (!imei) {
      console.error("❌ AVL packet recibido sin IMEI de sesión; no se enviará ACK");
      return;
    }

    const rawPacketHex = STORE_RAW_PACKET_HEX ? data.toString("hex") : null;
    const docsToInsert = [];
    let discarded = 0;

    for (const rec of avl.records) {
      const rawEventCode = rec?.event_id ?? rec?.eventId ?? null;
      const gps = {
        lat: rec?.lat ?? rec?.latitude ?? rec?.gps?.latitude,
        lng: rec?.lng ?? rec?.longitude ?? rec?.gps?.longitude,
      };
      const decision = shouldIgnoreTcpRecord({ rawEventCode, gps });
      if (decision.ignore) {
        const ignoredIoElements = Array.isArray(rec?.ioElements)
          ? rec.ioElements.map((io) => ({
              ...io,
              value: normalizeIoValue(io?.value),
            }))
          : [];
        const ignoredIoMap = ioMapFromRecord({ ioElements: ignoredIoElements });
        const ignoredDevice = await getDeviceFromImei(String(imei));

        console.log(`🚫 Ignored Teltonika record | imei=${imei} device_id=${ignoredDevice?.device_id ?? "null"} rawEventCode=${rawEventCode ?? "null"} reason=${decision.reason}`);
        console.log(`🚫 Ignored Teltonika IO pretty | imei=${imei} device_id=${ignoredDevice?.device_id ?? "null"}
${JSON.stringify(ioMapToPrettyObject(ignoredIoMap), null, 2)}`);

        const ignoredEyeListRaw = getIo(ignoredIoMap, 11317);
        if (ignoredEyeListRaw != null) {
          console.log(`👁️ Ignored EYE RAW 11317 | imei=${imei} device_id=${ignoredDevice?.device_id ?? "null"} rawEventCode=${rawEventCode ?? "null"}`);
          console.log(JSON.stringify(ignoredEyeListRaw, null, 2));
        }

        discarded += 1;
        if (DEBUG_FILTERS) {
          console.log('skip record', JSON.stringify({ imei, rawEventCode, reason: decision.reason }));
        }
        continue;
      }
      if (!hasValidGPS(rec)) {
        discarded += 1;
        if (DEBUG_FILTERS) {
          console.log('skip record', JSON.stringify({ imei, rawEventCode, reason: 'invalid_gps_legacy' }));
        }
        continue;
      }

      const doc = await normalizeAvlRecord(imei, rec, rawPacketHex);
      docsToInsert.push(doc);
    }

    // Política de seguridad: solo ACK cuando los registros aceptados del paquete
    // ya quedaron persistidos en Mongo.
    if (docsToInsert.length > 0) {
      await insertDocsToMongo(docsToInsert);
    }

    if (docsToInsert.length === 0 && discarded < avl.number_of_data) {
      console.error(`❌ No se generaron documentos para ACK | imei=${imei} total=${avl.number_of_data} discarded=${discarded}`);
      return;
    }

    const w = new binutils.BinaryWriter();
    w.WriteInt32(avl.number_of_data);
    maybeWrite(socket, Buffer.from(w.ByteBuffer), "AVL ACK");
  } catch (err) {
    console.error("❌ TCP ERROR:", err);
  }
}

const server = net.createServer((socket) => {
  const state = { imei: null, deviceName: null };

  socket.on("data", async (data) => {
    await handleIncomingPacket(data, socket, state);
  });

  const cleanup = () => {
    return;
  };

  socket.on('close', cleanup);
  socket.on('end', cleanup);
  socket.on('error', cleanup);
});

server.listen(PORT, "0.0.0.0", () =>
  console.log(`🚀 Teltonika TCP listening on ${PORT}`)
);

/* ------------------------------------ */
/* BATCH INSERT                         */
/* ------------------------------------ */
setInterval(async () => {
  try {
    await flushInsertBuffer();
  } catch (e) {
    console.error(`❌ flushInsertBuffer failed: ${e?.message || e}`);
  }
}, 1000);

export default server;
