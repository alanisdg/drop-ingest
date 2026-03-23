// Teltonika TCP Server (Codec8/8E)
import net from "node:net";
import Parser from "teltonika-parser-extended";
import binutils from "binutils64";
import { MongoClient } from "mongodb";
import dotenv from "dotenv";
import path from "path";

// IMEI para debug selectivo
let DEBUG_IMEI = process.env.DEBUG_IMEI || null;
const logForImei = (imeiValue, ...args) => {
  if (DEBUG_IMEI && String(imeiValue) === String(DEBUG_IMEI)) {
    console.log(...args);
  }
};

const __dirname = path.dirname(new URL(import.meta.url).pathname);
dotenv.config({ path: path.resolve(__dirname, "../.env") });

// In staging we may want to fully process packets but never send ACKs back (avoid loops).
// Default: ACKs enabled.
const ACK_ENABLED = process.env.ACK_ENABLED !== "false";

// Guardar hex completo de paquetes solo cuando se habilita explícitamente.
const STORE_RAW_PACKET_HEX = process.env.STORE_RAW_PACKET_HEX === "true";

// Nota: TCP mirroring / forwarding removido (ingestor mínimo)

function maybeWrite(socket, payload, label = "socket.write") {
  if (!ACK_ENABLED) {
    return;
  }
  try {
    socket.write(payload);
    if (String(label).includes('ACK')) {
    }
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

const WEEKLY_COLLECTION_PREFIX = process.env.WEEKLY_COLLECTION_PREFIX || "drops_week_";
const DAILY_COLLECTION_PREFIX = process.env.DAILY_COLLECTION_PREFIX || "drops_day_";

/* ------------------------------------ */
/* TCP SERVER                           */
/* ------------------------------------ */

const PORT = process.env.TCP_PORT ? Number(process.env.TCP_PORT) : 5003;
const insertBuffer = [];
const BATCH_SIZE = process.env.MONGO_BATCH_SIZE ? Number(process.env.MONGO_BATCH_SIZE) : 200;

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
  return Boolean(
    rec &&
      typeof rec.lat === "number" &&
      typeof rec.lng === "number" &&
      Number.isFinite(rec.lat) &&
      Number.isFinite(rec.lng) &&
      rec.lat !== 0 &&
      rec.lng !== 0
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

function normalizeAvlRecord(imei, rec, rawPacketHex) {
  const ioElements = Array.isArray(rec?.ioElements)
    ? rec.ioElements.map((io) => ({
        ...io,
        value: normalizeIoValue(io.value),
      }))
    : [];

  return {
    imei: String(imei),
    received_at: new Date(),
    update_time: rec?.timestamp ? new Date(rec.timestamp) : null,
    lat: rec?.lat ?? null,
    lng: rec?.lng ?? null,
    altitude: rec?.altitude ?? null,
    angle: rec?.angle ?? null,
    satellites: rec?.satellites ?? null,
    speed: rec?.speed ?? null,
    priority: rec?.priority ?? null,
    event_id: rec?.event_id ?? rec?.eventId ?? null,
    ioElements,
    raw_packet_hex: rawPacketHex,
  };
}

async function handleIncomingPacket(data, socket, state) {
    console.log('viene algo')
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
        return;
        }

        const rawPacketHex = STORE_RAW_PACKET_HEX ? data.toString("hex") : null;

        if (!imei) {
        return;
        }

        for (const rec of avl.records) {
        if (!hasValidGPS(rec)) {
            continue;
        }
        const doc = normalizeAvlRecord(imei, rec, rawPacketHex);
        insertBuffer.push(doc);
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
async function insertDropsBatch() {
  if (!insertBuffer.length) return;

  const docs = insertBuffer.splice(0, BATCH_SIZE);
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

  try {
    for (const [colName, groupDocs] of weeklyGroups.entries()) {
      await mongoDb.collection(colName).insertMany(groupDocs);
    }
    for (const [colName, groupDocs] of dailyGroups.entries()) {
      await mongoDb.collection(colName).insertMany(groupDocs);
    }
    console.log(`💾 Mongo dynamic insert OK | count=${docs.length} | weeklyCols=${weeklyGroups.size} | dailyCols=${dailyGroups.size}`);
  } catch (e) {
    console.error(`❌ Mongo dynamic insert ERROR | count=${docs.length} | err=${e?.message || e}`);
    throw e;
  }
}

setInterval(async () => {
  try {
    await insertDropsBatch();
  } catch (e) {
    console.error(`❌ insertDropsBatch failed: ${e?.message || e}`);
  }
}, 1000);

export default server;