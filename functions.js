import dayjs from "dayjs";
import utc from "dayjs/plugin/utc.js";
import timezone from "dayjs/plugin/timezone.js";
import pointInPolygon from 'point-in-polygon'


dayjs.extend(utc);
dayjs.extend(timezone);
import convertBase from './convertBase.js'; // Asegúrate de tener convertBase.js en el mismo directorio
import { createClient } from 'redis';
const MX_TZ = "America/Mexico_City";
const s32 = (n) => (n & 0x80000000) ? (n - 0x100000000) : n;
import TeltonikaParserPkg from 'teltonika-parser-flex';
const TeltonikaParser = TeltonikaParserPkg.default || TeltonikaParserPkg;


const client = createClient();
await client.connect();

function get_decimal(hex) {
    return parseInt(hex, 16);
}

function parse_LatLng(v) {
    const d = parseInt(v,16); return (d < parseInt('7FFFFFFF', 16)) ? (d /  10000000) : 0 - ((parseInt('FFFFFFFF', 16) - d) / 10000000);
}
 
 const DEBUG_IMEI = "863719065035504";

  
  /* ---------------- Teltonika helpers ---------------- */
  function extractImeiFromUdp(buf) {
    const idx = buf.indexOf(Buffer.from([0x00, 0x0f]));
    if (idx < 0) return undefined;
    return buf.slice(idx + 2, idx + 2 + 15).toString();
  }
  
  // Mapea distintos formatos posibles de IO Elements a un object { [id]: value }
  function toUIntBE(x) {
    if (x == null) return undefined;
    if (typeof x === 'number') return x;
    if (Buffer.isBuffer(x)) return x.readUIntBE(0, x.length);
    if (Array.isArray(x))   return Buffer.from(x).readUIntBE(0, x.length);
    return Number(x);
  }
  
  // Acepta varios formatos posibles de IOs y devuelve { [id:number]: number }
  function normalizeIoElements(io) {
    if (!io) return {};
    // Caso 0: 'io' ya es un array de elementos [{id,value}, ...]
    if (Array.isArray(io)) {
      const out = {};
      for (const it of io) {
        if (!it || typeof it !== 'object') continue;
        const id  = Number(it.IOID ?? it.ioid ?? it.id ?? it.key);
        const val = toUIntBE(it.Value ?? it.value ?? it.val ?? it.raw);
        if (!Number.isNaN(id) && val != null) out[id] = val;
      }
      return out;
    }
    // Caso 1: objeto plano Elements / elements
    const base = io.Elements || io.elements;
    if (base && typeof base === 'object') {
      const out = {};
      for (const k of Object.keys(base)) {
        const id = Number(k);
        const v  = toUIntBE(base[k]);
        if (!Number.isNaN(id) && v != null) out[id] = v;
      }
      return out;
    }
  
    // Caso 2: arrays por tamaño (oneByte/twoBytes/..., 1B/2B/...)
    const out = {};
    const buckets = [
      io.oneByte, io.OneByte, io['1B'], io['one_byte'],
      io.twoBytes, io.TwoBytes, io['2B'], io['two_bytes'],
      io.fourBytes, io.FourBytes, io['4B'], io['four_bytes'],
      io.eightBytes, io.EightBytes, io['8B'], io['eight_bytes'],
      io.xBytes, io.XBytes, io['XB'], io['x_bytes'],
      io.io, io.list, io.items // por si acaso
    ].filter(Boolean);
  
    for (const arr of buckets) {
      if (!Array.isArray(arr)) continue;
      for (const it of arr) {
        // soporta {id,value} | {IOID,Value} | [id,val]
        let id, val;
        if (Array.isArray(it)) { id = Number(it[0]); val = toUIntBE(it[1]); }
        else if (it && typeof it === 'object') {
          id  = Number(it.IOID ?? it.ioid ?? it.id ?? it.key);
          val = toUIntBE(it.Value ?? it.value ?? it.val ?? it.raw);
        }
        if (!Number.isNaN(id) && val != null) out[id] = val;
      }
    }
    return out;
  }
  
  /* --------------- TELTONIKA PARSER --------------- */
// Debug helpers
// === Helpers CRC & framing ===
function crc16IBM(buf) {
    // CRC-16/IBM (poly 0xA001), init 0x0000, sin XOR final
    let crc = 0x0000;
    for (let i = 0; i < buf.length; i++) {
      crc ^= buf[i];
      for (let b = 0; b < 8; b++) {
        const lsb = crc & 1;
        crc >>>= 1;
        if (lsb) crc ^= 0xA001;
      }
    }
    return crc & 0xFFFF;
  }
  
  function hasTeltonikaCRC(avlPayload) {
    if (avlPayload.length < 5) return false;
    // En Teltonika el CRC son 4 bytes, los 2 primeros suelen ser 0x0000
    const a = avlPayload[avlPayload.length - 4];
    const b = avlPayload[avlPayload.length - 3];
    return a === 0x00 && b === 0x00;
  }
  
  function makeTcpLikeFrame(avlCore /* sin CRC */) {
    const crc = crc16IBM(avlCore);
    const crcBuf = Buffer.alloc(4);
    crcBuf.writeUInt16BE(0x0000, 0);         // high word zero
    crcBuf.writeUInt16BE(crc, 2);            // CRC16
  
    const data = Buffer.concat([avlCore, crcBuf]);
    const preamble = Buffer.alloc(4, 0x00);  // 00000000
    const lenBuf = Buffer.alloc(4);
    lenBuf.writeUInt32BE(data.length, 0);    // Data_Length BE
  
    return Buffer.concat([preamble, lenBuf, data]);
  }
  
  // Debug helpers (silenciados)
  const DBG_TEL = false;
  const hexSlice = (buf, from, to) => buf.slice(Math.max(0, from), Math.min(buf.length, to)).toString('hex');
  const dlog = () => {};
  
  /* --------------- TELTONIKA PARSER (UDP, con LOGS y CRC) --------------- */
  function parseTeltonika(datagramBuf, geofences) {
    try {
      dlog('len=', datagramBuf.length, 'head16=', hexSlice(datagramBuf, 0, 16));
  
      // 1) localizar 000F + IMEI(15)
      const tag = Buffer.from([0x00, 0x0f]);
      const imeiIdx = datagramBuf.indexOf(tag);
      dlog('imeiIdx=', imeiIdx);
  
      if (imeiIdx < 0 || imeiIdx + 2 + 15 > datagramBuf.length) {
        dlog('NO 000F+IMEI o fuera de rango. window=', hexSlice(datagramBuf, 0, 64));
        return [];
      }
  
      const imei = datagramBuf.slice(imeiIdx + 2, imeiIdx + 2 + 15).toString();
      let codecOff = imeiIdx + 2 + 15;
  
      // 2) codec 0x08/0x8E (escaneo defensivo)
      const isCodec = (b) => b === 0x08 || b === 0x8E;
      if (!isCodec(datagramBuf[codecOff])) {
        let found = -1;
        for (let i = 0; i < 8 && codecOff + i < datagramBuf.length; i++) {
          if (isCodec(datagramBuf[codecOff + i])) { found = codecOff + i; break; }
        }
        if (found === -1) {
          dlog('Codec no encontrado. around=', hexSlice(datagramBuf, codecOff - 8, codecOff + 24));
          return [];
        }
        codecOff = found;
      }
      const codecByte = datagramBuf[codecOff];
      const avlCore = datagramBuf.slice(codecOff); // 8E.. hasta fin (con o sin CRC)
      dlog('IMEI=', imei, 'codecOff=', codecOff, 'codecByte=', codecByte?.toString(16));
      dlog('avlCore.len=', avlCore.length, 'avl.head32=', hexSlice(avlCore, 0, 32));
  
      // 3) Construir frame TCP-like con CRC si hace falta
      let frame = avlCore;
      if (!hasTeltonikaCRC(avlCore)) {
        frame = makeTcpLikeFrame(avlCore);
        dlog('CRC agregado. frame.len=', frame.length, 'frame.head32=', hexSlice(frame, 0, 32));
      } else {
        // si ya trae CRC, prepender preámbulo+len para complacer al parser
        frame = Buffer.concat([
          Buffer.alloc(4, 0x00),
          Buffer.from(Uint8Array.of(
            0,0,0,(avlCore.length & 0xFF) // esto funciona si long<256; mejor BE genérico:
          ))
        ]);
        const lenBE = Buffer.alloc(4); lenBE.writeUInt32BE(avlCore.length, 0);
        frame = Buffer.concat([Buffer.alloc(4,0x00), lenBE, avlCore]);
        dlog('Ya había CRC. Envolviendo con preámbulo+len. frame.len=', frame.length);
      }
  
      // 4) Parse: primero intentar con frame completo; si no, fallback al payload crudo
      let avl, records;
      try {
        const parser = new TeltonikaParser(frame); 
        avl = parser.getAvl(); 
        records = avl?.records || avl?.data || avl?.AVL_Datas || []; 
        dlog('parse(frame) -> records.len=', Array.isArray(records) ? records.length : 'NA');
        if (!Array.isArray(records) || records.length === 0) {
          throw new Error('sin records en frame');
        }
      } catch (e) {
        dlog('parse(frame) ERROR:', e?.message || e);
        try {
          const parser2 = new TeltonikaParser(avlCore); 
          const avl2 = parser2.getAvl(); 
          records = avl2?.records || avl2?.data || avl2?.AVL_Datas || []; 
          dlog('parse(avlCore) -> records.len=', Array.isArray(records) ? records.length : 'NA');
        } catch (e2) {
          dlog('fallback parse(avlCore) ERROR:', e2?.message || e2);
          return [];
        }
      }
  
      if (!Array.isArray(records) || records.length === 0) {
        dlog('SIN records después de ambos intentos.');
        return [];
      }
  
      // 5) Normalización (igual que antes)
      const packets = records.map((rec, idx) => {  
        const utime = rec.timestamp ?? rec.Utime ?? rec.UtimeMs ?? rec.utime ?? rec.utimeMs ?? 0;
        const ms = utime > 1e12 ? utime : utime * 1000;
        const mx = dayjs.utc(ms).tz(MX_TZ);
  
        const gps = rec.gps || rec.GPSelement || rec.gps_element || {};
        const latRaw = gps.Latitude ?? gps.latitude ?? rec.lat ?? rec.Lat ?? 0;
        const lngRaw = gps.Longitude ?? gps.longitude ?? rec.lng ?? rec.Lng ?? 0;
        const altitude = gps.Altitude ?? gps.altitude ?? rec.altitude ?? 0;
        const speedKmh = gps.Speed ?? gps.speed ?? rec.speed ?? 0;
        const heading  = gps.Angle ?? gps.angle ?? rec.angle ?? 0; 
        const sats     = gps.Satellites ?? gps.satellites ?? rec.satellites ?? 0;
  
        // Intentar varias rutas posibles para IO elements segun la libreria
        const ioCandidate =
          rec.IOelement || rec.io || rec.ioelement ||
          rec.IOElements || rec.ioElements || rec.IO || rec.Io || rec.iO ||
          rec['IO element'] || rec['IO elements'] || rec['io elements'] || {};

        // Logs para ver estructura del record y de IO
        try {
          dlog(`rec#${idx} keys:`, Object.keys(rec));
          dlog(`rec#${idx} full rec:`, JSON.stringify(rec).slice(0, 1200));
        } catch {}
        try {
          dlog(`rec#${idx} IO candidate type:`, typeof ioCandidate);
          if (ioCandidate && typeof ioCandidate === 'object') {
            const candKeys = Object.keys(ioCandidate);
            dlog(`rec#${idx} IO candidate keys:`, candKeys);
            if (ioCandidate.Elements) dlog(`rec#${idx} IO.Elements:`, JSON.stringify(ioCandidate.Elements).slice(0, 600));
            if (ioCandidate.elements) dlog(`rec#${idx} IO.elements:`, JSON.stringify(ioCandidate.elements).slice(0, 600));
            if (ioCandidate.oneByte)  dlog(`rec#${idx} IO.oneByte:`, JSON.stringify(ioCandidate.oneByte).slice(0, 600));
            if (ioCandidate.twoBytes) dlog(`rec#${idx} IO.twoBytes:`, JSON.stringify(ioCandidate.twoBytes).slice(0, 600));
            if (ioCandidate.fourBytes) dlog(`rec#${idx} IO.fourBytes:`, JSON.stringify(ioCandidate.fourBytes).slice(0, 600));
            if (ioCandidate.eightBytes) dlog(`rec#${idx} IO.eightBytes:`, JSON.stringify(ioCandidate.eightBytes).slice(0, 600));
            if (ioCandidate['1B']) dlog(`rec#${idx} IO[1B]:`, JSON.stringify(ioCandidate['1B']).slice(0, 600));
            if (ioCandidate['2B']) dlog(`rec#${idx} IO[2B]:`, JSON.stringify(ioCandidate['2B']).slice(0, 600));
            if (ioCandidate['4B']) dlog(`rec#${idx} IO[4B]:`, JSON.stringify(ioCandidate['4B']).slice(0, 600));
            if (ioCandidate['8B']) dlog(`rec#${idx} IO[8B]:`, JSON.stringify(ioCandidate['8B']).slice(0, 600));
          }
        } catch {}

        const io = ioCandidate;
        const elems = normalizeIoElements(io);

         
        const eventId = io.EventID ?? io.event_id ?? rec.event_id ?? rec.EventID ?? 0;

        // IO 66: External Voltage (mV) -> V
        let powerSupply;
        if (elems[66] != null) {
          const mv = Number(elems[66]);
    
          powerSupply = (mv / 1000).toFixed(2);
        }
        // Soporta keys string "66"/"67"
        if (powerSupply == null && elems['66'] != null) {
          const mv = Number(elems['66']); powerSupply = (mv / 1000).toFixed(2);
        }

        // IO 67: Battery Voltage (mV) -> V (opcional)
        let powerBat;
        if (elems[67] != null)         powerBat = (Number(elems[67]) / 1000).toFixed(2);
        else if (elems['67'] != null)  powerBat = (Number(elems['67']) / 1000).toFixed(2);

        // Logs de depuración para IO/powers
        try {
          dlog(`rec#${idx} IO raw:`, JSON.stringify(io).slice(0, 500));
        } catch {}
        dlog(`rec#${idx} IO keys:`, Object.keys(elems));
        dlog(`rec#${idx} IO[66]=`, elems[66], ' IO["66"]=', elems['66']);
        dlog(`rec#${idx} IO[67]=`, elems[67], ' IO["67"]=', elems['67']);
        dlog(`rec#${idx} powerSupply=`, powerSupply, ' powerBat=', powerBat);
  
        let lat, lng;
        // Decidir si viene como entero escalado (1e7) o ya en grados
        if (typeof latRaw === 'number') {
          if (Number.isInteger(latRaw) && Math.abs(latRaw) > 1000) {
            lat = s32(latRaw) / 1e7;
            dlog(`rec#${idx} lat from scaled int`, latRaw, '->', lat);
          } else {
            lat = Number(latRaw);
            dlog(`rec#${idx} lat as degrees`, lat);
          }
        } else {
          lat = Number(latRaw);
        }
        if (typeof lngRaw === 'number') {
          if (Number.isInteger(lngRaw) && Math.abs(lngRaw) > 1000) {
            lng = s32(lngRaw) / 1e7;
            dlog(`rec#${idx} lng from scaled int`, lngRaw, '->', lng);
          } else {
            lng = Number(lngRaw);
            dlog(`rec#${idx} lng as degrees`, lng);
          }
        } else {
          lng = Number(lngRaw);
        }

        dlog(`rec#${idx}`, { utime, lat, lng, speedKmh, heading, sats, eventId, ioKeys: Object.keys(elems) });
        const rawEventCode = Number(eventId) || 0;
        const unifiedEventCode = mapTeltonikaEventToUnified(elems, rawEventCode);
   
        const rssi = extractTeltonikaRssi(elems);
        return { 
            protocol: 'teltonika',
          imei,
          timeOfFix:   mx.format(),
          update_time: mx.format('YYYY-MM-DDTHH:mm:ss'),
          lat, lng,
          altitude: Number(altitude),
          speed: Number(speedKmh).toFixed(1),
          heading: Number(heading),
          satelites: Number(sats),
          // Teltonika no provee RSSI en estos registros; evita undefined para Redis
          rssi: rssi,
          eventCode: rawEventCode,
          unified_event_code: unifiedEventCode,
          unified_event_name: getUnifiedEventName(unifiedEventCode),
          powerSupply,     // <- calculado de IO 66 (mV)
          powerBat,        // <- calculado de IO 67 (mV)
          geofences
          
          
        };
      });
  
      dlog('packets.len=', packets.length);
      return packets;
  
    } catch (err) {
      dlog('parseTeltonika FATAL:', err?.message || err);
      return [];
    }
  }
  function extractTeltonikaRssi(elems) { 
    // Probar con TODOS los IDs comunes de RSSI en Teltonika
    const rssiValue = elems[21] ?? elems['21'] ?? 0;
    
    //console.log('📶 RSSI encontrado:', rssiValue, 'en ID 21');
    
    // El valor 5 es normal - escala típica de Teltonika (0-31)
    return Number(rssiValue);
}
  const UNIFIED_EVENT = {
  UNKNOWN: 0,
  PERIODIC: 31,
  ENGINE_OFF: 20,
  ENGINE_ON: 21,
  DEVICE_CONNECTED: 9001,
  DEVICE_DISCONNECTED: 9002,
  DUALCAM_EVENT: 9100,
};

function mapTeltonikaEventToUnified(elems, eventId) {
    // Sin evento (periódico)
    if (!eventId) return UNIFIED_EVENT.PERIODIC;

    const numericEventId = Number(eventId);

    // Eventos DualCam / cámara: prioridad alta.
    // Deben clasificarse antes que cualquier fallback de ignición por IO 239,
    // porque algunos paquetes 497/498/499 llegan acompañados de IO 239.
    if ([497, 498, 499, 11317].includes(numericEventId)) {
      return UNIFIED_EVENT.DUALCAM_EVENT;
    }

    // FMC125 / FMB suele reportar ignition en Event ID 239 (valor 0/1 en IO 239)
    if (numericEventId === 239) {
      const ign239 = Number(elems[239] ?? elems['239']);
      if (ign239 === 1) return UNIFIED_EVENT.ENGINE_ON;
      if (ign239 === 0) return UNIFIED_EVENT.ENGINE_OFF;
    }

    // Algunos firmwares pueden reportar por cambio de IO 1
    if (numericEventId === 1) {
      const ign1 = Number(elems[1] ?? elems['1']);
      if (ign1 === 1) return UNIFIED_EVENT.ENGINE_ON;
      if (ign1 === 0) return UNIFIED_EVENT.ENGINE_OFF;
    }

    // Fallback: si viene el IO 239 presente aunque el eventId sea otro.
    const fallbackIgn = Number(elems[239] ?? elems['239']);
    if (fallbackIgn === 1) return UNIFIED_EVENT.ENGINE_ON;
    if (fallbackIgn === 0) return UNIFIED_EVENT.ENGINE_OFF;

    return UNIFIED_EVENT.UNKNOWN;
}

function mapCalampEventToUnified(eventCode) {
  // En CalAmp ya vienen 20/21 para ignition en este flujo.
  if (Number(eventCode) === 20) return UNIFIED_EVENT.ENGINE_OFF;
  if (Number(eventCode) === 21) return UNIFIED_EVENT.ENGINE_ON;
  if (Number(eventCode) === 11317) return UNIFIED_EVENT.DUALCAM_EVENT;
  if (!eventCode) return UNIFIED_EVENT.PERIODIC;
  return Number(eventCode) || UNIFIED_EVENT.UNKNOWN;
}

function getUnifiedEventName(code) {
  const c = Number(code);
  if (c === UNIFIED_EVENT.ENGINE_ON) return 'engine_on';
  if (c === UNIFIED_EVENT.ENGINE_OFF) return 'engine_off';
  if (c === UNIFIED_EVENT.PERIODIC) return 'periodic';
  if (c === UNIFIED_EVENT.DEVICE_CONNECTED) return 'device_connected';
  if (c === UNIFIED_EVENT.DEVICE_DISCONNECTED) return 'device_disconnected';
  if (c === UNIFIED_EVENT.DUALCAM_EVENT) return 'dualcam_event';
  return 'unknown';
}
  
  function parse(datagram, geofences) {
    const buf = normalizeToBuffer(datagram);
    const hex = buf.toString('hex'); // <-- usa hex siempre en CalAmp
  
    // Teltonika
    const isTeltonika = hasCafeMagic(buf, 0, 256) || hasTeltonikaImei(buf);
    if (isTeltonika) {
      return parseTeltonika(buf, geofences); // [{...}, ...]
    }else {  
      
      const decimal = get_decimal(hex.substring(72, 80));
      const speed = (decimal * 0.036).toFixed(1); 
  
      const packet = { 
        protocol: 'calamp',
        OptionsByte:  hex.substring(0, 2),
        MobileIDLength: hex.substring(2, 4),
        imei:          hex.substring(4, 19),
        MobileIDLen:   hex.substring(20, 22),
        MobileIDType:  hex.substring(22, 24),
        Secuence:      hex.substring(28, 32),
        timeOfFix: dayjs.utc(get_decimal(hex.substring(40, 48)) * 1000).tz(MX_TZ).format(),
        update_time: dayjs.utc(get_decimal(hex.substring(32, 40)) * 1000).tz(MX_TZ).format('YYYY-MM-DDTHH:mm:ss'),
        lat: parse_LatLng(hex.substring(48, 56)),
        lng: parse_LatLng(hex.substring(56, 64)),
        altitude: get_decimal(hex.substring(64, 72)),
        speed,
        heading:   get_decimal(hex.substring(81, 84)),
        satelites: get_decimal(hex.substring(84, 86)),
        rssi: convertBase.uintToInt(
          convertBase.bin2dec(convertBase.hex2bin(hex.substring(92, 96))),
          10
        ),
        eventCode: get_decimal(hex.substring(106, 108)),
        unified_event_code: mapCalampEventToUnified(get_decimal(hex.substring(106, 108))),
        unified_event_name: getUnifiedEventName(mapCalampEventToUnified(get_decimal(hex.substring(106, 108)))),
        powerSupply: (get_decimal(hex.substring(112, 120)) / 1000).toFixed(2),
        powerBat:    (() => {
          const volts = Number((get_decimal(hex.substring(120, 128)) / 1000).toFixed(3));
          const vMin = Number(process.env.BATTERY_VMIN ?? 3.0);
          const vMax = Number(process.env.BATTERY_VMAX ?? 4.2);
          const span = vMax - vMin;
          if (!Number.isFinite(volts) || !Number.isFinite(span) || span <= 0) return null;
          const pct = ((volts - vMin) / span) * 100;
          return Number(Math.max(0, Math.min(100, pct)).toFixed(2));
        })(),
        odometroTotal:   get_decimal(hex.substring(136, 144)),
        odometroReporte: get_decimal(hex.substring(144, 152)),
        geofences
      };
      if (hex.substring(32, 40) === '0000000c') {
        packet.update_time = packet.timeOfFix;
      }
      return [packet]; // <-- array
    }
    return [];
  }
  

function isInGeofence(device, geofence,lat,lng) { 
        const point = [ lat,lng]; 
        if(geofence.shape_type == 'polygon'){
            const polygon = geofence.coordinates.map(coord => [coord.lat, coord.lng]); 
            return pointInPolygon(point, polygon);
        }else{ 
            let distance =  getDistanceFromLatLonInKm(geofence.coordinates.center.lat,geofence.coordinates.center.lng,lat,lng);
            if(distance <= geofence.coordinates.radius/1000){
                return true;
            } 
        }
    return false;
}

function getDistanceFromLatLonInKm(lat1,lon1,lat2,lon2){
 
    var R = 6371; // Radius of the earth in km
    var dLat = deg2rad(lat2-lat1);  // deg2rad below
    var dLon = deg2rad(lon2-lon1);
    var a =
    Math.sin(dLat/2) * Math.sin(dLat/2) +
    Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) *
    Math.sin(dLon/2) * Math.sin(dLon/2)
    ;
    var c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
    var d = R * c; // Distance in km
    return d;
}
function deg2rad(deg) {
    return deg * (Math.PI/180)
    }

const REGION_STATE_KEY_PATTERN = 'geofence:region_state:*';
const GEO_TRACE_ENABLED = process.env.GEO_TRACE_LOG !== 'false';
const regionStateCache = {
  loadedAt: 0,
  ttlMs: 5 * 60 * 1000,
  states: [],
};

function normalizeStateCode(raw) {
  if (!raw) return null;
  return String(raw).replace(/^MX-/i, '').trim().toUpperCase();
}

function pointInBBox(lat, lng, bbox) {
  if (!bbox) return false;
  return lat >= bbox.minLat && lat <= bbox.maxLat && lng >= bbox.minLng && lng <= bbox.maxLng;
}

function computeBBox(coords = []) {
  let minLat = Infinity, maxLat = -Infinity, minLng = Infinity, maxLng = -Infinity;
  for (const c of coords) {
    const lat = Number(c?.lat);
    const lng = Number(c?.lng);
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) continue;
    if (lat < minLat) minLat = lat;
    if (lat > maxLat) maxLat = lat;
    if (lng < minLng) minLng = lng;
    if (lng > maxLng) maxLng = lng;
  }
  if (!Number.isFinite(minLat) || !Number.isFinite(maxLat) || !Number.isFinite(minLng) || !Number.isFinite(maxLng)) {
    return null;
  }
  return { minLat, maxLat, minLng, maxLng };
}

async function scanKeys(matchPattern) {
  const keys = [];
  let cursor = '0';
  let guard = 0;
  do {
    const reply = await client.scan(String(cursor), { MATCH: matchPattern, COUNT: 200 });
    cursor = String(reply?.cursor ?? '0');
    keys.push(...(reply?.keys || []));
    guard += 1;
    if (guard > 2000) {
      console.error(`❌ scanKeys guard break | pattern=${matchPattern} | cursor=${cursor} | keys=${keys.length}`);
      break;
    }
  } while (cursor !== '0');
  return keys;
}

async function getRegionStatesFromRedis(force = false) {
  const now = Date.now();
  if (!force && regionStateCache.states.length > 0 && (now - regionStateCache.loadedAt) < regionStateCache.ttlMs) {
    return regionStateCache.states;
  }

  const keys = await scanKeys(REGION_STATE_KEY_PATTERN);
  const states = (await Promise.all(keys.map(async (key) => {
    const raw = await client.get(key);
    if (!raw) return null;
    let parsed;
    try { parsed = JSON.parse(raw); } catch { return null; }

    const coords = Array.isArray(parsed?.coordinates) ? parsed.coordinates : null;
    if (!coords || coords.length < 3 || parsed?.shape_type !== 'polygon') return null;

    const bbox = computeBBox(coords);
    if (!bbox) return null;

    return {
      id: Number(parsed.id),
      geofence_name: parsed.name ?? null,
      country_code: String(parsed.country || '').toUpperCase() || null,
      state_code: normalizeStateCode(parsed.state),
      polygon: coords.map(c => [Number(c.lat), Number(c.lng)]),
      bbox,
    };
  }))).filter(Boolean);

  regionStateCache.states = states;
  regionStateCache.loadedAt = now;
  return states;
}

async function resolveRegionForDevice(device, lat, lng, imei) {
  if (!Number.isFinite(Number(lat)) || !Number.isFinite(Number(lng))) {
    return null;
  }

  const states = await getRegionStatesFromRedis();
  if (!states.length) {
    return null;
  }

  const latN = Number(lat);
  const lngN = Number(lng);
  const deviceKey = `device:${imei}`;

  const prevStateCode = normalizeStateCode(await client.hGet(deviceKey, 'geo_state_code'));
  const prevCountryCode = String(await client.hGet(deviceKey, 'geo_country_code') || '').toUpperCase() || null;
  const prevGeofenceId = Number(await client.hGet(deviceKey, 'geo_state_geofence_id'));

  let sticky = null;
  if (Number.isFinite(prevGeofenceId) && prevGeofenceId > 0) {
    sticky = states.find(s => s.id === prevGeofenceId) || null;
  }
  if (!sticky && prevStateCode) {
    sticky = states.find(s => s.state_code === prevStateCode && (!prevCountryCode || s.country_code === prevCountryCode)) || null;
  }

  if (sticky && pointInBBox(latN, lngN, sticky.bbox) && pointInPolygon([latN, lngN], sticky.polygon)) {
    await client.hSet(deviceKey, {
      geo_country_code: sticky.country_code || '',
      geo_state_code: sticky.state_code || '',
      geo_state_geofence_id: String(sticky.id),
      geo_state_checked_at: String(Date.now()),
    });
    if (GEO_TRACE_ENABLED) {
    }
    return {
      country_code: sticky.country_code,
      state_code: sticky.state_code,
      geofence_id: sticky.id,
      name: sticky.geofence_name,
    };
  }

  const candidates = states.filter(s => pointInBBox(latN, lngN, s.bbox));
  const pool = candidates.length ? candidates : states;

  for (const state of pool) {
    if (pointInPolygon([latN, lngN], state.polygon)) {
      await client.hSet(deviceKey, {
        geo_country_code: state.country_code || '',
        geo_state_code: state.state_code || '',
        geo_state_geofence_id: String(state.id),
        geo_state_checked_at: String(Date.now()),
      });
      if (GEO_TRACE_ENABLED) {
      }
      return {
        country_code: state.country_code,
        state_code: state.state_code,
        geofence_id: state.id,
        name: state.geofence_name,
      };
    }
  }

  if (GEO_TRACE_ENABLED) {
  }

  return null;
}

async function checkDeviceGeofences(device,lat, lng,imei) {
  // return [];
        try {
            const customerId = device.customer_id; 
            
            // Obtener y validar geocercas
           // const geofenceKeys = await client.keys(`geofence:${customerId}:*`); 

            const keys = await client.keys(`geofence:${customerId}:*`);

const geofenceKeys = keys.filter(k => k.split(':').length === 3);


        //  console.log(`🧱 Geocercas en Redis para customer ${customerId}:`, geofenceKeys.length);

            const geofences = (await Promise.all(
                geofenceKeys.map(async key => {
                    const data = await client.get(key);
                    try {
                        const parsed = JSON.parse(data); 
                        return parsed?.id ? parsed : null; // Solo retornar objetos con ID
                    } catch {
                        return null;
                    }
                })
            )).filter(Boolean); // Filtrar entradas inválidas
    
            const deviceGeofences = [];
            
            // Verificar geocercas válidas
        
            for (const geofence of geofences) {
                const statusKey = `geofence:${customerId}:${geofence.id}:${device.id}`;
                let status = await client.get(statusKey);
                if (status === null || status === undefined) {
                    // Auto-heal: si faltó inicialización en Laravel, arrancar en "afuera".
                    await client.set(statusKey, '0');
                    status = '0';
                }
                if(status == 0){
                    if (isInGeofence(device, geofence,lat,lng)){ 
                        await client.set(statusKey, '1');
                        deviceGeofences.push({
                          id: geofence.id,
                          geofence_name: geofence.name ?? null,
                          device_id: device.id,
                          device_name: device.name ?? null,
                          status: 1,
                          customer_id: customerId,
                        });
                    }else{
                      //  console.log('sigue afuera')
                    }
                }
                if(status == 1){  
                    if (isInGeofence(device, geofence,lat,lng)){
                       // console.log('sigue adentro')
                    }else{
                        await client.set(statusKey, '0');
                        deviceGeofences.push({
                          id: geofence.id,
                          geofence_name: geofence.name ?? null,
                          device_id: device.id,
                          device_name: device.name ?? null,
                          status: 0,
                          customer_id: customerId,
                        });
                    }
                } 
            }
            
            return deviceGeofences;
        } catch (error) {
            console.error('❌ Error al verificar las geocercas:', error);
            return [];
        }
}


function normalizeToBuffer(input) {
    if (Buffer.isBuffer(input)) return input;
    const hex = String(input).replace(/\s+/g, '').toLowerCase();
    const isHex = /^[0-9a-f]+$/.test(hex) && hex.length % 2 === 0;
    return Buffer.from(isHex ? hex : input, isHex ? 'hex' : 'utf8');
  }
  
  /** Busca 0xCAFE en el rango [start, start+len) */
  function hasCafeMagic(buf, start = 0, len = 32) {
    const end = Math.min(buf.length, start + len);
    for (let i = start; i < end - 1; i++) {
      if (buf[i] === 0xCA && buf[i + 1] === 0xFE) return true;
    }
    return false;
  }
  
  /** Teltonika: 0x000F + 15 dígitos ASCII (IMEI) en cualquier parte del paquete */
  function hasTeltonikaImei(buf) {
    for (let i = 0; i < buf.length - 2 - 15; i++) {
      if (buf[i] === 0x00 && buf[i + 1] === 0x0F) {
        let allDigits = true;
        for (let k = 0; k < 15; k++) {
          const c = buf[i + 2 + k];
          if (c < 0x30 || c > 0x39) { allDigits = false; break; }
        }
        if (allDigits) return true;
      }
    }
    return false;
  }
  
  /** Heurística CalAmp: 0x8F temprano + dos epochs consecutivos en primeros ~64B */
  function looksLikeCalAmp(buf) {
    // 0x8F tempranero (opciones) en los primeros 24 bytes
    const has8fEarly = indexOfByte(buf, 0x8F, 0, 24) !== -1;
  
    // Buscar dos enteros de 4 bytes consecutivos plausibles como epoch (big-endian)
    // Ventana: primeros 64 bytes
    const earlyWindowEnd = Math.min(buf.length, 64);
    let twoEpochs = false;
    for (let i = 0; i <= earlyWindowEnd - 8; i++) {
      const t1 = buf.readUInt32BE(i);
      const t2 = buf.readUInt32BE(i + 4);
      if (isPlausibleUnix(t1) && isPlausibleUnix(t2)) {
        twoEpochs = true;
        break;
      }
    }
  
    return has8fEarly && twoEpochs;
  }
  
  function indexOfByte(buf, byte, start = 0, end = buf.length) {
    const lim = Math.min(end, buf.length);
    for (let i = start; i < lim; i++) if (buf[i] === byte) return i;
    return -1;
  }
  
  /** Epoch plausible (2000-01-01 .. 2100-01-01) en segundos */
  function isPlausibleUnix(x) {
    const MIN = 946684800;   // 2000-01-01
    const MAX = 4102444800;  // 2100-01-01
    return x >= MIN && x <= MAX;
  }
function hasValidGPS(rec) {
  return true;
    if (!rec || !rec.gps) return false;

    const { latitude, longitude } = rec.gps;

    // Regla 1: debe traer lat/lng válidos
    if (!latitude || !longitude) return false;
    if (latitude === 0 || longitude === 0) return false;
      const ioMap = {};
      for (const io of rec.ioElements) {
        ioMap[io.id] = io.value;
      }
    // Regla 2: RSSI no debe venir null (IO 66 normalmente)
    const rssi = ioMap[21]; 
    
    if (rssi === null || rssi === undefined) return false;

    // Regla 3: event_id debe ser válido para ubicación
    const validEvents = [0, 239, 240, 247];
    if (!validEvents.includes(rec.event_id)) return false;

    return true;
}


function buildProtocolRecord(imei, avlRecord) {
 
  const ts = new Date(avlRecord.timestamp);
   const ioMap = {};
  for (const io of avlRecord.ioElements) {
    ioMap[io.id] = io.value;
  }

  const lat = avlRecord.gps.latitude;
  const lng = avlRecord.gps.longitude;
  const altitude = avlRecord.gps.altitude;
  const speedKmh = avlRecord.gps.speed;
  const heading = avlRecord.gps.angle;
  const sats = avlRecord.gps.satellites;

  // IO IDs we need
  const rawPowerSupply = ioMap[66] ?? null;   // Teltonika suele enviar mV
  const powerBat    = ioMap[67] ?? null;   // mV
  const rawRssi     = ioMap[21] ?? null;   // Teltonika CSQ (0-31)

  const normalizeRssiToDbm = (value) => {
    if (value === null || value === undefined) return null;
    const n = Number(value);
    if (!Number.isFinite(n)) return null;

    // Teltonika CSQ 0..31 -> dBm (3GPP)
    if (n >= 0 && n <= 31) {
      return -113 + (2 * n);
    }

    // Si ya viniera en dBm, conservarlo
    return n;
  };

  const normalizePowerSupplyToVolts = (value) => {
    if (value === null || value === undefined) return null;
    const n = Number(value);
    if (!Number.isFinite(n)) return null;

    // Teltonika normalmente reporta mV (ej. 14155)
    if (n > 100) {
      return Number((n / 1000).toFixed(3));
    }

    // Si ya viene en volts (ej. 14.01), conservar
    return n;
  };

  const normalizeBatteryPercent = (value) => {
    if (value === null || value === undefined) return null;
    const n = Number(value);
    if (!Number.isFinite(n)) return null;

    // Si viene en mV (ej. 4043), convertir a V
    const volts = n > 100 ? (n / 1000) : n;

    // Rango típico Li-ion (respaldo interno): 3.0V..4.2V
    const vMin = Number(process.env.BATTERY_VMIN ?? 3.0);
    const vMax = Number(process.env.BATTERY_VMAX ?? 4.2);
    const span = vMax - vMin;
    if (!Number.isFinite(span) || span <= 0) return null;

    const pct = ((volts - vMin) / span) * 100;
    const clamped = Math.max(0, Math.min(100, pct));
    return Number(clamped.toFixed(2));
  };

  const rssi = normalizeRssiToDbm(rawRssi);
  const powerSupply = normalizePowerSupplyToVolts(rawPowerSupply);
  const eventCode   = avlRecord.event_id ?? null;
  const unifiedEventCode = mapTeltonikaEventToUnified(ioMap, Number(eventCode));
  const eventValue  = eventCode !== null ? (ioMap[Number(eventCode)] ?? null) : null;
  const odometer    = ioMap[16] ?? null; 
  return {
    protocol: {
      imei,
      timeOfFix: ts.toISOString(),
      update_time: ts.toISOString().slice(0, 19),
      lat,
      lng,
      altitude: Number(altitude),
      speed: Number(speedKmh).toFixed(1),
      heading: Number(heading),
      satelites: Number(sats),
      rssi: rssi !== null ? Number(rssi) : null,
      eventCode: Number(eventCode),
      event_id: eventCode,
      event_value: eventValue,
      event_value_text: (eventCode != null && EVENT_VALUE_DICTIONARY[eventCode] && eventValue != null) ? (EVENT_VALUE_DICTIONARY[eventCode][eventValue] ?? null) : null,
      powerSupply: powerSupply !== null ? Number(powerSupply) : null,
      powerBat: normalizeBatteryPercent(powerBat),
      odometer:  Number(odometer),
      event_code: eventCode,
      event_name: EVENT_DICTIONARY[eventCode] ?? "Evento desconocido",
      unified_event_code: unifiedEventCode,
      unified_event_name: getUnifiedEventName(unifiedEventCode),
    }
  };
}
function buildDrop(imei, parsed, device) {
 
  return {
    device_id: device?.id || null,
    customer_id: device?.customer_id || null,

    imei: imei,
    lat: parsed.protocol.lat,
    lng: parsed.protocol.lng,
    speed: parsed.protocol.speed,
    heading: parsed.protocol.heading,
    satelites: parsed.protocol.satelites,
    rssi: parsed.protocol.rssi,
    odometer:parsed.protocol.odometer,
    powerSupply: parsed.protocol.powerSupply,
    powerBat: parsed.protocol.powerBat,
    event_code: parsed.protocol.eventCode,
    event_name: parsed.protocol.event_name,
    event_id: parsed.protocol.event_id,
    event_value: parsed.protocol.event_value,
    event_value_text: parsed.protocol.event_value_text,
    unified_event_code: parsed.protocol.unified_event_code ?? null,
    unified_event_name: parsed.protocol.unified_event_name ?? null,

    stoped: Number(parsed.protocol.speed) <= 1 ? 1 : 0,
    update_time: parsed.protocol.update_time,

    device: device || null,
  };
}


const EVENT_VALUE_DICTIONARY = {
  239: { 0: 'Ignition Off', 1: 'Ignition On' },
  240: { 0: 'Movement Off', 1: 'Movement On' },
  499: { 0: 'Server request', 1: 'DIN1', 2: 'DIN2', 3: 'Crash', 4: 'Towing', 5: 'Idling', 6: 'Geofence', 7: 'Unplug', 8: 'Green Driving' },
  497: { 0: 'Camera not detected', 1: 'No Card', 2: 'Card mount failed', 3: 'Card mounted', 4: 'Card faulty' },
  498: { 0: 'Camera not detected', 1: 'No Card', 2: 'Card mount failed', 3: 'Card mounted', 4: 'Card faulty' },
  80: { 0: 'Home On Stop', 1: 'Home On Moving', 2: 'Roaming On Stop', 3: 'Roaming On Moving', 4: 'Unknown On Stop', 5: 'Unknown On Moving' },
  21: { 1: 'GSM Signal 1', 2: 'GSM Signal 2', 3: 'GSM Signal 3', 4: 'GSM Signal 4', 5: 'GSM Signal 5' },
  200: { 0: 'No Sleep', 1: 'GPS Sleep', 2: 'Deep Sleep', 3: 'Online Sleep', 4: 'Ultra Sleep' },
  69: { 0: 'GNSS OFF', 1: 'GNSS ON with fix', 2: 'GNSS ON without fix', 3: 'GNSS sleep', 4: 'GNSS ON with fix, invalid data' },
  10: { 0: 'SD not present', 1: 'SD present' },
};
const EVENT_DICTIONARY = {
  // --- Básicos / Default ---
  0: "Periodic / Sin evento",
  1: "Digital Input 1",
  2: "Digital Input 2",
  3: "Digital Input 3",
  4: "Digital Input 4",

  // --- Encendido / Movimiento ---
  239: "Ignition",
  240: "Movement",
  250: "Trip Start/Stop",
  251: "Idling",
  21: "GSM Signal Change",
  69: "GNSS Status Change",
  200: "Sleep Mode Change",

  // --- Voltajes / Energía ---
  66: "External Voltage Change",
  67: "Internal Battery Voltage Change",
  68: "Battery Current Change",
  252: "Unplug (batería desconectada)",
  253: "Power Lost",
  254: "Power Restored",

  // --- Geocercas ---
  155: "Geofence Zone 1",
  156: "Geofence Zone 2",
  157: "Geofence Zone 3",
  158: "Geofence Zone 4",
  159: "Geofence Zone 5",
  160: "Geofence Zone 6",
  161: "Geofence Zone 7",
  162: "Geofence Zone 8",
  163: "Geofence Zone 9",
  164: "Geofence Zone 10",

  // --- Velocidad ---
  255: "Over Speeding",
  256: "Speed Normalized",

  // --- Eventos de Seguridad ---
  247: "Crash Detection",
  248: "Towing Detection",
  249: "Jamming Detection",

  // --- ADC / Entradas analógicas ---
  9: "ADC1 Change",
  10: "ADC2 Change",
  11: "ADC3 Change",

  // --- Salidas digitales ---
  179: "DOUT1 Change",
  180: "DOUT2 Change",

  // --- Odometer y counters ---
  16: "Total Odometer Update",
  449: "Ignition ON Counter",

  // --- CAN / OBD / FMS ---
  80: "Fuel Level CAN Change",
  81: "Engine RPM Change",
  82: "Vehicle Speed CAN Change",
  83: "Engine Temperature Change",
  84: "Fuel Used CAN Update",
  85: "FMS Total Fuel Used",
  86: "FMS Total Engine Hours",

  // --- BLE Sensor / EYE Sensor ---
  385: "BLE Event / EYE Sensor Trigger",
  386: "BLE Temperature Change",
  387: "BLE Humidity Change",
  388: "BLE Movement Detected",
  389: "BLE Shock Detection",

  // --- ADAS / DSM / DualCam ---
  10800: "DSM: Driver Fatigue",
  10801: "DSM: Distracted Driving",
  10802: "DSM: Phone Use",
  10803: "DSM: Smoking",
  10804: "DSM: Yawn",
  10805: "DSM: No Driver Detected",

  10820: "ADAS: Lane Departure Warning",
  10821: "ADAS: Forward Collision Warning",
  10822: "ADAS: Pedestrian Collision Warning",
  10823: "ADAS: Distance Too Close",
  10824: "ADAS: Speed Limit Sign Detected",

  10826: "ADAS Event Trigger",
  10827: "ADAS Event Trigger",
  10828: "ADAS Event Trigger",
  10829: "ADAS Event Trigger",
  10830: "ADAS Event Trigger",
  10831: "ADAS Event Trigger",

  // --- DualCam / Camera ---
  497: "DualCam Rear Camera TF State",
  498: "DualCam Front Camera TF State",
  499: "DualCam SOS Trigger",
  12969: "DualCam Front Camera Running State (COM1)",
  12970: "DualCam Rear Camera Running State (COM1)",
  12972: "DualCam Front Camera Running State (COM2)",
  12973: "DualCam Rear Camera Running State (COM2)",

  // --- DualCam / Photo / Video (legacy mappings usados en app) ---
  901: "Photo Captured",
  902: "Video Started",
  903: "Video Completed",
  904: "Camera Error",
  905: "Media Upload Started",
  906: "Media Upload Completed",

  // --- Internos / especiales del firmware ---
  300: "Device Restarted",
  301: "Configuration Changed",
  302: "Firmware Update",
  303: "Accelerometer Calibration Completed",
  304: "GNSS Module Restarted",

  // --- Otros ---
  241: "GSM Operator Changed",
  181: "PDOP Changed",
  182: "HDOP Changed",
};

export { parse, get_decimal, parse_LatLng, checkDeviceGeofences, buildProtocolRecord, buildDrop, hasValidGPS, resolveRegionForDevice };