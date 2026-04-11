const sessionsBySocketId = new Map();
const socketsByImei = new Map();

function nowIso() {
  return new Date().toISOString();
}

function socketId(socket) {
  return `${socket.remoteAddress || 'unknown'}:${socket.remotePort || 0}->${socket.localPort || 0}`;
}

function upsertSession(socket) {
  const id = socketId(socket);
  let session = sessionsBySocketId.get(id);

  if (!session) {
    session = {
      id,
      imei: null,
      remoteAddress: socket.remoteAddress || null,
      remotePort: socket.remotePort || null,
      localPort: socket.localPort || null,
      connectedAt: nowIso(),
      lastSeenAt: nowIso(),
      bytesIn: 0,
      bytesOut: 0,
    };
    sessionsBySocketId.set(id, session);
  }

  return session;
}

function removeFromImeiIndex(socket, imei) {
  if (!imei) return;
  const set = socketsByImei.get(String(imei));
  if (!set) return;
  set.delete(socketId(socket));
  if (set.size === 0) socketsByImei.delete(String(imei));
}

function addToImeiIndex(socket, imei) {
  if (!imei) return;
  const key = String(imei);
  let set = socketsByImei.get(key);
  if (!set) {
    set = new Set();
    socketsByImei.set(key, set);
  }
  set.add(socketId(socket));
}

export function registerSocket(socket) {
  return upsertSession(socket);
}

export function bindImei(socket, imei) {
  const session = upsertSession(socket);
  if (session.imei && session.imei !== String(imei)) {
    removeFromImeiIndex(socket, session.imei);
  }
  session.imei = String(imei);
  session.lastSeenAt = nowIso();
  addToImeiIndex(socket, session.imei);
  return session;
}

export function touch(socket, bytesIn = 0, bytesOut = 0) {
  const session = upsertSession(socket);
  session.lastSeenAt = nowIso();
  session.bytesIn += Number(bytesIn) || 0;
  session.bytesOut += Number(bytesOut) || 0;
  return session;
}

export function unregisterSocket(socket) {
  const id = socketId(socket);
  const session = sessionsBySocketId.get(id);
  if (!session) return false;
  if (session.imei) removeFromImeiIndex(socket, session.imei);
  sessionsBySocketId.delete(id);
  return true;
}

export function countSessions() {
  return sessionsBySocketId.size;
}

export function listSessions() {
  return [...sessionsBySocketId.values()]
    .map((session) => ({ ...session }))
    .sort((a, b) => {
      const ai = a.imei || '';
      const bi = b.imei || '';
      if (ai !== bi) return ai.localeCompare(bi);
      return a.id.localeCompare(b.id);
    });
}

export function listImeis() {
  return [...socketsByImei.entries()]
    .map(([imei, set]) => ({ imei, connections: set.size }))
    .sort((a, b) => a.imei.localeCompare(b.imei));
}

export function getSnapshot() {
  return {
    activeSessions: countSessions(),
    activeImeis: listImeis(),
    sessions: listSessions(),
  };
}

export function printSnapshot() {
  const snapshot = getSnapshot();
  console.clear();
  console.log(`[${nowIso()}] activeSessions=${snapshot.activeSessions}`);
  if (!snapshot.sessions.length) {
    console.log('No active sessions');
    return;
  }
  console.table(snapshot.sessions.map((s) => ({
    imei: s.imei || '(pending-imei)',
    remote: `${s.remoteAddress || '-'}:${s.remotePort || '-'}`,
    connectedAt: s.connectedAt,
    lastSeenAt: s.lastSeenAt,
    bytesIn: s.bytesIn,
    bytesOut: s.bytesOut,
  })));
}
