const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const crypto = require('crypto');
const path = require('path');

const app = express();
const server = http.createServer(app);

// Serve static files (frontend)
app.use(express.static(path.join(__dirname, 'public')));
app.use(express.static(__dirname));

// ─── CORS CONFIG ──────────────────────────────────────────────────────────────
const io = new Server(server, {
  cors: {
    origin: true,
    methods: ['GET', 'POST'],
  },
});

// ─── IN-MEMORY STATE ──────────────────────────────────────────────────────────
const waitingQueue = []; // [{ id, interests, maxWait, queuedAt }]
const activePairs = new Map(); // socketId → partnerId
const userSettings = new Map(); // socketId → { interests, maxWait }
const sessions = new Map(); // socketId → session metadata

// ─── HELPERS ──────────────────────────────────────────────────────────────────
function generateSessionId() {
  return crypto.randomBytes(8).toString('hex');
}

function getOnlineCount() {
  return io.engine.clientsCount;
}

function broadcastOnlineCount() {
  io.emit('online_count', getOnlineCount());
}

function removeFromQueue(socketId) {
  const idx = waitingQueue.findIndex(u => u.id === socketId);
  if (idx !== -1) waitingQueue.splice(idx, 1);
}

function pairUsers(socket1Id, socket2Id, sharedInterests = []) {
  const sessionId = generateSessionId();

  activePairs.set(socket1Id, socket2Id);
  activePairs.set(socket2Id, socket1Id);

  sessions.set(socket1Id, { sessionId, paired: true, pairedAt: Date.now() });
  sessions.set(socket2Id, { sessionId, paired: true, pairedAt: Date.now() });

  io.to(socket1Id).emit('chat_start', { sessionId, sharedInterests });
  io.to(socket2Id).emit('chat_start', { sessionId, sharedInterests });

  console.log(`[pair] ${socket1Id.slice(0,6)} ↔ ${socket2Id.slice(0,6)} (${sessionId}) shared: [${sharedInterests.join(', ')}]`);
}

function cleanupUser(socketId) {
  removeFromQueue(socketId);
  userSettings.delete(socketId);

  const partnerId = activePairs.get(socketId);
  if (partnerId) {
    activePairs.delete(socketId);
    activePairs.delete(partnerId);
    sessions.delete(socketId);
    sessions.delete(partnerId);

    // Notify partner
    io.to(partnerId).emit('stranger_disconnected');
  }

  sessions.delete(socketId);
}

function sendQueuePositions() {
  waitingQueue.forEach((user, index) => {
    io.to(user.id).emit('queue_position', index + 1);
  });
}

function tryMatch() {
  if (waitingQueue.length < 2) return;

  let matched = true;
  while (matched && waitingQueue.length >= 2) {
    matched = false;
    // 1. Try Interest-based Matching
    for (let i = 0; i < waitingQueue.length; i++) {
      for (let j = i + 1; j < waitingQueue.length; j++) {
        const u1 = waitingQueue[i];
        const u2 = waitingQueue[j];

        const shared = u1.interests.filter(tag => u2.interests.includes(tag));
        if (shared.length > 0) {
          waitingQueue.splice(j, 1);
          waitingQueue.splice(i, 1);
          pairUsers(u1.id, u2.id, shared);
          matched = true;
          break; // Break inner, will continue while loop
        }
      }
      if (matched) break; // Break outer
    }
  }

  // 2. Try Fallback (FIFO) for blind-eligible users
  if (waitingQueue.length >= 2) {
    const now = Date.now();
    const getBlindEligibleIdx = () => waitingQueue.findIndex(u => {
      if (u.interests.length === 0) return true;
      if (u.maxWait === -1) return false;
      return (now - u.queuedAt) > (u.maxWait * 1000);
    });

    let idx1 = getBlindEligibleIdx();
    if (idx1 !== -1) {
      for (let j = 0; j < waitingQueue.length; j++) {
        if (j === idx1) continue;
        
        const u2 = waitingQueue[j];
        const isU2Eligible = u2.interests.length === 0 || 
                             (u2.maxWait !== -1 && (now - u2.queuedAt) > (u2.maxWait * 1000));
        
        if (isU2Eligible) {
          const u1 = waitingQueue[idx1];
          const firstIdx = Math.min(idx1, j);
          const secondIdx = Math.max(idx1, j);
          waitingQueue.splice(secondIdx, 1);
          waitingQueue.splice(firstIdx, 1);
          pairUsers(u1.id, u2.id, []);
          return tryMatch(); // Fallback can still recurse once as it's rare
        }
      }
    }
  }

  sendQueuePositions();
}

// ─── SOCKET HANDLERS ──────────────────────────────────────────────────────────
io.on('connection', (socket) => {
  console.log(`[connect] ${socket.id.slice(0,6)} (total: ${getOnlineCount()})`);
  broadcastOnlineCount();

  socket.emit('online_count', getOnlineCount());

  // ── FIND STRANGER ──
  socket.on('find_stranger', (settings) => {
    if (activePairs.has(socket.id)) return;
    removeFromQueue(socket.id);

    const interests = Array.isArray(settings?.interests) ? settings.interests.slice(0, 10) : [];
    const maxWait = typeof settings?.maxWait === 'number' ? settings.maxWait : 30;

    userSettings.set(socket.id, { interests, maxWait });
    
    waitingQueue.push({
      id: socket.id,
      interests,
      maxWait,
      queuedAt: Date.now()
    });

    console.log(`[queue] ${socket.id.slice(0,6)} waiting with ${interests.length} tags (queue: ${waitingQueue.length})`);
    tryMatch();
  });

  // ── CANCEL SEARCH ──
  socket.on('cancel_search', () => {
    removeFromQueue(socket.id);
    sendQueuePositions();
    console.log(`[cancel] ${socket.id.slice(0,6)}`);
  });

  // ── MESSAGE ──
  socket.on('message', ({ text, id, replyTo }) => {
    if (typeof text !== 'string') return;
    const trimmed = text.trim().slice(0, 500);
    if (!trimmed) return;

    const partnerId = activePairs.get(socket.id);
    if (!partnerId) return;

    const timestamp = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    io.to(partnerId).emit('message', { text: trimmed, timestamp, id, replyTo });
    console.log(`[msg] ${socket.id.slice(0,6)} → ${partnerId.slice(0,6)}: "${trimmed.slice(0,40)}"`);
  });

  // ── VOICE MESSAGE ──
  socket.on('voice_message', ({ audio, id, replyTo }) => {
    const partnerId = activePairs.get(socket.id);
    if (!partnerId) return;

    io.to(partnerId).emit('voice_message', { audio, id, replyTo });
    console.log(`[voice] ${socket.id.slice(0,6)} → ${partnerId.slice(0,6)} (${id})`);
  });

  // ── EDIT MESSAGE ──
  socket.on('message_edit', ({ id, text }) => {
    const partnerId = activePairs.get(socket.id);
    if (!partnerId) return;

    io.to(partnerId).emit('message_edit', { id, text });
    console.log(`[edit] ${socket.id.slice(0,6)} edited ${id}`);
  });

  // ── REACTION ──
  socket.on('message_reaction', ({ msgId, emoji }) => {
    const partnerId = activePairs.get(socket.id);
    if (!partnerId) return;

    io.to(partnerId).emit('message_reaction', { msgId, emoji });
    console.log(`[react] ${socket.id.slice(0,6)} reacted to ${msgId} with ${emoji}`);
  });

  // ── MESSAGE STATUS ──
  socket.on('message_delivered', ({ msgId }) => {
    const partnerId = activePairs.get(socket.id);
    if (partnerId) {
      io.to(partnerId).emit('message_delivered', { msgId });
      console.log(`[delivered] ${socket.id.slice(0,6)} acknowledged ${msgId}`);
    }
  });

  socket.on('message_seen', ({ msgId }) => {
    const partnerId = activePairs.get(socket.id);
    if (partnerId) {
      io.to(partnerId).emit('message_seen', { msgId });
      console.log(`[seen] ${socket.id.slice(0,6)} viewed ${msgId}`);
    }
  });

  // ── TYPING ──
  socket.on('typing_start', () => {
    const partnerId = activePairs.get(socket.id);
    if (partnerId) io.to(partnerId).emit('typing_start');
  });

  socket.on('typing_stop', () => {
    const partnerId = activePairs.get(socket.id);
    if (partnerId) io.to(partnerId).emit('typing_stop');
  });

  // ── SKIP ──
  socket.on('skip_stranger', () => {
    const partnerId = activePairs.get(socket.id);
    if (partnerId) {
      activePairs.delete(socket.id);
      activePairs.delete(partnerId);
      sessions.delete(socket.id);
      sessions.delete(partnerId);
      io.to(partnerId).emit('stranger_skipped');
      console.log(`[skip] ${socket.id.slice(0,6)} skipped ${partnerId.slice(0,6)}`);
    }

    const settings = userSettings.get(socket.id) || { interests: [], maxWait: 30 };
    waitingQueue.push({
      id: socket.id,
      interests: settings.interests,
      maxWait: settings.maxWait,
      queuedAt: Date.now()
    });
    tryMatch();
  });

  // ── END CHAT ──
  socket.on('end_chat', () => {
    const partnerId = activePairs.get(socket.id);
    if (partnerId) {
      activePairs.delete(socket.id);
      activePairs.delete(partnerId);
      sessions.delete(socket.id);
      sessions.delete(partnerId);
      io.to(partnerId).emit('stranger_disconnected');
      console.log(`[end] ${socket.id.slice(0,6)} ended chat with ${partnerId.slice(0,6)}`);
    }
  });

  // ── DISCONNECT ──
  socket.on('disconnect', () => {
    cleanupUser(socket.id);
    broadcastOnlineCount();
    sendQueuePositions();
    console.log(`[disconnect] ${socket.id.slice(0,6)} (total: ${getOnlineCount()})`);
  });
});

setInterval(tryMatch, 1000);

app.get('/status', (req, res) => {
  res.json({
    status: 'ok',
    online: getOnlineCount(),
    waiting: waitingQueue.length,
    activePairs: activePairs.size / 2,
  });
});

const PORT = process.env.PORT || 8080;
server.listen(PORT, () => {
  console.log(`\n🟢 strangerz server running on port ${PORT}\n`);
});
