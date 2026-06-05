const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);

// ─── CORS CONFIG ──────────────────────────────────────────────────────────────
// Replace with your GitHub Pages URL when deployed
const ALLOWED_ORIGINS = [
  'http://localhost:5500',
  'http://127.0.0.1:5500',
  'http://localhost:3000',
  'https://YOUR_GITHUB_USERNAME.github.io', // ← replace this
];

const io = new Server(server, {
  cors: {
    origin: ALLOWED_ORIGINS,
    methods: ['GET', 'POST'],
  },
});

// ─── IN-MEMORY STATE ──────────────────────────────────────────────────────────
const waitingQueue = []; // socket IDs waiting for a partner
const activePairs = new Map(); // socketId → partnerId
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
  const idx = waitingQueue.indexOf(socketId);
  if (idx !== -1) waitingQueue.splice(idx, 1);
}

function pairUsers(socket1Id, socket2Id) {
  const sessionId = generateSessionId();

  activePairs.set(socket1Id, socket2Id);
  activePairs.set(socket2Id, socket1Id);

  sessions.set(socket1Id, { sessionId, paired: true, pairedAt: Date.now() });
  sessions.set(socket2Id, { sessionId, paired: true, pairedAt: Date.now() });

  io.to(socket1Id).emit('chat_start', { sessionId });
  io.to(socket2Id).emit('chat_start', { sessionId });

  console.log(`[pair] ${socket1Id.slice(0,6)} ↔ ${socket2Id.slice(0,6)} (${sessionId})`);
}

function cleanupUser(socketId) {
  removeFromQueue(socketId);

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
  waitingQueue.forEach((socketId, index) => {
    io.to(socketId).emit('queue_position', index + 1);
  });
}

function tryMatch() {
  while (waitingQueue.length >= 2) {
    const id1 = waitingQueue.shift();
    const id2 = waitingQueue.shift();

    // Verify both sockets still exist
    const s1 = io.sockets.sockets.get(id1);
    const s2 = io.sockets.sockets.get(id2);

    if (s1 && s2) {
      pairUsers(id1, id2);
    } else {
      // Put valid one back
      if (s1) waitingQueue.unshift(id1);
      if (s2) waitingQueue.unshift(id2);
    }
  }
  sendQueuePositions();
}

// ─── SOCKET HANDLERS ──────────────────────────────────────────────────────────
io.on('connection', (socket) => {
  console.log(`[connect] ${socket.id.slice(0,6)} (total: ${getOnlineCount()})`);
  broadcastOnlineCount();

  // Send current count to new user
  socket.emit('online_count', getOnlineCount());

  // ── FIND STRANGER ──
  socket.on('find_stranger', () => {
    // Must not already be in a chat or queue
    if (activePairs.has(socket.id)) return;
    removeFromQueue(socket.id);

    waitingQueue.push(socket.id);
    console.log(`[queue] ${socket.id.slice(0,6)} waiting (queue: ${waitingQueue.length})`);
    sendQueuePositions();
    tryMatch();
  });

  // ── CANCEL SEARCH ──
  socket.on('cancel_search', () => {
    removeFromQueue(socket.id);
    sendQueuePositions();
    console.log(`[cancel] ${socket.id.slice(0,6)}`);
  });

  // ── MESSAGE ──
  socket.on('message', ({ text }) => {
    if (typeof text !== 'string') return;

    const trimmed = text.trim().slice(0, 500); // enforce limit server-side
    if (!trimmed) return;

    const partnerId = activePairs.get(socket.id);
    if (!partnerId) return;

    const timestamp = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    io.to(partnerId).emit('message', { text: trimmed, timestamp });

    console.log(`[msg] ${socket.id.slice(0,6)} → ${partnerId.slice(0,6)}: "${trimmed.slice(0,40)}"`);
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

    // Re-queue skipper
    waitingQueue.push(socket.id);
    sendQueuePositions();
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

// ─── HTTP HEALTH CHECK ────────────────────────────────────────────────────────
app.get('/', (req, res) => {
  res.json({
    status: 'ok',
    online: getOnlineCount(),
    waiting: waitingQueue.length,
    activePairs: activePairs.size / 2,
  });
});

// ─── START ────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`\n🟢 strangerz server running on port ${PORT}\n`);
});
