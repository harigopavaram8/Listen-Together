const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const PUBLIC_DIR = path.join(__dirname, 'public');
const UPLOAD_DIR = path.join(__dirname, 'uploads');
const MAX_BYTES = 100 * 1024 * 1024; // 100 MB per uploaded track
const ALLOWED_EXT = ['.mp3', '.wav', '.ogg', '.m4a', '.aac', '.flac', '.webm'];
const MIME = {
  '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css',
  '.mp3': 'audio/mpeg', '.wav': 'audio/wav', '.ogg': 'audio/ogg', '.m4a': 'audio/mp4',
  '.aac': 'audio/aac', '.flac': 'audio/flac', '.webm': 'audio/webm',
};

fs.mkdirSync(UPLOAD_DIR, { recursive: true });

// ---------- Room state ----------
const rooms = new Map();

function getRoom(id) {
  if (!rooms.has(id)) {
    rooms.set(id, {
      id,
      track: null,      // uploaded-track mode: { url, name, file }
      playing: false,
      position: 0,
      updatedAt: Date.now(),
      host: null,       // live mode: the websocket currently streaming
      clients: new Set(),
    });
  }
  return rooms.get(id);
}

function currentPosition(room) {
  return room.playing ? room.position + (Date.now() - room.updatedAt) / 1000 : room.position;
}

function snapshot(room) {
  return {
    type: 'state',
    track: room.track ? { url: room.track.url, name: room.track.name } : null,
    playing: room.playing,
    position: currentPosition(room),
    listeners: room.clients.size,
    live: !!room.host,
    hostId: room.host ? room.host.id : null,
  };
}

function broadcast(room) {
  const msg = JSON.stringify(snapshot(room));
  for (const c of room.clients) if (c.readyState === 1) c.send(msg);
}

function removeTrackFile(room) {
  if (room.track && room.track.file) fs.unlink(room.track.file, () => {});
}

const validRoom = (id) => /^[a-z0-9-]{1,32}$/i.test(id || '');

// ---------- HTTP: static files (with Range support for audio), upload ----------
function json(res, code, obj) {
  res.writeHead(code, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(obj));
}

function serveFile(req, res, filePath) {
  fs.stat(filePath, (err, st) => {
    if (err || !st.isFile()) { res.writeHead(404); return res.end('Not found'); }
    const type = MIME[path.extname(filePath).toLowerCase()] || 'application/octet-stream';
    const range = /^bytes=(\d*)-(\d*)$/.exec(req.headers.range || '');
    if (range && (range[1] || range[2])) {
      let start = range[1] ? parseInt(range[1], 10) : st.size - parseInt(range[2], 10);
      let end = range[1] && range[2] ? parseInt(range[2], 10) : st.size - 1;
      start = Math.max(0, start); end = Math.min(end, st.size - 1);
      if (start > end) { res.writeHead(416, { 'Content-Range': `bytes */${st.size}` }); return res.end(); }
      res.writeHead(206, {
        'Content-Type': type, 'Accept-Ranges': 'bytes',
        'Content-Range': `bytes ${start}-${end}/${st.size}`, 'Content-Length': end - start + 1,
      });
      return fs.createReadStream(filePath, { start, end }).pipe(res);
    }
    res.writeHead(200, { 'Content-Type': type, 'Accept-Ranges': 'bytes', 'Content-Length': st.size });
    fs.createReadStream(filePath).pipe(res);
  });
}

function handleUpload(req, res, roomId, query) {
  if (!validRoom(roomId)) return json(res, 400, { error: 'Bad room id' });

  const name = String(query.get('name') || 'track').slice(0, 120);
  const ext = path.extname(name).toLowerCase();
  if (!ALLOWED_EXT.includes(ext)) return json(res, 400, { error: 'Unsupported audio type' });

  const declared = Number(req.headers['content-length'] || 0);
  if (declared > MAX_BYTES) return json(res, 413, { error: 'File too large' });

  const fileName = crypto.randomBytes(12).toString('hex') + ext;
  const filePath = path.join(UPLOAD_DIR, fileName);
  const out = fs.createWriteStream(filePath);
  let received = 0;
  let aborted = false;

  req.on('data', (chunk) => {
    received += chunk.length;
    if (received > MAX_BYTES && !aborted) {
      aborted = true;
      out.destroy();
      fs.unlink(filePath, () => {});
      json(res, 413, { error: 'File too large' });
      req.destroy();
    }
  });
  req.pipe(out);

  out.on('finish', () => {
    if (aborted) return;
    const room = getRoom(roomId);
    removeTrackFile(room);
    room.track = { url: '/uploads/' + fileName, name, file: filePath };
    room.playing = false;
    room.position = 0;
    room.updatedAt = Date.now();
    broadcast(room);
    json(res, 200, { ok: true });
  });
  out.on('error', () => { if (!aborted) json(res, 500, { error: 'Upload failed' }); });
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://localhost');
  let pathname;
  try { pathname = decodeURIComponent(url.pathname); } catch { res.writeHead(400); return res.end(); }

  if (req.method === 'POST' && pathname.startsWith('/upload/')) {
    return handleUpload(req, res, pathname.slice('/upload/'.length), url.searchParams);
  }
  if (req.method !== 'GET' && req.method !== 'HEAD') { res.writeHead(405); return res.end(); }

  if (pathname.startsWith('/uploads/')) {
    const file = path.join(UPLOAD_DIR, path.basename(pathname));
    return serveFile(req, res, file);
  }
  const rel = pathname === '/' ? 'index.html' : pathname.slice(1);
  const file = path.join(PUBLIC_DIR, rel);
  if (!file.startsWith(PUBLIC_DIR + path.sep)) { res.writeHead(403); return res.end(); }
  serveFile(req, res, file);
});

// ---------- WebSocket: sync + WebRTC signaling ----------
const wss = new WebSocketServer({ server });

function endLive(room, ws) {
  if (room.host === ws) {
    room.host = null;
    broadcast(room);
  }
}

wss.on('connection', (ws) => {
  ws.id = crypto.randomBytes(6).toString('hex');
  ws.room = null;
  ws.send(JSON.stringify({ type: 'hello', id: ws.id }));

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    if (msg.type === 'ping') return ws.send(JSON.stringify({ type: 'pong', t: msg.t }));

    if (msg.type === 'join') {
      if (!validRoom(msg.room)) return;
      const room = getRoom(msg.room);
      ws.room = room;
      room.clients.add(ws);
      broadcast(room);
      // Tell a live host that a new listener needs a stream
      if (room.host && room.host !== ws && room.host.readyState === 1) {
        room.host.send(JSON.stringify({ type: 'listener-joined', id: ws.id }));
      }
      return;
    }

    const room = ws.room;
    if (!room) return;

    if (msg.type === 'sync') return ws.send(JSON.stringify(snapshot(room)));

    // ----- Live mode -----
    if (msg.type === 'go-live') {
      if (room.host && room.host !== ws && room.host.readyState === 1) {
        room.host.send(JSON.stringify({ type: 'live-ended' })); // takeover
      }
      room.host = ws;
      room.playing = false; // pause uploaded-track mode
      room.position = currentPosition(room);
      room.updatedAt = Date.now();
      broadcast(room);
      for (const c of room.clients) {
        if (c !== ws && c.readyState === 1) {
          ws.send(JSON.stringify({ type: 'listener-joined', id: c.id }));
        }
      }
      return;
    }

    if (msg.type === 'stop-live') return endLive(room, ws);

    if (msg.type === 'signal') {
      for (const c of room.clients) {
        if (c.id === msg.to && c.readyState === 1) {
          c.send(JSON.stringify({ type: 'signal', from: ws.id, data: msg.data }));
          break;
        }
      }
      return;
    }

    // ----- Uploaded-track mode -----
    const pos = Math.max(0, Number(msg.position) || 0);
    if (msg.type === 'play') {
      room.playing = true; room.position = pos; room.updatedAt = Date.now(); broadcast(room);
    } else if (msg.type === 'pause') {
      room.playing = false; room.position = pos; room.updatedAt = Date.now(); broadcast(room);
    } else if (msg.type === 'seek') {
      room.position = pos; room.updatedAt = Date.now(); broadcast(room);
    }
  });

  ws.on('close', () => {
    const room = ws.room;
    if (!room) return;
    room.clients.delete(ws);
    if (room.host === ws) room.host = null;
    if (room.clients.size === 0) {
      removeTrackFile(room);
      rooms.delete(room.id);
    } else {
      broadcast(room);
    }
  });
});

server.listen(PORT, () => console.log(`Listen Together running on http://localhost:${PORT}`));
