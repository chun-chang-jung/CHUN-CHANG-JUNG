// 사계절 행성 — 멀티 서버
// 게임 파일(public/index.html)을 보여 주고, WebSocket(/ws)으로 방마다 각자 상태를 주고받게 해 줍니다.
// 로그인 없음: 닉네임과 방 코드만으로 들어옵니다. 방은 메모리에만 있고, 비면 30분 뒤 사라집니다.
'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT) || 3000;
const LIMITS = {
  rooms: 1000,          // 방 최대 개수
  perRoom: 8,           // 방 하나에 최대 인원
  connections: 600,     // 서버 전체 동시 접속
  presenceBytes: 4096,  // 한 사람 상태 최대 크기
  messageBytes: 16384,  // 메시지 하나 최대 크기
  emptyRoomMs: 30 * 60 * 1000,
};
const INDEX = fs.readFileSync(path.join(__dirname, 'public', 'index.html'));
const ALPHA = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // 헷갈리는 글자(I, O, 0, 1) 뺌
const CODE_RE = /^[A-Z2-9]{4}$/;

// ── 웹 페이지 ──
const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/healthz') {
    res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
    res.end('ok');
    return;
  }
  if (url === '/' || url === '/index.html') {
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' });
    res.end(INDEX);
    return;
  }
  res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('없는 페이지예요');
});

// ── 방 ──
const rooms = new Map(); // code → { code, peers: Map(id → { ws, nick, p }), emptySince }
function newCode() {
  for (;;) {
    let c = '';
    for (let i = 0; i < 4; i++) c += ALPHA[crypto.randomInt(ALPHA.length)];
    if (!rooms.has(c)) return c;
  }
}
function makeRoom(code) {
  const r = { code, peers: new Map(), emptySince: 0 };
  rooms.set(code, r);
  return r;
}
const cleanNick = (s) => String(s || '')
  .replace(/[\u0000-\u001f\u007f-\u009f\u00ad\u200b-\u200f\u2028-\u202e\u2060-\u206f\ufeff]/g, '')
  .replace(/\s+/g, ' ').trim().slice(0, 12);
function send(ws, msg) {
  if (ws.readyState === 1) ws.send(JSON.stringify(msg));
}
function broadcast(room, msg, exceptId) {
  const text = JSON.stringify(msg);
  for (const [id, peer] of room.peers) if (id !== exceptId && peer.ws.readyState === 1) peer.ws.send(text);
}

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: LIMITS.messageBytes });
wss.on('connection', (ws) => {
  if (wss.clients.size > LIMITS.connections) {
    send(ws, { t: 'error', code: 'busy' });
    ws.close();
    return;
  }
  let room = null, id = null;
  let budget = 40, lastAt = Date.now(); // 초당 30개쯤까지 (잠깐 몰리면 40개)
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  function leave() {
    if (!room) return;
    room.peers.delete(id);
    broadcast(room, { t: 'left', id });
    if (room.peers.size === 0) room.emptySince = Date.now();
    room = null;
    id = null;
  }

  ws.on('message', (buf) => {
    const now = Date.now();
    budget = Math.min(40, budget + (now - lastAt) * 0.03);
    lastAt = now;
    if (budget < 1) return;
    budget -= 1;
    let m;
    try { m = JSON.parse(buf); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;

    if (m.t === 'create' || m.t === 'join') {
      leave();
      const nick = cleanNick(m.nick) || '친구';
      const asked = String(m.room || '').toUpperCase();
      let r;
      if (m.t === 'create') {
        if (rooms.size >= LIMITS.rooms) return send(ws, { t: 'error', code: 'busy' });
        r = makeRoom(newCode());
      } else {
        if (!CODE_RE.test(asked)) return send(ws, { t: 'error', code: 'no_room' });
        r = rooms.get(asked);
        // 다시 잇기(recreate): 서버가 새로 켜져 방이 사라졌으면 같은 코드로 다시 만듦
        if (!r && m.recreate === true && rooms.size < LIMITS.rooms) r = makeRoom(asked);
        if (!r) return send(ws, { t: 'error', code: 'no_room' });
      }
      if (r.peers.size >= LIMITS.perRoom) return send(ws, { t: 'error', code: 'full' });
      const cid = typeof m.cid === 'string' && /^[a-z0-9]{10}$/.test(m.cid) && !r.peers.has(m.cid) ? m.cid : crypto.randomBytes(5).toString('hex');
      id = cid;
      room = r;
      r.emptySince = 0;
      r.peers.set(id, { ws, nick, p: {} });
      send(ws, {
        t: 'joined', room: r.code, you: id, nick,
        peers: [...r.peers].filter(([pid]) => pid !== id).map(([pid, q]) => ({ id: pid, nick: q.nick, p: q.p })),
      });
      broadcast(r, { t: 'peer', id, nick, p: {} }, id);
    } else if (m.t === 'presence' && room) {
      const p = m.p;
      if (!p || typeof p !== 'object' || Array.isArray(p)) return;
      if (Buffer.byteLength(JSON.stringify(p)) > LIMITS.presenceBytes) return;
      room.peers.get(id).p = p;
      broadcast(room, { t: 'presence', id, p }, id);
    } else if (m.t === 'leave') {
      leave();
    }
  });
  ws.on('close', leave);
  ws.on('error', () => {});
});

// 끊긴 연결 정리(25초마다 확인) + 오래 빈 방 지우기
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
  const now = Date.now();
  for (const [code, r] of rooms) if (r.peers.size === 0 && r.emptySince && now - r.emptySince > LIMITS.emptyRoomMs) rooms.delete(code);
}, 25000).unref();

server.listen(PORT, () => console.log(`사계절 행성 서버가 켜졌어요: http://localhost:${PORT}`));
