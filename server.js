const { createServer } = require("http");
const { Server } = require("socket.io");

const PORT = process.env.PORT || 3000;
const ALLOWED_ORIGINS = process.env.ALLOWED_ORIGINS
  ? process.env.ALLOWED_ORIGINS.split(",").map((s) => s.trim())
  : "*";

const httpServer = createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true, rooms: rooms.size }));
    return;
  }
  res.writeHead(404);
  res.end();
});

const io = new Server(httpServer, {
  cors: { origin: ALLOWED_ORIGINS, methods: ["GET", "POST"] },
});

const rooms = new Map();
const socketMeta = new Map();

function broadcastUsers(sessionId) {
  const room = rooms.get(sessionId);
  if (!room) return;
  io.to(sessionId).emit("users-updated", room.users);
}

function removeFromRoom(socket) {
  const meta = socketMeta.get(socket.id);
  if (!meta) return;
  const { sessionId, userid } = meta;
  const room = rooms.get(sessionId);
  if (room) {
    delete room.users[userid];
    socket.leave(sessionId);
    if (Object.keys(room.users).length === 0) {
      rooms.delete(sessionId);
    } else {
      if (userid === room.ownerUserId) {
        room.ownerUserId = Object.keys(room.users)[0];
      }
      broadcastUsers(sessionId);
    }
  }
  socketMeta.delete(socket.id);
}

io.on("connection", (socket) => {
  socket.on("open-room", (payload, cb) => {
    try {
      const { extra, maxPlayers, password } = payload || {};
      if (!extra || !extra.sessionid || !extra.userid) {
        return cb && cb("Requisição inválida");
      }
      const sessionId = extra.sessionid;
      if (rooms.has(sessionId)) {
        return cb && cb("Sala já existe");
      }
      const room = {
        ownerUserId: extra.userid,
        maxPlayers: maxPlayers || 4,
        password: password || null,
        users: {
          [extra.userid]: { ...extra, socketId: socket.id },
        },
      };
      rooms.set(sessionId, room);
      socketMeta.set(socket.id, { sessionId, userid: extra.userid });
      socket.join(sessionId);
      cb && cb(null);
      broadcastUsers(sessionId);
    } catch (err) {
      cb && cb("Erro no servidor: " + err.message);
    }
  });

  socket.on("join-room", (payload, cb) => {
    try {
      const { extra, password } = payload || {};
      if (!extra || !extra.sessionid || !extra.userid) {
        return cb && cb("Requisição inválida");
      }
      const sessionId = extra.sessionid;
      const room = rooms.get(sessionId);
      if (!room) return cb && cb("Sala não encontrada");
      if (room.password && room.password !== password) {
        return cb && cb("Senha incorreta");
      }
      const currentCount = Object.keys(room.users).length;
      if (currentCount >= room.maxPlayers) {
        return cb && cb("Sala cheia");
      }
      room.users[extra.userid] = { ...extra, socketId: socket.id };
      socketMeta.set(socket.id, { sessionId, userid: extra.userid });
      socket.join(sessionId);
      cb && cb(null, room.users);
      broadcastUsers(sessionId);
    } catch (err) {
      cb && cb("Erro no servidor: " + err.message);
    }
  });

  socket.on("leave-room", () => {
    const meta = socketMeta.get(socket.id);
    if (meta) removeFromRoom(socket);
  });

  socket.on("webrtc-signal", (data) => {
    if (!data || !data.target) return;
    const { target, ...rest } = data;
    io.to(target).emit("webrtc-signal", { ...rest, sender: socket.id });
  });

  socket.on("data-message", (data) => {
    const meta = socketMeta.get(socket.id);
    if (!meta) return;
    socket.to(meta.sessionId).emit("data-message", data);
  });

  socket.on("disconnect", () => removeFromRoom(socket));
});

httpServer.listen(PORT, () => {
  console.log(`Servidor de sinalização do Netplay rodando na porta ${PORT}`);
});
