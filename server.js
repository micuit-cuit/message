import http from "node:http";
import { fileURLToPath } from "node:url";
import express from "express";
import { Op } from "sequelize";
import { WebSocketServer } from "ws";
import {
  sequelize,
  User,
  Message,
  bus,
  userToApi,
  messageToApi,
  checkUser,
  userExists,
  createMessage,
} from "./db.js";

const PORT = Number(process.env.PORT ?? 5500);
const DOCS_PATH = fileURLToPath(new URL("../docs/API.md", import.meta.url));

const str = (v) => (typeof v === "string" ? v.trim() : "");
const toInt = (v) => {
  const n = Number.parseInt(v, 10);
  return Number.isFinite(n) ? n : null;
};
const ok = (res, extra = {}) => res.json({ status: "success", ...extra });
const fail = (res, message, status = "error", code = 200) =>
  res.status(code).json({ status, message });

// ------------------------------------------------------------------ Express

const app = express();

app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

// Accepte le JSON quel que soit le Content-Type (comme l'ancien PHP qui lisait php://input).
app.use(express.json({ type: () => true }));

const router = express.Router();

router.get("/docs", (req, res) => {
  res.type("text/markdown; charset=utf-8").sendFile(DOCS_PATH);
});

// API historique : tout passe par la query string sur "/".
router.get("/", async (req, res) => {
  const q = req.query;

  if (q.idUtilisateur !== undefined) {
    const u = await User.findByPk(toInt(q.idUtilisateur) ?? 0);
    return u ? ok(res, { data: userToApi(u) }) : fail(res, "User not found");
  }

  if (q.idMessage !== undefined) {
    const m = await Message.findByPk(toInt(q.idMessage) ?? 0);
    return m ? ok(res, { data: messageToApi(m) }) : fail(res, "Message not found");
  }

  if (q.NomExpediteur !== undefined || q.NomDestinataire !== undefined) {
    const where =
      q.NomExpediteur !== undefined
        ? { sender: str(q.NomExpediteur) }
        : { recipient: str(q.NomDestinataire) };
    const rows = await Message.findAll({ where, order: [["id", "ASC"]] });
    return rows.length
      ? ok(res, { data: rows.map(messageToApi) })
      : fail(res, "No messages found");
  }

  if (q.NomUtilisateur !== undefined && q.MotDePasse !== undefined) {
    return (await checkUser(str(q.NomUtilisateur), str(q.MotDePasse)))
      ? ok(res, { message: "Connection was successful" })
      : fail(res, "Connection has failed : Wrong username or password", "failed");
  }

  // Ajouts : listes complètes (utile pour l'onglet dev de l'app).
  if (q.all === "users") {
    const rows = await User.findAll({ order: [["id", "ASC"]] });
    return ok(res, { data: rows.map(userToApi) });
  }
  if (q.all === "messages") {
    const rows = await Message.findAll({ order: [["id", "ASC"]] });
    return ok(res, { data: rows.map(messageToApi) });
  }

  return fail(res, "No request found", "failed");
});

// Création d'un message ou d'un utilisateur selon les champs du JSON.
router.post("/", async (req, res) => {
  const b = req.body ?? {};

  if (b.NomExpediteur !== undefined && b.NomDestinataire !== undefined && b.Contenu !== undefined) {
    const sender = str(b.NomExpediteur);
    const recipient = str(b.NomDestinataire);
    const subject = str(b.Sujet).slice(0, 255);
    const content = typeof b.Contenu === "string" ? b.Contenu : "";

    if (!sender || !recipient || !content.trim()) return fail(res, "Missing parameters");
    if (!(await userExists(recipient))) return fail(res, "Recipient not found");

    const m = await createMessage({ sender, recipient, subject, content });
    return ok(res, { message: "Message added successfully", idMessage: m.id });
  }

  if (b.NomUtilisateur !== undefined && b.MotDePasse !== undefined) {
    const username = str(b.NomUtilisateur);
    const password = typeof b.MotDePasse === "string" ? b.MotDePasse : "";

    if (!username || !password) return fail(res, "Missing parameters");
    if (username.length > 64) return fail(res, "Username too long");
    if (await userExists(username)) return fail(res, "Username already taken");

    const u = await User.create({ username, password });
    return ok(res, { message: "User added successfully", idUtilisateur: u.id });
  }

  return fail(res, "Missing parameters");
});

// Requête d'attente (long polling) : répond dès qu'un message arrive pour l'utilisateur,
// ou avec une liste vide au bout de `timeout` secondes.
router.get("/wait", async (req, res) => {
  const user = str(req.query.NomUtilisateur);
  if (!(await checkUser(user, str(req.query.MotDePasse)))) {
    return fail(res, "Wrong username or password", "failed", 401);
  }

  const timeoutMs = Math.min(Math.max(toInt(req.query.timeout) ?? 25, 1), 60) * 1000;
  const since = toInt(req.query.since) ?? (await Message.max("id")) ?? 0;

  let done = false;
  let timer;

  const cleanup = () => {
    done = true;
    clearTimeout(timer);
    bus.off("message", onMessage);
  };
  const finish = (rows) => {
    if (done) return;
    cleanup();
    const next = rows.reduce((max, m) => Math.max(max, m.id), since);
    ok(res, { data: rows.map(messageToApi), next });
  };
  const onMessage = (m) => {
    if (m.recipient === user && m.id > since) finish([m]);
  };

  // On s'abonne avant de lire la base pour ne rater aucun message.
  bus.on("message", onMessage);
  timer = setTimeout(() => finish([]), timeoutMs);
  res.on("close", () => {
    if (!done) cleanup();
  });

  const pending = await Message.findAll({
    where: { recipient: user, id: { [Op.gt]: since } },
    order: [["id", "ASC"]],
  });
  if (pending.length) finish(pending);
});

app.use("/api_messagerie/index.php", router);
app.use("/", router);

app.use((err, req, res, next) => {
  if (err.type === "entity.parse.failed") return fail(res, "Invalid JSON", "error", 400);
  console.error(err);
  fail(res, "Server error: " + err.message, "error", 500);
});

// ---------------------------------------------------------------- WebSocket

const server = http.createServer(app);
const wss = new WebSocketServer({ noServer: true });
const sockets = new Map(); // username -> Set<WebSocket>

const sendJson = (ws, obj) => {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
};

server.on("upgrade", async (req, socket, head) => {
  try {
    const url = new URL(req.url ?? "/", "http://localhost");
    const user = str(url.searchParams.get("NomUtilisateur") ?? "");
    const pass = str(url.searchParams.get("MotDePasse") ?? "");

    if (!url.pathname.endsWith("/ws")) return socket.destroy();
    if (!(await checkUser(user, pass))) {
      socket.write("HTTP/1.1 401 Unauthorized\r\nConnection: close\r\n\r\n");
      return socket.destroy();
    }

    wss.handleUpgrade(req, socket, head, (ws) => onConnection(ws, user));
  } catch {
    socket.destroy();
  }
});

function onConnection(ws, user) {
  if (!sockets.has(user)) sockets.set(user, new Set());
  sockets.get(user).add(ws);

  ws.isAlive = true;
  ws.on("pong", () => (ws.isAlive = true));
  ws.on("close", () => {
    sockets.get(user)?.delete(ws);
    if (sockets.get(user)?.size === 0) sockets.delete(user);
  });

  ws.on("message", async (raw) => {
    try {
      const msg = JSON.parse(raw.toString());

      if (msg.type === "ping") return sendJson(ws, { type: "pong" });

      if (msg.type === "send") {
        const recipient = str(msg.NomDestinataire);
        const content = typeof msg.Contenu === "string" ? msg.Contenu : "";
        if (!recipient || !content.trim()) {
          return sendJson(ws, { type: "error", message: "Missing parameters" });
        }
        if (!(await userExists(recipient))) {
          return sendJson(ws, { type: "error", message: "Recipient not found" });
        }
        const m = await createMessage({
          sender: user,
          recipient,
          subject: str(msg.Sujet).slice(0, 255),
          content,
        });
        return sendJson(ws, { type: "ack", idMessage: m.id });
      }

      sendJson(ws, { type: "error", message: "Unknown type" });
    } catch {
      sendJson(ws, { type: "error", message: "Invalid JSON" });
    }
  });

  sendJson(ws, { type: "ready", user });
}

// Un nouveau message est poussé à toutes les connexions ouvertes du destinataire.
bus.on("message", (m) => {
  for (const ws of sockets.get(m.recipient) ?? []) {
    sendJson(ws, { type: "message", data: messageToApi(m) });
  }
});

// Ping régulier pour détecter les connexions mortes.
const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, 30_000);
heartbeat.unref();

// ------------------------------------------------------------------- Start

await sequelize.sync();
server.listen(PORT, "0.0.0.0", () => {
  console.log(`API messagerie sur http://0.0.0.0:${PORT}  (docs : /docs, ws : /ws)`);
});
