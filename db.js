import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { EventEmitter } from "node:events";
import { Sequelize, DataTypes } from "sequelize";

const storage = process.env.DB_PATH ?? "data/messagerie.sqlite";
mkdirSync(dirname(storage), { recursive: true });

export const sequelize = new Sequelize({
  dialect: "sqlite",
  storage,
  logging: false,
});

export const User = sequelize.define(
  "User",
  {
    id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
    username: { type: DataTypes.STRING(64), allowNull: false, unique: true },
    password: { type: DataTypes.STRING(255), allowNull: false },
  },
  { tableName: "utilisateurs", timestamps: false },
);

export const Message = sequelize.define(
  "Message",
  {
    id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
    sender: { type: DataTypes.STRING(64), allowNull: false },
    recipient: { type: DataTypes.STRING(64), allowNull: false },
    subject: { type: DataTypes.STRING(255), allowNull: false, defaultValue: "" },
    content: { type: DataTypes.TEXT, allowNull: false },
  },
  {
    tableName: "messages",
    updatedAt: false,
    indexes: [{ fields: ["recipient", "id"] }, { fields: ["sender", "id"] }],
  },
);

// Les noms de champs JSON gardent ceux de l'ancienne API PHP pour rester compatible avec l'app.
export const userToApi = (u) => ({
  idUtilisateur: u.id,
  NomUtilisateur: u.username,
  MotDePasse: u.password,
});

export const messageToApi = (m) => ({
  idMessage: m.id,
  NomExpediteur: m.sender,
  NomDestinataire: m.recipient,
  Sujet: m.subject,
  Contenu: m.content,
  DateEnvoi: m.createdAt,
});

// Émet "message" à chaque nouveau message : consommé par le long polling et le WebSocket.
export const bus = new EventEmitter();
bus.setMaxListeners(0);

export async function checkUser(username, password) {
  if (!username || !password) return false;
  const u = await User.findOne({ where: { username } });
  return u !== null && u.password === password;
}

export async function userExists(username) {
  return (await User.count({ where: { username } })) > 0;
}

export async function createMessage({ sender, recipient, subject = "", content }) {
  const m = await Message.create({ sender, recipient, subject, content });
  bus.emit("message", m);
  return m;
}