require('dotenv').config();
const path = require('path');
const fs = require('fs').promises;
const crypto = require('crypto');
const express = require('express');
const sql = require('mssql');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const cookieParser = require('cookie-parser');
const rateLimit = require('express-rate-limit');

const SECRET = process.env.JWT_SECRET;
if (!SECRET || SECRET.length < 32) throw new Error('ตั้งค่า JWT_SECRET ใน .env อย่างน้อย 32 ตัวอักษร');

const databaseName = process.env.DB_NAME || 'KitchenStock';
const cfg = {
  server: process.env.DB_SERVER || 'localhost',
  ...(process.env.DB_PORT ? { port: Number(process.env.DB_PORT) } : {}),
  database: databaseName,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  options: {
    ...(process.env.DB_INSTANCE ? { instanceName: process.env.DB_INSTANCE } : {}),
    encrypt: process.env.DB_ENCRYPT !== 'false',
    trustServerCertificate: process.env.DB_TRUST_CERT === 'true',
  },
};

async function connectDatabase() {
  // เชื่อม master ก่อน เพื่อให้เริ่มใช้งานได้แม้ยังไม่ได้สร้างฐานข้อมูล
  const masterPool = await new sql.ConnectionPool({ ...cfg, database: 'master' }).connect();
  try {
    await masterPool.request().input('name', sql.NVarChar(128), databaseName).query(`
      IF DB_ID(@name) IS NULL
      BEGIN
        DECLARE @createSql nvarchar(max) = N'CREATE DATABASE ' + QUOTENAME(@name);
        EXEC (@createSql);
      END;
    `);
  } finally {
    await masterPool.close();
  }

  const pool = await new sql.ConnectionPool(cfg).connect();
  // สร้างตารางที่จำเป็นเมื่อเริ่มระบบ โดยไม่ลบหรือเขียนทับข้อมูลเดิม
  await pool.request().query(`
    IF OBJECT_ID(N'dbo.Users', N'U') IS NULL
    BEGIN
      CREATE TABLE dbo.Users (
        Id int IDENTITY(1,1) NOT NULL PRIMARY KEY,
        Username nvarchar(50) NOT NULL UNIQUE,
        PasswordHash nvarchar(255) NOT NULL,
        Role nvarchar(20) NOT NULL
      );
    END;

    IF OBJECT_ID(N'dbo.Items', N'U') IS NULL
    BEGIN
      CREATE TABLE dbo.Items (
        Id int IDENTITY(1,1) NOT NULL PRIMARY KEY,
        Name nvarchar(200) NOT NULL,
        Qty decimal(12,3) NOT NULL CONSTRAINT DF_Items_Qty DEFAULT (0),
        Unit nvarchar(30) NOT NULL,
        Category nvarchar(100) NOT NULL,
        UpdatedAt datetime2(3) NOT NULL CONSTRAINT DF_Items_UpdatedAt DEFAULT (SYSUTCDATETIME())
      );
    END;

    IF OBJECT_ID(N'dbo.Requisitions', N'U') IS NULL
    BEGIN
      CREATE TABLE dbo.Requisitions (
        Id int IDENTITY(1,1) NOT NULL PRIMARY KEY,
        ItemId int NOT NULL,
        ItemName nvarchar(200) NOT NULL,
        Unit nvarchar(30) NOT NULL,
        Qty decimal(12,3) NOT NULL,
        ChefName nvarchar(100) NOT NULL,
        CreatedAt datetime2(3) NOT NULL CONSTRAINT DF_Requisitions_CreatedAt DEFAULT (SYSUTCDATETIME()),
        Status nvarchar(20) NOT NULL CONSTRAINT DF_Requisitions_Status DEFAULT (N'pending'),
        PreparedAt datetime2(3) NULL
      );
      CREATE INDEX IX_Requisitions_Status_CreatedAt ON dbo.Requisitions(Status, CreatedAt DESC, Id DESC);
    END;
  `);
  return pool;
}


// ---- LINE: แจ้งเตือนเข้ากลุ่มผ่าน Messaging API ----
const BATCH_MS = (+process.env.LINE_BATCH_SECONDS || 30) * 1000;   // รวมหลายรายการของเชฟคนเดียวเป็นข้อความเดียว
const MAX_PER_HOUR = +process.env.LINE_MAX_PER_HOUR || 30;          // กันถูกยิงจนโควต้า LINE หมด
let sentLog = [];
async function captureLineGroup(groupId) {
  if (!groupId || process.env.LINE_GROUP_ID) return;
  const token = process.env.LINE_CHANNEL_ACCESS_TOKEN;
  if (!token) return;
  try {
    const r = await fetch(`https://api.line.me/v2/bot/group/${encodeURIComponent(groupId)}/summary`, {
      headers: { Authorization: 'Bearer ' + token },
      signal: AbortSignal.timeout(8000),
    });
    if (!r.ok) return console.warn('[LINE] อ่านชื่อกลุ่มจาก group event ไม่สำเร็จ', r.status);
    const summary = await r.json();
    if (String(summary.groupName || '').trim().toLocaleLowerCase() !== 'a') return;
    const envPath = path.join(__dirname, '.env');
    let contents = await fs.readFile(envPath, 'utf8');
    if (/^LINE_GROUP_ID=.*$/m.test(contents)) {
      contents = contents.replace(/^LINE_GROUP_ID=.*$/m, `LINE_GROUP_ID=${groupId}`);
    } else {
      contents += `${contents.endsWith('\n') ? '' : '\n'}LINE_GROUP_ID=${groupId}\n`;
    }
    await fs.writeFile(envPath, contents, 'utf8');
    process.env.LINE_GROUP_ID = groupId;
    console.log('[LINE] พบและบันทึก group ID ของกลุ่ม A แล้ว');
  } catch (e) {
    console.warn('[LINE] บันทึก group ID อัตโนมัติไม่สำเร็จ', e.message);
  }
}
async function pushLine(text) {
  const token = process.env.LINE_CHANNEL_ACCESS_TOKEN, to = process.env.LINE_GROUP_ID;
  if (!token || !to) return console.log('[LINE ยังไม่ได้ตั้งค่า] ข้อความที่จะส่ง:\n' + text);
  const now = Date.now(); sentLog = sentLog.filter(t => now - t < 3600e3);
  if (sentLog.length >= MAX_PER_HOUR) return console.warn('[LINE] ถึงเพดานการส่งต่อชั่วโมงแล้ว ข้ามการแจ้งเตือนนี้');
  sentLog.push(now);
  try {
    const r = await fetch('https://api.line.me/v2/bot/message/push', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + token },
      body: JSON.stringify({ to, messages: [{ type: 'text', text: text.slice(0, 4900) }] }),
      signal: AbortSignal.timeout(8000),
    });
    if (!r.ok) console.error('[LINE] ส่งไม่สำเร็จ', r.status, await r.text());
  } catch (e) { console.error('[LINE] ผิดพลาด', e.message); }
}
const batches = new Map();
function notifyBatch(kind, chef, line) {
  chef = String(chef).replace(/\s+/g, ' ');
  const key = kind + '|' + chef; let b = batches.get(key);
  if (!b) batches.set(key, b = { lines: [], timer: null });
  b.lines.push(line); clearTimeout(b.timer);
  b.timer = setTimeout(() => {
    batches.delete(key);
    const head = kind === 'new' ? `🧑‍🍳 เชฟ ${chef} ขอเบิกวัตถุดิบ` : `✅ เตรียมวัตถุดิบเสร็จแล้ว (ของเชฟ ${chef})`;
    const tail = kind === 'new' ? '⏳ รอฝ่ายจัดซื้อจัดเตรียม' : '📦 เชฟมารับของได้เลย';
    pushLine([head, ...b.lines, tail].join('\n'));
  }, BATCH_MS);
}
// ---- end LINE ----

const h = fn => (req, res) => fn(req, res).catch(e => { console.error(e); res.status(500).json({ error: 'เซิร์ฟเวอร์ผิดพลาด' }); });
const itemDto = r => ({ id: String(r.Id), name: r.Name, qty: Number(r.Qty), unit: r.Unit, category: r.Category });
const str = (v, max) => typeof v === 'string' && v.trim().length > 0 && v.trim().length <= max ? v.trim() : null;

function requireAdmin(req, res, next) {
  try {
    const p = jwt.verify(req.cookies.token, SECRET);
    if (p.role !== 'admin') throw new Error();
    req.user = p; next();
  } catch { res.status(401).json({ error: 'กรุณาเข้าสู่ระบบผู้ดูแล' }); }
}

function parseItem(b) {
  const name = str(b.name, 200), unit = str(b.unit, 30), category = str(b.category, 100), qty = Number(b.qty);
  if (!name || !unit || !category || !Number.isFinite(qty) || qty < 0 || qty > 999999999) return null;
  return { name, unit, category, qty };
}

(async () => {
  const pool = await connectDatabase();
  const app = express();
  // cloudflared ส่ง X-Forwarded-For มาจากเครื่องเดียวกัน ให้เชื่อถือเฉพาะ loopback proxy
  app.set('trust proxy', 'loopback');
  app.disable('x-powered-by');
  app.use(express.json({ limit: '20kb', verify: (req, res, buf) => { req.rawBody = buf; } }), cookieParser());
  app.use(express.static(path.join(__dirname, 'public')));

  // สร้างหรือซิงก์บัญชีผู้ดูแลจาก .env ทุกครั้งที่เริ่มระบบ
  if (process.env.ADMIN_USERNAME && process.env.ADMIN_PASSWORD) {
    const username = process.env.ADMIN_USERNAME;
    const passwordHash = await bcrypt.hash(process.env.ADMIN_PASSWORD, 12);
    const existing = await pool.request().input('u', sql.NVarChar(50), username)
      .query('SELECT TOP (1) Id FROM Users WHERE Username=@u');
    if (existing.recordset.length) {
      await pool.request().input('id', sql.Int, existing.recordset[0].Id)
        .input('p', sql.NVarChar(255), passwordHash)
        .query("UPDATE Users SET PasswordHash=@p, Role='admin' WHERE Id=@id");
      console.log('อัปเดตบัญชี admin จาก .env แล้ว:', username);
    } else {
      await pool.request().input('u', sql.NVarChar(50), username)
        .input('p', sql.NVarChar(255), passwordHash)
        .query("INSERT INTO Users(Username,PasswordHash,Role) VALUES(@u,@p,'admin')");
      console.log('สร้างบัญชี admin จาก .env แล้ว:', username);
    }
  }

  // ---- auth ----
  app.post('/api/login', rateLimit({ windowMs: 15 * 60 * 1000, max: 10, standardHeaders: true, message: { error: 'ลองใหม่อีกครั้งภายหลัง' } }),
    h(async (req, res) => {
      const u = str(req.body.username, 50), pw = typeof req.body.password === 'string' ? req.body.password : '';
      const r = u && await pool.request().input('u', sql.NVarChar, u).query('SELECT Id,PasswordHash,Role FROM Users WHERE Username=@u');
      const row = r && r.recordset[0];
      if (!row || row.Role !== 'admin' || !(await bcrypt.compare(pw, row.PasswordHash)))
        return res.status(401).json({ error: 'ชื่อผู้ใช้หรือรหัสผ่านไม่ถูกต้อง' });
      const token = jwt.sign({ uid: row.Id, role: row.Role }, SECRET, { expiresIn: '8h' });
      res.cookie('token', token, { httpOnly: true, sameSite: 'strict', secure: process.env.COOKIE_SECURE === 'true', maxAge: 8 * 3600 * 1000 });
      res.json({ ok: true });
    }));
  app.post('/api/logout', (req, res) => { res.clearCookie('token'); res.json({ ok: true }); });
  app.get('/api/me', (req, res) => {
    try { res.json({ admin: jwt.verify(req.cookies.token, SECRET).role === 'admin' }); } catch { res.json({ admin: false }); }
  });

  // ---- สาธารณะ: เชฟดูสต็อก/ประวัติ และเบิกได้โดยไม่ต้องล็อกอิน ----
  app.get('/api/items', h(async (req, res) => {
    const r = await pool.request().query('SELECT Id,Name,Qty,Unit,Category FROM Items ORDER BY Category,Name');
    res.json(r.recordset.map(itemDto));
  }));
  app.get('/api/requisitions', h(async (req, res) => {
    const r = await pool.request().query(`SELECT Id,ItemName,Unit,Qty,ChefName,CreatedAt,Status FROM Requisitions
      WHERE Status='pending' OR Id IN (SELECT TOP 30 Id FROM Requisitions WHERE Status<>'pending' ORDER BY CreatedAt DESC, Id DESC)
      ORDER BY CreatedAt DESC, Id DESC`);
    res.json(r.recordset.map(x => ({ id: String(x.Id), name: x.ItemName, unit: x.Unit, qty: Number(x.Qty), chef: x.ChefName, at: x.CreatedAt, status: x.Status })));
  }));
  app.post('/api/requisitions', rateLimit({ windowMs: 60 * 1000, max: 20 }), h(async (req, res) => {
    const itemId = parseInt(req.body.itemId, 10), qty = Number(req.body.qty), chef = str(req.body.chef, 100);
    if (!Number.isInteger(itemId) || !(qty > 0) || qty > 999999 || !chef) return res.status(400).json({ error: 'ข้อมูลไม่ถูกต้อง' });
    const tx = new sql.Transaction(pool);
    await tx.begin();
    try {
      // หักสต็อกแบบ atomic: สำเร็จเฉพาะเมื่อคงเหลือพอ
      const u = await new sql.Request(tx).input('id', sql.Int, itemId).input('q', sql.Decimal(12, 3), qty)
        .query('UPDATE Items SET Qty=Qty-@q, UpdatedAt=SYSUTCDATETIME() OUTPUT inserted.Name, inserted.Unit WHERE Id=@id AND Qty>=@q');
      if (!u.recordset.length) { await tx.rollback(); return res.status(400).json({ error: 'จำนวนคงเหลือไม่พอ หรือไม่พบรายการ' }); }
      await new sql.Request(tx).input('id', sql.Int, itemId).input('n', sql.NVarChar, u.recordset[0].Name)
        .input('u', sql.NVarChar, u.recordset[0].Unit).input('q', sql.Decimal(12, 3), qty).input('c', sql.NVarChar, chef)
        .query('INSERT INTO Requisitions(ItemId,ItemName,Unit,Qty,ChefName) VALUES(@id,@n,@u,@q,@c)');
      await tx.commit();
      notifyBatch('new', chef, `• ${u.recordset[0].Name} ${qty} ${u.recordset[0].Unit}`);
      res.json({ ok: true });
    } catch (e) { await tx.rollback().catch(() => {}); throw e; }
  }));

  // ---- admin เท่านั้น: จัดการรายการวัตถุดิบ ----
  app.post('/api/items', requireAdmin, h(async (req, res) => {
    const it = parseItem(req.body); if (!it) return res.status(400).json({ error: 'ข้อมูลไม่ถูกต้อง' });
    await pool.request().input('n', sql.NVarChar, it.name).input('q', sql.Decimal(12, 3), it.qty)
      .input('u', sql.NVarChar, it.unit).input('c', sql.NVarChar, it.category)
      .query('INSERT INTO Items(Name,Qty,Unit,Category) VALUES(@n,@q,@u,@c)');
    res.json({ ok: true });
  }));
  app.put('/api/items/:id', requireAdmin, h(async (req, res) => {
    const id = parseInt(req.params.id, 10), it = parseItem(req.body);
    if (!Number.isInteger(id) || !it) return res.status(400).json({ error: 'ข้อมูลไม่ถูกต้อง' });
    const r = await pool.request().input('id', sql.Int, id).input('n', sql.NVarChar, it.name).input('q', sql.Decimal(12, 3), it.qty)
      .input('u', sql.NVarChar, it.unit).input('c', sql.NVarChar, it.category)
      .query('UPDATE Items SET Name=@n,Qty=@q,Unit=@u,Category=@c,UpdatedAt=SYSUTCDATETIME() WHERE Id=@id');
    r.rowsAffected[0] ? res.json({ ok: true }) : res.status(404).json({ error: 'ไม่พบรายการ' });
  }));
  app.delete('/api/items/:id', requireAdmin, h(async (req, res) => {
    const id = parseInt(req.params.id, 10);
    if (!Number.isInteger(id)) return res.status(400).json({ error: 'ข้อมูลไม่ถูกต้อง' });
    await pool.request().input('id', sql.Int, id).query('DELETE FROM Items WHERE Id=@id');
    res.json({ ok: true });
  }));

  // ---- admin: กด "เตรียมเสร็จ" -> แจ้งเตือน LINE ----
  app.post('/api/requisitions/:id/prepare', requireAdmin, h(async (req, res) => {
    const id = parseInt(req.params.id, 10);
    if (!Number.isInteger(id)) return res.status(400).json({ error: 'ข้อมูลไม่ถูกต้อง' });
    const r = await pool.request().input('id', sql.Int, id).query(
      "UPDATE Requisitions SET Status='prepared', PreparedAt=SYSUTCDATETIME() OUTPUT inserted.ItemName, inserted.Unit, inserted.Qty, inserted.ChefName WHERE Id=@id AND Status='pending'");
    const row = r.recordset[0];
    if (!row) return res.status(409).json({ error: 'รายการนี้เตรียมแล้วหรือไม่พบ' });
    notifyBatch('done', row.ChefName, `• ${row.ItemName} ${Number(row.Qty)} ${row.Unit}`);
    res.json({ ok: true });
  }));

  // ---- LINE webhook: ใช้ครั้งแรกเพื่อดู groupId (ตรวจลายเซ็นด้วย Channel secret) ----
  app.post('/line/webhook', (req, res) => {
    const secret = process.env.LINE_CHANNEL_SECRET, sig = req.get('x-line-signature') || '';
    if (!secret || !req.rawBody) return res.sendStatus(503);
    const mac = crypto.createHmac('sha256', secret).update(req.rawBody).digest('base64');
    if (sig.length !== mac.length || !crypto.timingSafeEqual(Buffer.from(sig), Buffer.from(mac))) return res.sendStatus(401);
    for (const ev of req.body.events || []) {
      if (ev.source && ev.source.type === 'group') void captureLineGroup(ev.source.groupId);
    }
    res.sendStatus(200);
  });

  const port = process.env.PORT || 3000;
  app.listen(port, () => console.log(`เปิดใช้งานที่ http://localhost:${port}`));
})().catch(e => { console.error('เริ่มระบบไม่สำเร็จ:', e.message); process.exit(1); });
