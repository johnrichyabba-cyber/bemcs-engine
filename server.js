const express = require("express");
const path = require("path");
const crypto = require("crypto");
const bcrypt = require("bcryptjs");
const { Pool } = require("pg");

const app = express();
const PORT = process.env.PORT || 10000;

// Middleware
app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

// PostgreSQL connection
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    rejectUnauthorized: false
  }
});

const SESSION_COOKIE = "bemcs_session";
const SESSION_MAX_AGE_SECONDS = 60 * 60 * 24;

function parseCookies(cookieHeader = "") {
  const cookies = {};
  cookieHeader.split(";").forEach((part) => {
    const [key, ...rest] = part.trim().split("=");
    if (!key) return;
    cookies[key] = decodeURIComponent(rest.join("="));
  });
  return cookies;
}

function generateSessionToken() {
  return crypto.randomBytes(32).toString("hex");
}

function looksHashed(password) {
  return typeof password === "string" && password.startsWith("$2");
}

function toMoney(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

async function writeAuditLog({
  actorUsername,
  actorRole = null,
  actionType,
  entityType,
  entityId = null,
  shipmentId = null,
  details = ""
}) {
  try {
    await pool.query(
      `
      INSERT INTO audit_logs (
        actor_username,
        actor_role,
        action_type,
        entity_type,
        entity_id,
        shipment_id,
        details
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      `,
      [
        actorUsername || "system",
        actorRole,
        actionType,
        entityType,
        entityId,
        shipmentId,
        details
      ]
    );
  } catch (error) {
    console.error("Audit log write error:", error.message);
  }
}

async function createPersistentSession(user) {
  const token = generateSessionToken();
  const expiresAt = new Date(Date.now() + SESSION_MAX_AGE_SECONDS * 1000);

  await pool.query(
    `
    INSERT INTO sessions (token, user_id, username, role, expires_at)
    VALUES ($1, $2, $3, $4, $5)
    `,
    [token, user.id, user.username, user.role, expiresAt]
  );

  return token;
}

async function getSession(req) {
  const cookies = parseCookies(req.headers.cookie || "");
  const token = cookies[SESSION_COOKIE];
  if (!token) return null;

  const result = await pool.query(
    `
    SELECT token, user_id, username, role, expires_at
    FROM sessions
    WHERE token = $1
    LIMIT 1
    `,
    [token]
  );

  if (!result.rows.length) return null;

  const session = result.rows[0];
  const now = new Date();

  if (new Date(session.expires_at) < now) {
    await pool.query("DELETE FROM sessions WHERE token = $1", [token]);
    return null;
  }

  return {
    token: session.token,
    id: session.user_id,
    username: session.username,
    role: session.role,
    expiresAt: session.expires_at
  };
}

async function destroySession(req) {
  const cookies = parseCookies(req.headers.cookie || "");
  const token = cookies[SESSION_COOKIE];
  if (!token) return null;

  const session = await getSession(req);
  await pool.query("DELETE FROM sessions WHERE token = $1", [token]);
  return session;
}

async function requireAuth(req, res, next) {
  try {
    const session = await getSession(req);
    if (!session) {
      return res.redirect("/login");
    }

    req.user = session;
    next();
  } catch (error) {
    console.error("Auth middleware error:", error.message);
    return res.redirect("/login");
  }
}

async function requireAdmin(req, res, next) {
  try {
    const session = await getSession(req);

    if (!session) {
      return res.status(401).json({
        success: false,
        error: "Unauthorized"
      });
    }

    if (session.role !== "admin") {
      return res.status(403).json({
        success: false,
        error: "Forbidden"
      });
    }

    req.user = session;
    next();
  } catch (error) {
    console.error("Admin middleware error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Authorization failed"
    });
  }
}

function allowRoles(...roles) {
  return async (req, res, next) => {
    try {
      const session = await getSession(req);

      if (!session) {
        return res.redirect("/login");
      }

      if (!roles.includes(session.role)) {
        return res.redirect("/403");
      }

      req.user = session;
      next();
    } catch (error) {
      console.error("Role middleware error:", error.message);
      return res.redirect("/login");
    }
  };
}

function apiAllowRoles(...roles) {
  return async (req, res, next) => {
    try {
      const session = await getSession(req);

      if (!session) {
        return res.status(401).json({
          success: false,
          error: "Unauthorized"
        });
      }

      if (!roles.includes(session.role)) {
        return res.status(403).json({
          success: false,
          error: "Forbidden"
        });
      }

      req.user = session;
      next();
    } catch (error) {
      console.error("API role middleware error:", error.message);
      return res.status(500).json({
        success: false,
        error: "Authorization failed"
      });
    }
  };
}

// Initialize database
async function initDb() {
  if (!process.env.DATABASE_URL) {
    throw new Error("DATABASE_URL is missing.");
  }

  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY,
      username TEXT UNIQUE NOT NULL,
      password TEXT NOT NULL,
      role TEXT NOT NULL DEFAULT 'admin',
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS sessions (
      id SERIAL PRIMARY KEY,
      token TEXT UNIQUE NOT NULL,
      user_id INTEGER NOT NULL,
      username TEXT NOT NULL,
      role TEXT NOT NULL,
      expires_at TIMESTAMP NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS shipments (
      id SERIAL PRIMARY KEY,
      client_name TEXT NOT NULL,
      reference_number TEXT UNIQUE NOT NULL,
      shipping_line TEXT NOT NULL,
      cargo_description TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'Pending',
      origin_port TEXT,
      destination_port TEXT,
      created_by TEXT NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS tracking_events (
      id SERIAL PRIMARY KEY,
      shipment_id INTEGER NOT NULL REFERENCES shipments(id) ON DELETE CASCADE,
      event_status TEXT NOT NULL,
      location_name TEXT NOT NULL,
      remarks TEXT,
      event_time TIMESTAMP NOT NULL,
      created_by TEXT NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS invoices (
      id SERIAL PRIMARY KEY,
      shipment_id INTEGER NOT NULL REFERENCES shipments(id) ON DELETE CASCADE,
      invoice_number TEXT UNIQUE NOT NULL,
      charge_type TEXT NOT NULL,
      amount NUMERIC(14,2) NOT NULL,
      currency TEXT NOT NULL DEFAULT 'TZS',
      notes TEXT,
      created_by TEXT NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS payments (
      id SERIAL PRIMARY KEY,
      invoice_id INTEGER NOT NULL REFERENCES invoices(id) ON DELETE CASCADE,
      amount NUMERIC(14,2) NOT NULL,
      payment_method TEXT NOT NULL,
      reference_text TEXT,
      notes TEXT,
      created_by TEXT NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS audit_logs (
      id SERIAL PRIMARY KEY,
      actor_username TEXT NOT NULL,
      actor_role TEXT,
      action_type TEXT NOT NULL,
      entity_type TEXT NOT NULL,
      entity_id INTEGER,
      shipment_id INTEGER,
      details TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);

  const seedUsers = [
    { username: "admin", password: "1234", role: "admin" },
    { username: "operations", password: "1234", role: "operations" },
    { username: "accounts", password: "1234", role: "accounts" },
    { username: "customs", password: "1234", role: "customs" },
    { username: "admin@bemcs.com", password: "123456", role: "admin" }
  ];

  for (const user of seedUsers) {
    const existing = await pool.query(
      "SELECT id, username, password FROM users WHERE username = $1 LIMIT 1",
      [user.username]
    );

    if (existing.rows.length === 0) {
      const hashedPassword = await bcrypt.hash(user.password, 10);
      await pool.query(
        "INSERT INTO users (username, password, role) VALUES ($1, $2, $3)",
        [user.username, hashedPassword, user.role]
      );
      continue;
    }

    const existingUser = existing.rows[0];
    if (!looksHashed(existingUser.password)) {
      const hashedPassword = await bcrypt.hash(existingUser.password, 10);
      await pool.query(
        "UPDATE users SET password = $1 WHERE id = $2",
        [hashedPassword, existingUser.id]
      );
    }
  }

  await pool.query(`
    DELETE FROM sessions
    WHERE expires_at < NOW()
  `);

  console.log("PostgreSQL initialized successfully.");
}

// Auth routes
app.get("/", async (req, res) => {
  try {
    const session = await getSession(req);
    if (session) return res.redirect("/dashboard");
    return res.redirect("/login");
  } catch (error) {
    return res.redirect("/login");
  }
});

app.get("/login", async (req, res) => {
  try {
    const session = await getSession(req);
    if (session) return res.redirect("/dashboard");
    return res.sendFile(path.join(__dirname, "public", "login.html"));
  } catch (error) {
    return res.sendFile(path.join(__dirname, "public", "login.html"));
  }
});

app.get("/403", requireAuth, (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "403.html"));
});

app.post("/login", async (req, res) => {
  const username = String(req.body.username || "").trim();
  const password = String(req.body.password || "").trim();

  try {
    const result = await pool.query(
      "SELECT id, username, password, role FROM users WHERE username = $1 LIMIT 1",
      [username]
    );

    if (result.rows.length === 0) {
      return res.status(401).json({
        success: false,
        error: "Invalid username or password."
      });
    }

    const user = result.rows[0];
    const passwordOk = await bcrypt.compare(password, user.password);

    if (!passwordOk) {
      return res.status(401).json({
        success: false,
        error: "Invalid username or password."
      });
    }

    const token = await createPersistentSession({
      id: user.id,
      username: user.username,
      role: user.role
    });

    res.setHeader(
      "Set-Cookie",
      `${SESSION_COOKIE}=${token}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${SESSION_MAX_AGE_SECONDS}`
    );

    await writeAuditLog({
      actorUsername: user.username,
      actorRole: user.role,
      actionType: "LOGIN",
      entityType: "SESSION",
      entityId: null,
      shipmentId: null,
      details: `User logged in successfully.`
    });

    return res.json({
      success: true,
      redirect: "/dashboard",
      user: {
        id: user.id,
        username: user.username,
        role: user.role
      }
    });
  } catch (error) {
    console.error("Login error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Login service unavailable."
    });
  }
});

app.post("/logout", async (req, res) => {
  try {
    const session = await destroySession(req);

    if (session) {
      await writeAuditLog({
        actorUsername: session.username,
        actorRole: session.role,
        actionType: "LOGOUT",
        entityType: "SESSION",
        details: `User logged out.`
      });
    }

    res.setHeader(
      "Set-Cookie",
      `${SESSION_COOKIE}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0`
    );

    return res.json({
      success: true,
      redirect: "/login"
    });
  } catch (error) {
    console.error("Logout error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Logout failed"
    });
  }
});

app.get("/api/me", requireAuth, (req, res) => {
  return res.json({
    success: true,
    user: req.user
  });
});

app.post("/api/users", requireAdmin, async (req, res) => {
  const username = String(req.body.username || "").trim();
  const password = String(req.body.password || "").trim();
  const role = String(req.body.role || "operations").trim();

  if (!username || !password) {
    return res.status(400).json({
      success: false,
      error: "Username and password are required."
    });
  }

  try {
    const hashedPassword = await bcrypt.hash(password, 10);

    const result = await pool.query(
      "INSERT INTO users (username, password, role) VALUES ($1, $2, $3) RETURNING id, username, role",
      [username, hashedPassword, role]
    );

    await writeAuditLog({
      actorUsername: req.user.username,
      actorRole: req.user.role,
      actionType: "CREATE_USER",
      entityType: "USER",
      entityId: result.rows[0].id,
      details: `Created user ${result.rows[0].username} with role ${result.rows[0].role}.`
    });

    return res.status(201).json({
      success: true,
      user: result.rows[0]
    });
  } catch (error) {
    if (error.code === "23505") {
      return res.status(409).json({
        success: false,
        error: "Username already exists."
      });
    }

    console.error("Create user error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Could not create user."
    });
  }
});

// Dashboard summary API
app.get("/api/dashboard-summary", requireAuth, async (req, res) => {
  try {
    const shipmentsResult = await pool.query(`
      SELECT
        COUNT(*)::int AS total_shipments,
        COUNT(*) FILTER (WHERE LOWER(status) NOT IN ('delivered', 'completed', 'closed'))::int AS active_shipments,
        COUNT(*) FILTER (WHERE LOWER(status) IN ('delivered', 'completed', 'closed'))::int AS delivered_shipments
      FROM shipments
    `);

    const invoicesResult = await pool.query(`
      SELECT
        COUNT(*)::int AS total_invoices,
        COALESCE(SUM(amount), 0) AS total_billed
      FROM invoices
    `);

    const paidResult = await pool.query(`
      SELECT
        COALESCE(SUM(amount), 0) AS total_paid
      FROM payments
    `);

    const trackingResult = await pool.query(`
      SELECT
        te.id,
        te.event_status,
        te.location_name,
        te.remarks,
        te.event_time,
        s.reference_number,
        s.client_name
      FROM tracking_events te
      JOIN shipments s ON s.id = te.shipment_id
      ORDER BY te.event_time DESC, te.created_at DESC
      LIMIT 5
    `);

    const recentInvoicesResult = await pool.query(`
      SELECT
        i.id,
        i.invoice_number,
        i.charge_type,
        i.amount,
        i.currency,
        s.reference_number,
        s.client_name,
        i.created_at
      FROM invoices i
      JOIN shipments s ON s.id = i.shipment_id
      ORDER BY i.created_at DESC
      LIMIT 5
    `);

    const recentPaymentsResult = await pool.query(`
      SELECT
        p.id,
        p.amount,
        p.payment_method,
        p.reference_text,
        p.created_at,
        i.invoice_number,
        s.reference_number,
        s.client_name
      FROM payments p
      JOIN invoices i ON i.id = p.invoice_id
      JOIN shipments s ON s.id = i.shipment_id
      ORDER BY p.created_at DESC
      LIMIT 5
    `);

    const shipments = shipmentsResult.rows[0] || {};
    const invoices = invoicesResult.rows[0] || {};
    const paid = paidResult.rows[0] || {};

    const totalBilled = toMoney(invoices.total_billed);
    const totalPaid = toMoney(paid.total_paid);
    const outstanding = totalBilled - totalPaid;

    return res.json({
      success: true,
      summary: {
        total_shipments: Number(shipments.total_shipments || 0),
        active_shipments: Number(shipments.active_shipments || 0),
        delivered_shipments: Number(shipments.delivered_shipments || 0),
        total_invoices: Number(invoices.total_invoices || 0),
        total_billed: totalBilled,
        total_paid: totalPaid,
        outstanding
      },
      recent_tracking: trackingResult.rows,
      recent_invoices: recentInvoicesResult.rows.map((row) => ({
        ...row,
        amount: toMoney(row.amount)
      })),
      recent_payments: recentPaymentsResult.rows.map((row) => ({
        ...row,
        amount: toMoney(row.amount)
      }))
    });
  } catch (error) {
    console.error("Dashboard summary error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Could not load dashboard summary."
    });
  }
});

// Audit log APIs
app.get("/api/audit-logs", apiAllowRoles("admin", "operations", "accounts", "customs"), async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        id,
        actor_username,
        actor_role,
        action_type,
        entity_type,
        entity_id,
        shipment_id,
        details,
        created_at
      FROM audit_logs
      ORDER BY created_at DESC
      LIMIT 100
    `);

    return res.json({
      success: true,
      logs: result.rows
    });
  } catch (error) {
    console.error("Audit logs error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Could not fetch audit logs."
    });
  }
});

app.get("/api/shipments/:id/audit-logs", apiAllowRoles("admin", "operations", "accounts", "customs"), async (req, res) => {
  const shipmentId = Number(req.params.id);

  try {
    const result = await pool.query(
      `
      SELECT
        id,
        actor_username,
        actor_role,
        action_type,
        entity_type,
        entity_id,
        shipment_id,
        details,
        created_at
      FROM audit_logs
      WHERE shipment_id = $1
      ORDER BY created_at DESC
      `,
      [shipmentId]
    );

    return res.json({
      success: true,
      logs: result.rows
    });
  } catch (error) {
    console.error("Shipment audit logs error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Could not fetch shipment audit logs."
    });
  }
});

// Shipment APIs
app.get("/api/shipments", apiAllowRoles("admin", "operations", "accounts", "customs"), async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        id,
        client_name,
        reference_number,
        shipping_line,
        cargo_description,
        status,
        origin_port,
        destination_port,
        created_by,
        created_at
      FROM shipments
      ORDER BY created_at DESC
    `);

    return res.json({
      success: true,
      shipments: result.rows
    });
  } catch (error) {
    console.error("List shipments error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Could not fetch shipments."
    });
  }
});

app.get("/api/shipments/:id", apiAllowRoles("admin", "operations", "accounts", "customs"), async (req, res) => {
  const id = Number(req.params.id);

  try {
    const shipmentResult = await pool.query(
      `
      SELECT
        id,
        client_name,
        reference_number,
        shipping_line,
        cargo_description,
        status,
        origin_port,
        destination_port,
        created_by,
        created_at
      FROM shipments
      WHERE id = $1
      LIMIT 1
      `,
      [id]
    );

    if (!shipmentResult.rows.length) {
      return res.status(404).json({
        success: false,
        error: "Shipment not found."
      });
    }

    const trackingResult = await pool.query(
      `
      SELECT
        id,
        shipment_id,
        event_status,
        location_name,
        remarks,
        event_time,
        created_by,
        created_at
      FROM tracking_events
      WHERE shipment_id = $1
      ORDER BY event_time DESC, created_at DESC
      `,
      [id]
    );

    return res.json({
      success: true,
      shipment: shipmentResult.rows[0],
      tracking_events: trackingResult.rows
    });
  } catch (error) {
    console.error("Get shipment error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Could not fetch shipment."
    });
  }
});

app.get("/api/shipments/:id/full-detail", apiAllowRoles("admin", "operations", "accounts", "customs"), async (req, res) => {
  const shipmentId = Number(req.params.id);

  try {
    const shipmentResult = await pool.query(
      `
      SELECT
        id,
        client_name,
        reference_number,
        shipping_line,
        cargo_description,
        status,
        origin_port,
        destination_port,
        created_by,
        created_at
      FROM shipments
      WHERE id = $1
      LIMIT 1
      `,
      [shipmentId]
    );

    if (!shipmentResult.rows.length) {
      return res.status(404).json({
        success: false,
        error: "Shipment not found."
      });
    }

    const trackingResult = await pool.query(
      `
      SELECT
        id,
        shipment_id,
        event_status,
        location_name,
        remarks,
        event_time,
        created_by,
        created_at
      FROM tracking_events
      WHERE shipment_id = $1
      ORDER BY event_time DESC, created_at DESC
      `,
      [shipmentId]
    );

    const invoicesResult = await pool.query(
      `
      SELECT
        i.id,
        i.shipment_id,
        i.invoice_number,
        i.charge_type,
        i.amount,
        i.currency,
        i.notes,
        i.created_by,
        i.created_at,
        COALESCE(SUM(p.amount), 0) AS paid_amount
      FROM invoices i
      LEFT JOIN payments p ON p.invoice_id = i.id
      WHERE i.shipment_id = $1
      GROUP BY
        i.id, i.shipment_id, i.invoice_number, i.charge_type,
        i.amount, i.currency, i.notes, i.created_by, i.created_at
      ORDER BY i.created_at DESC
      `,
      [shipmentId]
    );

    const invoiceIds = invoicesResult.rows.map((row) => row.id);
    let payments = [];

    if (invoiceIds.length) {
      const paymentsResult = await pool.query(
        `
        SELECT
          id,
          invoice_id,
          amount,
          payment_method,
          reference_text,
          notes,
          created_by,
          created_at
        FROM payments
        WHERE invoice_id = ANY($1::int[])
        ORDER BY created_at DESC
        `,
        [invoiceIds]
      );

      payments = paymentsResult.rows.map((p) => ({
        ...p,
        amount: toMoney(p.amount)
      }));
    }

    const invoices = invoicesResult.rows.map((row) => {
      const amount = toMoney(row.amount);
      const paid = toMoney(row.paid_amount);
      return {
        ...row,
        amount,
        paid_amount: paid,
        balance: amount - paid,
        payments: payments.filter((p) => p.invoice_id === row.id)
      };
    });

    const totalBilled = invoices.reduce((sum, item) => sum + toMoney(item.amount), 0);
    const totalPaid = invoices.reduce((sum, item) => sum + toMoney(item.paid_amount), 0);
    const outstanding = totalBilled - totalPaid;

    return res.json({
      success: true,
      shipment: shipmentResult.rows[0],
      tracking_events: trackingResult.rows,
      invoices,
      financial_summary: {
        total_billed: totalBilled,
        total_paid: totalPaid,
        outstanding
      }
    });
  } catch (error) {
    console.error("Full shipment detail error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Could not fetch integrated shipment detail."
    });
  }
});

app.post("/api/shipments", apiAllowRoles("admin", "operations"), async (req, res) => {
  const clientName = String(req.body.client_name || "").trim();
  const referenceNumber = String(req.body.reference_number || "").trim();
  const shippingLine = String(req.body.shipping_line || "").trim();
  const cargoDescription = String(req.body.cargo_description || "").trim();
  const status = String(req.body.status || "Pending").trim();
  const originPort = String(req.body.origin_port || "").trim();
  const destinationPort = String(req.body.destination_port || "").trim();

  if (!clientName || !referenceNumber || !shippingLine || !cargoDescription) {
    return res.status(400).json({
      success: false,
      error: "Client name, reference number, shipping line, and cargo description are required."
    });
  }

  try {
    const result = await pool.query(
      `
      INSERT INTO shipments (
        client_name,
        reference_number,
        shipping_line,
        cargo_description,
        status,
        origin_port,
        destination_port,
        created_by
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      RETURNING
        id,
        client_name,
        reference_number,
        shipping_line,
        cargo_description,
        status,
        origin_port,
        destination_port,
        created_by,
        created_at
      `,
      [
        clientName,
        referenceNumber,
        shippingLine,
        cargoDescription,
        status,
        originPort,
        destinationPort,
        req.user.username
      ]
    );

    await writeAuditLog({
      actorUsername: req.user.username,
      actorRole: req.user.role,
      actionType: "CREATE_SHIPMENT",
      entityType: "SHIPMENT",
      entityId: result.rows[0].id,
      shipmentId: result.rows[0].id,
      details: `Created shipment ${result.rows[0].reference_number} for client ${result.rows[0].client_name}.`
    });

    return res.status(201).json({
      success: true,
      shipment: result.rows[0]
    });
  } catch (error) {
    if (error.code === "23505") {
      return res.status(409).json({
        success: false,
        error: "Reference number already exists."
      });
    }

    console.error("Create shipment error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Could not create shipment."
    });
  }
});

// Tracking APIs
app.get("/api/tracking-events", apiAllowRoles("admin", "operations", "accounts", "customs"), async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        te.id,
        te.shipment_id,
        te.event_status,
        te.location_name,
        te.remarks,
        te.event_time,
        te.created_by,
        te.created_at,
        s.reference_number,
        s.client_name
      FROM tracking_events te
      JOIN shipments s ON s.id = te.shipment_id
      ORDER BY te.event_time DESC, te.created_at DESC
      LIMIT 50
    `);

    return res.json({
      success: true,
      tracking_events: result.rows
    });
  } catch (error) {
    console.error("List tracking events error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Could not fetch tracking events."
    });
  }
});

app.get("/api/shipments/:id/tracking-events", apiAllowRoles("admin", "operations", "accounts", "customs"), async (req, res) => {
  const shipmentId = Number(req.params.id);

  try {
    const result = await pool.query(
      `
      SELECT
        id,
        shipment_id,
        event_status,
        location_name,
        remarks,
        event_time,
        created_by,
        created_at
      FROM tracking_events
      WHERE shipment_id = $1
      ORDER BY event_time DESC, created_at DESC
      `,
      [shipmentId]
    );

    return res.json({
      success: true,
      tracking_events: result.rows
    });
  } catch (error) {
    console.error("Shipment tracking events error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Could not fetch shipment tracking events."
    });
  }
});

app.post("/api/tracking-events", apiAllowRoles("admin", "operations", "customs"), async (req, res) => {
  const shipmentId = Number(req.body.shipment_id);
  const eventStatus = String(req.body.event_status || "").trim();
  const locationName = String(req.body.location_name || "").trim();
  const remarks = String(req.body.remarks || "").trim();
  const eventTime = String(req.body.event_time || "").trim();

  if (!shipmentId || !eventStatus || !locationName || !eventTime) {
    return res.status(400).json({
      success: false,
      error: "Shipment, event status, location name, and event time are required."
    });
  }

  try {
    const shipmentCheck = await pool.query(
      "SELECT id, reference_number FROM shipments WHERE id = $1 LIMIT 1",
      [shipmentId]
    );

    if (!shipmentCheck.rows.length) {
      return res.status(404).json({
        success: false,
        error: "Shipment not found."
      });
    }

    const result = await pool.query(
      `
      INSERT INTO tracking_events (
        shipment_id,
        event_status,
        location_name,
        remarks,
        event_time,
        created_by
      )
      VALUES ($1, $2, $3, $4, $5, $6)
      RETURNING
        id,
        shipment_id,
        event_status,
        location_name,
        remarks,
        event_time,
        created_by,
        created_at
      `,
      [shipmentId, eventStatus, locationName, remarks, eventTime, req.user.username]
    );

    await pool.query(
      `
      UPDATE shipments
      SET status = $1
      WHERE id = $2
      `,
      [eventStatus, shipmentId]
    );

    await writeAuditLog({
      actorUsername: req.user.username,
      actorRole: req.user.role,
      actionType: "ADD_TRACKING_EVENT",
      entityType: "TRACKING_EVENT",
      entityId: result.rows[0].id,
      shipmentId,
      details: `Added tracking event "${eventStatus}" at "${locationName}" for shipment ${shipmentCheck.rows[0].reference_number}.`
    });

    return res.status(201).json({
      success: true,
      tracking_event: result.rows[0]
    });
  } catch (error) {
    console.error("Create tracking event error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Could not create tracking event."
    });
  }
});

// Accounting APIs
app.get("/api/invoices", apiAllowRoles("admin", "accounts", "operations"), async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        i.id,
        i.shipment_id,
        i.invoice_number,
        i.charge_type,
        i.amount,
        i.currency,
        i.notes,
        i.created_by,
        i.created_at,
        s.reference_number,
        s.client_name,
        COALESCE(SUM(p.amount), 0) AS paid_amount
      FROM invoices i
      JOIN shipments s ON s.id = i.shipment_id
      LEFT JOIN payments p ON p.invoice_id = i.id
      GROUP BY
        i.id, i.shipment_id, i.invoice_number, i.charge_type, i.amount,
        i.currency, i.notes, i.created_by, i.created_at, s.reference_number, s.client_name
      ORDER BY i.created_at DESC
    `);

    const invoices = result.rows.map((row) => {
      const amount = toMoney(row.amount);
      const paid = toMoney(row.paid_amount);
      return {
        ...row,
        amount,
        paid_amount: paid,
        balance: amount - paid
      };
    });

    return res.json({
      success: true,
      invoices
    });
  } catch (error) {
    console.error("List invoices error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Could not fetch invoices."
    });
  }
});

app.get("/api/invoices/:id", apiAllowRoles("admin", "accounts", "operations"), async (req, res) => {
  const invoiceId = Number(req.params.id);

  try {
    const invoiceResult = await pool.query(
      `
      SELECT
        i.id,
        i.shipment_id,
        i.invoice_number,
        i.charge_type,
        i.amount,
        i.currency,
        i.notes,
        i.created_by,
        i.created_at,
        s.reference_number,
        s.client_name
      FROM invoices i
      JOIN shipments s ON s.id = i.shipment_id
      WHERE i.id = $1
      LIMIT 1
      `,
      [invoiceId]
    );

    if (!invoiceResult.rows.length) {
      return res.status(404).json({
        success: false,
        error: "Invoice not found."
      });
    }

    const paymentsResult = await pool.query(
      `
      SELECT
        id,
        invoice_id,
        amount,
        payment_method,
        reference_text,
        notes,
        created_by,
        created_at
      FROM payments
      WHERE invoice_id = $1
      ORDER BY created_at DESC
      `,
      [invoiceId]
    );

    const invoice = invoiceResult.rows[0];
    const payments = paymentsResult.rows.map((p) => ({
      ...p,
      amount: toMoney(p.amount)
    }));

    const paid = payments.reduce((sum, p) => sum + toMoney(p.amount), 0);
    const amount = toMoney(invoice.amount);

    return res.json({
      success: true,
      invoice: {
        ...invoice,
        amount,
        paid_amount: paid,
        balance: amount - paid
      },
      payments
    });
  } catch (error) {
    console.error("Get invoice error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Could not fetch invoice."
    });
  }
});

app.post("/api/invoices", apiAllowRoles("admin", "accounts", "operations"), async (req, res) => {
  const shipmentId = Number(req.body.shipment_id);
  const invoiceNumber = String(req.body.invoice_number || "").trim();
  const chargeType = String(req.body.charge_type || "").trim();
  const amount = toMoney(req.body.amount);
  const currency = String(req.body.currency || "TZS").trim();
  const notes = String(req.body.notes || "").trim();

  if (!shipmentId || !invoiceNumber || !chargeType || !amount) {
    return res.status(400).json({
      success: false,
      error: "Shipment, invoice number, charge type, and amount are required."
    });
  }

  try {
    const shipmentCheck = await pool.query(
      "SELECT id, reference_number FROM shipments WHERE id = $1 LIMIT 1",
      [shipmentId]
    );

    if (!shipmentCheck.rows.length) {
      return res.status(404).json({
        success: false,
        error: "Shipment not found."
      });
    }

    const result = await pool.query(
      `
      INSERT INTO invoices (
        shipment_id,
        invoice_number,
        charge_type,
        amount,
        currency,
        notes,
        created_by
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      RETURNING
        id,
        shipment_id,
        invoice_number,
        charge_type,
        amount,
        currency,
        notes,
        created_by,
        created_at
      `,
      [shipmentId, invoiceNumber, chargeType, amount, currency, notes, req.user.username]
    );

    await writeAuditLog({
      actorUsername: req.user.username,
      actorRole: req.user.role,
      actionType: "CREATE_INVOICE",
      entityType: "INVOICE",
      entityId: result.rows[0].id,
      shipmentId,
      details: `Created invoice ${invoiceNumber} for shipment ${shipmentCheck.rows[0].reference_number}.`
    });

    return res.status(201).json({
      success: true,
      invoice: {
        ...result.rows[0],
        amount: toMoney(result.rows[0].amount)
      }
    });
  } catch (error) {
    if (error.code === "23505") {
      return res.status(409).json({
        success: false,
        error: "Invoice number already exists."
      });
    }

    console.error("Create invoice error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Could not create invoice."
    });
  }
});

app.post("/api/payments", apiAllowRoles("admin", "accounts"), async (req, res) => {
  const invoiceId = Number(req.body.invoice_id);
  const amount = toMoney(req.body.amount);
  const paymentMethod = String(req.body.payment_method || "").trim();
  const referenceText = String(req.body.reference_text || "").trim();
  const notes = String(req.body.notes || "").trim();

  if (!invoiceId || !amount || !paymentMethod) {
    return res.status(400).json({
      success: false,
      error: "Invoice, amount, and payment method are required."
    });
  }

  try {
    const invoiceCheck = await pool.query(
      `
      SELECT i.id, i.invoice_number, i.shipment_id
      FROM invoices i
      WHERE i.id = $1
      LIMIT 1
      `,
      [invoiceId]
    );

    if (!invoiceCheck.rows.length) {
      return res.status(404).json({
        success: false,
        error: "Invoice not found."
      });
    }

    const result = await pool.query(
      `
      INSERT INTO payments (
        invoice_id,
        amount,
        payment_method,
        reference_text,
        notes,
        created_by
      )
      VALUES ($1, $2, $3, $4, $5, $6)
      RETURNING
        id,
        invoice_id,
        amount,
        payment_method,
        reference_text,
        notes,
        created_by,
        created_at
      `,
      [invoiceId, amount, paymentMethod, referenceText, notes, req.user.username]
    );

    await writeAuditLog({
      actorUsername: req.user.username,
      actorRole: req.user.role,
      actionType: "ADD_PAYMENT",
      entityType: "PAYMENT",
      entityId: result.rows[0].id,
      shipmentId: invoiceCheck.rows[0].shipment_id,
      details: `Added payment to invoice ${invoiceCheck.rows[0].invoice_number} via ${paymentMethod}.`
    });

    return res.status(201).json({
      success: true,
      payment: {
        ...result.rows[0],
        amount: toMoney(result.rows[0].amount)
      }
    });
  } catch (error) {
    console.error("Create payment error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Could not create payment."
    });
  }
});

// Protected pages
app.get("/dashboard", requireAuth, (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "dashboard.html"));
});

app.get("/index.html", requireAuth, (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "dashboard.html"));
});

app.get("/about", allowRoles("admin"), (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "about.html"));
});

app.get("/tracking", allowRoles("admin", "operations", "customs", "accounts"), (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "tracking.html"));
});

app.get("/shipment-registry", allowRoles("admin", "operations", "accounts", "customs"), (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "shipment-registry.html"));
});

app.get("/registry-detail", allowRoles("admin", "operations", "accounts", "customs"), (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "registry-detail.html"));
});

app.get("/accounting", allowRoles("admin", "accounts", "operations"), (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "accounting.html"));
});

app.get("/system-health", allowRoles("admin", "customs", "operations", "accounts"), (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "system-health.html"));
});

app.get("/health", async (req, res) => {
  try {
    await pool.query("SELECT 1");
    return res.status(200).json({
      success: true,
      message: "Server and database are healthy."
    });
  } catch (error) {
    console.error("Health check error:", error.message);
    return res.status(500).json({
      success: false,
      message: "Database health check failed."
    });
  }
});

// Start server
async function startServer() {
  try {
    await initDb();

    app.listen(PORT, () => {
      console.log(`Server running on port ${PORT}`);
    });
  } catch (error) {
    console.error("Startup error:", error.message);
    process.exit(1);
  }
}

startServer();