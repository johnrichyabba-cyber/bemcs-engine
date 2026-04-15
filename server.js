require("dotenv").config();

const express = require("express");
const path = require("path");
const crypto = require("crypto");
const bcrypt = require("bcryptjs");
const { Pool } = require("pg");

const { attachCustomsLiveRoutes } = require("./providers/customs-live");
const { attachMultiSourceTrackingRoutes } = require("./providers/multi-source-tracking");

const app = express();
const PORT = process.env.PORT || 10000;

app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
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

  if (new Date(session.expires_at) < new Date()) {
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
    if (!session) return res.redirect("/login");
    req.user = session;
    next();
  } catch (error) {
    console.error("Auth middleware error:", error.message);
    return res.redirect("/login");
  }
}

function allowRoles(...roles) {
  return async (req, res, next) => {
    try {
      const session = await getSession(req);
      if (!session) return res.redirect("/login");
      if (!roles.includes(session.role)) return res.redirect("/403");
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
        return res.status(401).json({ success: false, error: "Unauthorized" });
      }
      if (!roles.includes(session.role)) {
        return res.status(403).json({ success: false, error: "Forbidden" });
      }
      req.user = session;
      next();
    } catch (error) {
      console.error("API role middleware error:", error.message);
      return res.status(500).json({ success: false, error: "Authorization failed" });
    }
  };
}

async function ensureColumn(tableName, columnName, definition) {
  await pool.query(`
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = '${tableName}' AND column_name = '${columnName}'
      ) THEN
        EXECUTE 'ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${definition}';
      END IF;
    END $$;
  `);
}

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

  await ensureColumn("shipments", "tracking_provider", "TEXT");
  await ensureColumn("shipments", "carrier_code", "TEXT");
  await ensureColumn("shipments", "bill_of_lading", "TEXT");
  await ensureColumn("shipments", "container_number", "TEXT");
  await ensureColumn("shipments", "booking_number", "TEXT");
  await ensureColumn("shipments", "port_of_loading", "TEXT");
  await ensureColumn("shipments", "port_of_discharge", "TEXT");
  await ensureColumn("shipments", "final_destination", "TEXT");
  await ensureColumn("shipments", "current_tracking_status", "TEXT");
  await ensureColumn("shipments", "current_tracking_time", "TIMESTAMP");

  await pool.query(`
    CREATE TABLE IF NOT EXISTS tracking_events (
      id SERIAL PRIMARY KEY,
      shipment_id INTEGER NOT NULL REFERENCES shipments(id) ON DELETE CASCADE,
      event_status TEXT NOT NULL,
      location_name TEXT,
      remarks TEXT,
      event_time TIMESTAMP NOT NULL,
      source_name TEXT DEFAULT 'manual',
      created_by TEXT NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS tracking_sources (
      id SERIAL PRIMARY KEY,
      shipment_id INTEGER NOT NULL REFERENCES shipments(id) ON DELETE CASCADE,
      provider_name TEXT NOT NULL,
      carrier_code TEXT,
      bill_of_lading TEXT,
      container_number TEXT,
      booking_number TEXT,
      tracking_mode TEXT DEFAULT 'polling',
      source_label TEXT,
      external_reference_id TEXT,
      provider_status TEXT,
      last_synced_at TIMESTAMP,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS sync_logs (
      id SERIAL PRIMARY KEY,
      shipment_id INTEGER NOT NULL REFERENCES shipments(id) ON DELETE CASCADE,
      provider_name TEXT NOT NULL,
      sync_status TEXT NOT NULL,
      message TEXT,
      synced_by TEXT NOT NULL DEFAULT 'system',
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

  await pool.query(`
    CREATE TABLE IF NOT EXISTS shipment_documents (
      id SERIAL PRIMARY KEY,
      shipment_id INTEGER NOT NULL REFERENCES shipments(id) ON DELETE CASCADE,
      document_name TEXT NOT NULL,
      document_category TEXT NOT NULL,
      document_url TEXT NOT NULL,
      notes TEXT,
      uploaded_by TEXT NOT NULL,
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

  await pool.query("DELETE FROM sessions WHERE expires_at < NOW()");
  console.log("PostgreSQL initialized successfully.");
}

app.get("/", async (req, res) => {
  try {
    const session = await getSession(req);
    return res.redirect(session ? "/dashboard" : "/login");
  } catch {
    return res.redirect("/login");
  }
});

app.get("/login", async (req, res) => {
  try {
    const session = await getSession(req);
    if (session) return res.redirect("/dashboard");
    return res.sendFile(path.join(__dirname, "public", "login.html"));
  } catch {
    return res.sendFile(path.join(__dirname, "public", "login.html"));
  }
});

app.get("/403", requireAuth, (_req, res) => {
  res.sendFile(path.join(__dirname, "public", "403.html"));
});

app.post("/login", async (req, res) => {
  const username = String(req.body.username || "").trim();
  const password = String(req.body.password || "").trim();

  try {
    const result = await pool.query(
      "SELECT id, username, password, role FROM users WHERE username = $1 LIMIT 1",
      [username]
    );

    if (!result.rows.length) {
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
      details: "User logged in successfully."
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
        details: "User logged out."
      });
    }

    res.setHeader(
      "Set-Cookie",
      `${SESSION_COOKIE}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0`
    );

    return res.json({ success: true, redirect: "/login" });
  } catch (error) {
    console.error("Logout error:", error.message);
    return res.status(500).json({ success: false, error: "Logout failed" });
  }
});

app.get("/api/me", requireAuth, (req, res) => {
  res.json({ success: true, user: req.user });
});

app.post("/api/change-password", requireAuth, async (req, res) => {
  const currentPassword = String(req.body.current_password || "").trim();
  const newPassword = String(req.body.new_password || "").trim();

  if (!currentPassword || !newPassword) {
    return res.status(400).json({
      success: false,
      error: "Current password and new password are required."
    });
  }

  if (newPassword.length < 4) {
    return res.status(400).json({
      success: false,
      error: "New password must have at least 4 characters."
    });
  }

  try {
    const userResult = await pool.query(
      "SELECT id, username, password, role FROM users WHERE id = $1 LIMIT 1",
      [req.user.id]
    );

    if (!userResult.rows.length) {
      return res.status(404).json({
        success: false,
        error: "User not found."
      });
    }

    const user = userResult.rows[0];
    const passwordOk = await bcrypt.compare(currentPassword, user.password);

    if (!passwordOk) {
      return res.status(400).json({
        success: false,
        error: "Current password is incorrect."
      });
    }

    const newHashedPassword = await bcrypt.hash(newPassword, 10);

    await pool.query(
      "UPDATE users SET password = $1 WHERE id = $2",
      [newHashedPassword, user.id]
    );

    await writeAuditLog({
      actorUsername: user.username,
      actorRole: user.role,
      actionType: "CHANGE_PASSWORD",
      entityType: "USER",
      entityId: user.id,
      details: "User changed own password."
    });

    return res.json({
      success: true,
      message: "Password changed successfully."
    });
  } catch (error) {
    console.error("Change password error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Could not change password."
    });
  }
});

app.get("/api/dashboard-summary", requireAuth, async (_req, res) => {
  try {
    const shipmentsResult = await pool.query(`
      SELECT
        COUNT(*)::int AS total_shipments,
        COUNT(*) FILTER (WHERE LOWER(COALESCE(status, '')) NOT IN ('delivered', 'completed', 'closed'))::int AS active_shipments,
        COUNT(*) FILTER (WHERE LOWER(COALESCE(status, '')) IN ('delivered', 'completed', 'closed'))::int AS delivered_shipments
      FROM shipments
    `);

    const invoicesResult = await pool.query(`
      SELECT COUNT(*)::int AS total_invoices, COALESCE(SUM(amount), 0) AS total_billed
      FROM invoices
    `);

    const paidResult = await pool.query(`
      SELECT COALESCE(SUM(amount), 0) AS total_paid
      FROM payments
    `);

    const trackingResult = await pool.query(`
      SELECT te.id, te.shipment_id, te.event_status, te.location_name, te.remarks, te.event_time, te.source_name, s.reference_number, s.client_name
      FROM tracking_events te
      JOIN shipments s ON s.id = te.shipment_id
      ORDER BY te.event_time DESC, te.created_at DESC
      LIMIT 5
    `);

    const recentInvoicesResult = await pool.query(`
      SELECT i.id, i.invoice_number, i.charge_type, i.amount, i.currency, s.reference_number, s.client_name, i.created_at
      FROM invoices i
      JOIN shipments s ON s.id = i.shipment_id
      ORDER BY i.created_at DESC
      LIMIT 5
    `);

    const recentPaymentsResult = await pool.query(`
      SELECT p.id, p.amount, p.payment_method, p.reference_text, p.created_at, i.invoice_number, s.reference_number, s.client_name
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

    return res.json({
      success: true,
      summary: {
        total_shipments: Number(shipments.total_shipments || 0),
        active_shipments: Number(shipments.active_shipments || 0),
        delivered_shipments: Number(shipments.delivered_shipments || 0),
        total_invoices: Number(invoices.total_invoices || 0),
        total_billed: totalBilled,
        total_paid: totalPaid,
        outstanding: totalBilled - totalPaid
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

app.get("/api/shipments", apiAllowRoles("admin", "operations", "accounts", "customs"), async (_req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        id, client_name, reference_number, shipping_line, cargo_description, status,
        origin_port, destination_port, tracking_provider, carrier_code, bill_of_lading,
        container_number, booking_number, port_of_loading, port_of_discharge,
        final_destination, current_tracking_status, current_tracking_time, created_by, created_at
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

app.post("/api/shipments", apiAllowRoles("admin", "operations"), async (req, res) => {
  const payload = {
    client_name: String(req.body.client_name || "").trim(),
    reference_number: String(req.body.reference_number || "").trim(),
    shipping_line: String(req.body.shipping_line || "").trim(),
    cargo_description: String(req.body.cargo_description || "").trim(),
    status: String(req.body.status || "Pending").trim(),
    origin_port: String(req.body.origin_port || "").trim(),
    destination_port: String(req.body.destination_port || "").trim(),
    tracking_provider: String(req.body.tracking_provider || "").trim(),
    carrier_code: String(req.body.carrier_code || "").trim(),
    bill_of_lading: String(req.body.bill_of_lading || "").trim(),
    container_number: String(req.body.container_number || "").trim(),
    booking_number: String(req.body.booking_number || "").trim(),
    port_of_loading: String(req.body.port_of_loading || "").trim(),
    port_of_discharge: String(req.body.port_of_discharge || "").trim(),
    final_destination: String(req.body.final_destination || "").trim()
  };

  if (!payload.client_name || !payload.reference_number || !payload.shipping_line || !payload.cargo_description) {
    return res.status(400).json({
      success: false,
      error: "Client name, reference number, shipping line, and cargo description are required."
    });
  }

  try {
    const result = await pool.query(
      `
      INSERT INTO shipments (
        client_name, reference_number, shipping_line, cargo_description, status,
        origin_port, destination_port, tracking_provider, carrier_code, bill_of_lading,
        container_number, booking_number, port_of_loading, port_of_discharge,
        final_destination, created_by
      )
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
      RETURNING *
      `,
      [
        payload.client_name,
        payload.reference_number,
        payload.shipping_line,
        payload.cargo_description,
        payload.status,
        payload.origin_port || null,
        payload.destination_port || null,
        payload.tracking_provider || null,
        payload.carrier_code || null,
        payload.bill_of_lading || null,
        payload.container_number || null,
        payload.booking_number || null,
        payload.port_of_loading || null,
        payload.port_of_discharge || null,
        payload.final_destination || null,
        req.user.username
      ]
    );

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

app.post("/api/shipments/:id/tracking-config", apiAllowRoles("admin", "operations", "customs"), async (req, res) => {
  const shipmentId = Number(req.params.id);
  const providerName = String(req.body.provider_name || "vizion").trim();
  const carrierCode = String(req.body.carrier_code || "").trim();
  const billOfLading = String(req.body.bill_of_lading || "").trim();
  const containerNumber = String(req.body.container_number || "").trim();
  const bookingNumber = String(req.body.booking_number || "").trim();
  const trackingMode = String(req.body.tracking_mode || "polling").trim();
  const sourceLabel = String(req.body.source_label || "Primary Tracking Source").trim();

  if (!shipmentId) {
    return res.status(400).json({
      success: false,
      error: "Valid shipment ID is required."
    });
  }

  try {
    const shipmentCheck = await pool.query(
      "SELECT id FROM shipments WHERE id = $1 LIMIT 1",
      [shipmentId]
    );

    if (!shipmentCheck.rows.length) {
      return res.status(404).json({
        success: false,
        error: "Shipment not found."
      });
    }

    await pool.query(
      `
      UPDATE shipments
      SET tracking_provider = $2,
          carrier_code = $3,
          bill_of_lading = COALESCE(NULLIF($4, ''), bill_of_lading),
          container_number = COALESCE(NULLIF($5, ''), container_number),
          booking_number = COALESCE(NULLIF($6, ''), booking_number)
      WHERE id = $1
      `,
      [shipmentId, providerName || null, carrierCode || null, billOfLading, containerNumber, bookingNumber]
    );

    const existing = await pool.query(
      "SELECT id FROM tracking_sources WHERE shipment_id = $1 LIMIT 1",
      [shipmentId]
    );

    const trackingSourceResult = existing.rows.length
      ? await pool.query(
          `
          UPDATE tracking_sources
          SET provider_name = $2,
              carrier_code = $3,
              bill_of_lading = $4,
              container_number = $5,
              booking_number = $6,
              tracking_mode = $7,
              source_label = $8
          WHERE shipment_id = $1
          RETURNING *
          `,
          [
            shipmentId,
            providerName || null,
            carrierCode || null,
            billOfLading || null,
            containerNumber || null,
            bookingNumber || null,
            trackingMode,
            sourceLabel
          ]
        )
      : await pool.query(
          `
          INSERT INTO tracking_sources (
            shipment_id, provider_name, carrier_code, bill_of_lading, container_number,
            booking_number, tracking_mode, source_label
          )
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
          RETURNING *
          `,
          [
            shipmentId,
            providerName || null,
            carrierCode || null,
            billOfLading || null,
            containerNumber || null,
            bookingNumber || null,
            trackingMode,
            sourceLabel
          ]
        );

    return res.json({
      success: true,
      tracking_source: trackingSourceResult.rows[0]
    });
  } catch (error) {
    console.error("Tracking config error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Could not save tracking configuration."
    });
  }
});

app.get("/api/shipments/:id/tracking-source", apiAllowRoles("admin", "operations", "accounts", "customs"), async (req, res) => {
  const shipmentId = Number(req.params.id);

  try {
    const result = await pool.query(
      "SELECT * FROM tracking_sources WHERE shipment_id = $1 ORDER BY id DESC LIMIT 1",
      [shipmentId]
    );

    return res.json({
      success: true,
      tracking_source: result.rows[0] || null
    });
  } catch (error) {
    console.error("Tracking source error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Could not fetch tracking source."
    });
  }
});

app.get("/api/shipments/:id/sync-logs", apiAllowRoles("admin", "operations", "accounts", "customs"), async (req, res) => {
  const shipmentId = Number(req.params.id);

  try {
    const result = await pool.query(
      "SELECT * FROM sync_logs WHERE shipment_id = $1 ORDER BY created_at DESC, id DESC LIMIT 50",
      [shipmentId]
    );

    return res.json({
      success: true,
      sync_logs: result.rows
    });
  } catch (error) {
    console.error("Sync logs error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Could not fetch sync logs."
    });
  }
});

app.get("/api/shipments/:id/live-status", apiAllowRoles("admin", "operations", "accounts", "customs"), async (req, res) => {
  const shipmentId = Number(req.params.id);

  try {
    const shipmentResult = await pool.query(
      "SELECT * FROM shipments WHERE id = $1 LIMIT 1",
      [shipmentId]
    );

    if (!shipmentResult.rows.length) {
      return res.status(404).json({
        success: false,
        error: "Shipment not found."
      });
    }

    const [trackingSourceResult, latestSyncResult, latestEventResult] = await Promise.all([
      pool.query("SELECT * FROM tracking_sources WHERE shipment_id = $1 ORDER BY id DESC LIMIT 1", [shipmentId]),
      pool.query("SELECT * FROM sync_logs WHERE shipment_id = $1 ORDER BY created_at DESC, id DESC LIMIT 1", [shipmentId]),
      pool.query("SELECT * FROM tracking_events WHERE shipment_id = $1 ORDER BY event_time DESC NULLS LAST, id DESC LIMIT 1", [shipmentId])
    ]);

    const shipment = shipmentResult.rows[0];

    return res.json({
      success: true,
      live_status: {
        shipment_id: shipment.id,
        reference_number: shipment.reference_number,
        current_status: shipment.current_tracking_status || shipment.status || null,
        current_status_time: shipment.current_tracking_time || null,
        tracking_enabled: Boolean(
          shipment.tracking_provider ||
          shipment.bill_of_lading ||
          shipment.container_number ||
          shipment.booking_number
        ),
        tracking_source: trackingSourceResult.rows[0] || null,
        latest_sync: latestSyncResult.rows[0] || null,
        latest_event: latestEventResult.rows[0] || null
      }
    });
  } catch (error) {
    console.error("Live status error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Could not fetch live status."
    });
  }
});

app.get("/api/shipments/:id/full-detail", apiAllowRoles("admin", "operations", "accounts", "customs"), async (req, res) => {
  const shipmentId = Number(req.params.id);

  try {
    const shipmentResult = await pool.query(
      "SELECT * FROM shipments WHERE id = $1 LIMIT 1",
      [shipmentId]
    );

    if (!shipmentResult.rows.length) {
      return res.status(404).json({
        success: false,
        error: "Shipment not found."
      });
    }

    const trackingResult = await pool.query(
      "SELECT * FROM tracking_events WHERE shipment_id = $1 ORDER BY event_time DESC, created_at DESC",
      [shipmentId]
    );

    const invoicesResult = await pool.query(`
      SELECT
        i.id, i.shipment_id, i.invoice_number, i.charge_type, i.amount, i.currency, i.notes, i.created_by, i.created_at,
        COALESCE(SUM(p.amount), 0) AS paid_amount
      FROM invoices i
      LEFT JOIN payments p ON p.invoice_id = i.id
      WHERE i.shipment_id = $1
      GROUP BY i.id, i.shipment_id, i.invoice_number, i.charge_type, i.amount, i.currency, i.notes, i.created_by, i.created_at
      ORDER BY i.created_at DESC
    `, [shipmentId]);

    const documentsResult = await pool.query(
      "SELECT * FROM shipment_documents WHERE shipment_id = $1 ORDER BY created_at DESC",
      [shipmentId]
    );

    const trackingSourceResult = await pool.query(
      "SELECT * FROM tracking_sources WHERE shipment_id = $1 ORDER BY id DESC LIMIT 1",
      [shipmentId]
    );

    const syncLogsResult = await pool.query(
      "SELECT * FROM sync_logs WHERE shipment_id = $1 ORDER BY created_at DESC, id DESC LIMIT 20",
      [shipmentId]
    );

    const invoiceIds = invoicesResult.rows.map((row) => row.id);
    let payments = [];

    if (invoiceIds.length) {
      const paymentsResult = await pool.query(
        `
        SELECT id, invoice_id, amount, payment_method, reference_text, notes, created_by, created_at
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
    const shipment = shipmentResult.rows[0];
    const latestEvent = trackingResult.rows[0] || null;

    return res.json({
      success: true,
      shipment,
      tracking_events: trackingResult.rows,
      invoices,
      documents: documentsResult.rows,
      tracking_source: trackingSourceResult.rows[0] || null,
      sync_logs: syncLogsResult.rows || [],
      live_status: {
        shipment_id: shipment.id,
        reference_number: shipment.reference_number,
        current_status: shipment.current_tracking_status || shipment.status || null,
        current_status_time: shipment.current_tracking_time || null,
        tracking_enabled: Boolean(
          shipment.tracking_provider ||
          shipment.bill_of_lading ||
          shipment.container_number ||
          shipment.booking_number
        ),
        tracking_source: trackingSourceResult.rows[0] || null,
        latest_sync: syncLogsResult.rows[0] || null,
        latest_event: latestEvent
      },
      financial_summary: {
        total_billed: totalBilled,
        total_paid: totalPaid,
        outstanding: totalBilled - totalPaid
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

app.get("/api/tracking-events", apiAllowRoles("admin", "operations", "accounts", "customs"), async (req, res) => {
  const shipmentId = Number(req.query.shipment_id);

  try {
    const result = shipmentId
      ? await pool.query(
          `
          SELECT te.*, s.reference_number, s.client_name
          FROM tracking_events te
          JOIN shipments s ON s.id = te.shipment_id
          WHERE te.shipment_id = $1
          ORDER BY te.event_time DESC, te.created_at DESC
          `,
          [shipmentId]
        )
      : await pool.query(
          `
          SELECT te.*, s.reference_number, s.client_name
          FROM tracking_events te
          JOIN shipments s ON s.id = te.shipment_id
          ORDER BY te.event_time DESC, te.created_at DESC
          LIMIT 50
          `
        );

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

app.post("/api/tracking-events", apiAllowRoles("admin", "operations", "customs"), async (req, res) => {
  const shipmentId = Number(req.body.shipment_id);
  const eventStatus = String(req.body.event_status || "").trim();
  const locationName = String(req.body.location_name || "").trim();
  const remarks = String(req.body.remarks || "").trim();
  const eventTime = String(req.body.event_time || "").trim();
  const sourceName = String(req.body.source_name || "manual").trim();

  if (!shipmentId || !eventStatus || !locationName || !eventTime) {
    return res.status(400).json({
      success: false,
      error: "Shipment, event status, location name, and event time are required."
    });
  }

  try {
    const shipmentCheck = await pool.query(
      "SELECT id FROM shipments WHERE id = $1 LIMIT 1",
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
        shipment_id, event_status, location_name, remarks, event_time, source_name, created_by
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      RETURNING *
      `,
      [shipmentId, eventStatus, locationName, remarks, eventTime, sourceName, req.user.username]
    );

    await pool.query(
      `
      UPDATE shipments
      SET status = $1, current_tracking_status = $1, current_tracking_time = $2
      WHERE id = $3
      `,
      [eventStatus, eventTime, shipmentId]
    );

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

app.get("/api/invoices", apiAllowRoles("admin", "accounts", "operations"), async (_req, res) => {
  try {
    const invoicesResult = await pool.query(`
      SELECT
        i.id, i.shipment_id, i.invoice_number, i.charge_type, i.amount, i.currency, i.notes, i.created_by, i.created_at,
        s.reference_number, s.client_name,
        COALESCE(SUM(p.amount), 0) AS paid_amount
      FROM invoices i
      JOIN shipments s ON s.id = i.shipment_id
      LEFT JOIN payments p ON p.invoice_id = i.id
      GROUP BY i.id, i.shipment_id, i.invoice_number, i.charge_type, i.amount, i.currency, i.notes, i.created_by, i.created_at, s.reference_number, s.client_name
      ORDER BY i.created_at DESC
    `);

    const invoiceIds = invoicesResult.rows.map((row) => row.id);
    let payments = [];

    if (invoiceIds.length) {
      const paymentsResult = await pool.query(
        `
        SELECT
          id, invoice_id, amount, payment_method, reference_text, notes, created_by, created_at
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

app.get("/api/invoices/:id/print-data", apiAllowRoles("admin", "accounts", "operations"), async (req, res) => {
  const invoiceId = Number(req.params.id);

  try {
    const invoiceResult = await pool.query(
      `
      SELECT
        i.id, i.shipment_id, i.invoice_number, i.charge_type, i.amount, i.currency, i.notes, i.created_by, i.created_at,
        s.reference_number, s.client_name, s.shipping_line, s.cargo_description
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
      SELECT id, invoice_id, amount, payment_method, reference_text, notes, created_by, created_at
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

    return res.json({
      success: true,
      invoice: {
        ...invoice,
        amount: toMoney(invoice.amount),
        paid_amount: paid,
        balance: toMoney(invoice.amount) - paid,
        payments
      }
    });
  } catch (error) {
    console.error("Invoice print data error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Could not load invoice print data."
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
      "SELECT id FROM shipments WHERE id = $1 LIMIT 1",
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
        shipment_id, invoice_number, charge_type, amount, currency, notes, created_by
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      RETURNING *
      `,
      [shipmentId, invoiceNumber, chargeType, amount, currency, notes, req.user.username]
    );

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

app.get("/api/payments/:id/receipt-data", apiAllowRoles("admin", "accounts", "operations"), async (req, res) => {
  const paymentId = Number(req.params.id);

  try {
    const result = await pool.query(
      `
      SELECT
        p.id, p.invoice_id, p.amount, p.payment_method, p.reference_text, p.notes, p.created_by, p.created_at,
        i.invoice_number, i.currency, i.shipment_id,
        s.reference_number, s.client_name, s.shipping_line
      FROM payments p
      JOIN invoices i ON i.id = p.invoice_id
      JOIN shipments s ON s.id = i.shipment_id
      WHERE p.id = $1
      LIMIT 1
      `,
      [paymentId]
    );

    if (!result.rows.length) {
      return res.status(404).json({
        success: false,
        error: "Payment not found."
      });
    }

    return res.json({
      success: true,
      payment: {
        ...result.rows[0],
        amount: toMoney(result.rows[0].amount)
      }
    });
  } catch (error) {
    console.error("Receipt print data error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Could not load receipt print data."
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
      "SELECT i.id FROM invoices i WHERE i.id = $1 LIMIT 1",
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
        invoice_id, amount, payment_method, reference_text, notes, created_by
      )
      VALUES ($1, $2, $3, $4, $5, $6)
      RETURNING *
      `,
      [invoiceId, amount, paymentMethod, referenceText, notes, req.user.username]
    );

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

app.get("/api/shipments/:id/documents", apiAllowRoles("admin", "operations", "accounts", "customs"), async (req, res) => {
  const shipmentId = Number(req.params.id);

  try {
    const result = await pool.query(
      "SELECT * FROM shipment_documents WHERE shipment_id = $1 ORDER BY created_at DESC",
      [shipmentId]
    );

    return res.json({
      success: true,
      documents: result.rows
    });
  } catch (error) {
    console.error("List shipment documents error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Could not fetch shipment documents."
    });
  }
});

app.post("/api/documents", apiAllowRoles("admin", "operations", "accounts", "customs"), async (req, res) => {
  const shipmentId = Number(req.body.shipment_id);
  const documentName = String(req.body.document_name || "").trim();
  const documentCategory = String(req.body.document_category || "").trim();
  const documentUrl = String(req.body.document_url || "").trim();
  const notes = String(req.body.notes || "").trim();

  if (!shipmentId || !documentName || !documentCategory || !documentUrl) {
    return res.status(400).json({
      success: false,
      error: "Shipment, document name, category, and document URL are required."
    });
  }

  try {
    const shipmentCheck = await pool.query(
      "SELECT id FROM shipments WHERE id = $1 LIMIT 1",
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
      INSERT INTO shipment_documents (
        shipment_id, document_name, document_category, document_url, notes, uploaded_by
      )
      VALUES ($1, $2, $3, $4, $5, $6)
      RETURNING *
      `,
      [shipmentId, documentName, documentCategory, documentUrl, notes, req.user.username]
    );

    return res.status(201).json({
      success: true,
      document: result.rows[0]
    });
  } catch (error) {
    console.error("Create document error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Could not save document."
    });
  }
});

app.delete("/api/documents/:id", apiAllowRoles("admin", "operations"), async (req, res) => {
  const documentId = Number(req.params.id);

  try {
    const existing = await pool.query(
      "SELECT id FROM shipment_documents WHERE id = $1 LIMIT 1",
      [documentId]
    );

    if (!existing.rows.length) {
      return res.status(404).json({
        success: false,
        error: "Document not found."
      });
    }

    await pool.query("DELETE FROM shipment_documents WHERE id = $1", [documentId]);

    return res.json({
      success: true,
      deleted_id: documentId
    });
  } catch (error) {
    console.error("Delete document error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Could not delete document."
    });
  }
});

app.get("/dashboard", requireAuth, (_req, res) => {
  res.sendFile(path.join(__dirname, "public", "dashboard.html"));
});

app.get("/index.html", requireAuth, (_req, res) => {
  res.sendFile(path.join(__dirname, "public", "dashboard.html"));
});

app.get("/tracking", allowRoles("admin", "operations", "customs", "accounts"), (_req, res) => {
  res.sendFile(path.join(__dirname, "public", "tracking.html"));
});

app.get("/shipment-registry", allowRoles("admin", "operations", "accounts", "customs"), (_req, res) => {
  res.sendFile(path.join(__dirname, "public", "shipment-registry.html"));
});

app.get("/registry-detail", allowRoles("admin", "operations", "accounts", "customs"), (_req, res) => {
  res.sendFile(path.join(__dirname, "public", "registry-detail.html"));
});

app.get("/accounting", allowRoles("admin", "accounts", "operations"), (_req, res) => {
  res.sendFile(path.join(__dirname, "public", "accounting.html"));
});

app.get("/system-health", allowRoles("admin", "customs", "operations", "accounts"), (_req, res) => {
  res.sendFile(path.join(__dirname, "public", "system-health.html"));
});

app.get("/health", async (_req, res) => {
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

attachCustomsLiveRoutes({ app, pool, requireAuth });
attachMultiSourceTrackingRoutes({ app, requireAuth });

app.post("/api/shipments/:id/sync-tracking", requireAuth, async (req, res) => {
  req.url = `/api/shipments/${req.params.id}/live-refresh`;
  return app._router.handle(req, res, () => {});
});

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