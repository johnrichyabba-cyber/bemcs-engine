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

// Simple in-memory sessions
const sessions = new Map();
const SESSION_COOKIE = "bemcs_session";

function parseCookies(cookieHeader = "") {
  const cookies = {};
  cookieHeader.split(";").forEach((part) => {
    const [key, ...rest] = part.trim().split("=");
    if (!key) return;
    cookies[key] = decodeURIComponent(rest.join("="));
  });
  return cookies;
}

function createSession(user) {
  const token = crypto.randomBytes(32).toString("hex");
  sessions.set(token, {
    id: user.id,
    username: user.username,
    role: user.role,
    createdAt: Date.now()
  });
  return token;
}

function getSession(req) {
  const cookies = parseCookies(req.headers.cookie || "");
  const token = cookies[SESSION_COOKIE];
  if (!token) return null;
  return sessions.get(token) || null;
}

function requireAuth(req, res, next) {
  const session = getSession(req);
  if (!session) {
    return res.redirect("/login");
  }
  req.user = session;
  next();
}

function requireAdmin(req, res, next) {
  const session = getSession(req);
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
}

function allowRoles(...roles) {
  return (req, res, next) => {
    const session = getSession(req);

    if (!session) {
      return res.redirect("/login");
    }

    if (!roles.includes(session.role)) {
      return res.redirect("/403");
    }

    req.user = session;
    next();
  };
}

function looksHashed(password) {
  return typeof password === "string" && password.startsWith("$2");
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

  console.log("PostgreSQL initialized successfully.");
}

// Routes
app.get("/", (req, res) => {
  const session = getSession(req);
  if (session) {
    return res.redirect("/dashboard");
  }
  return res.redirect("/login");
});

app.get("/login", (req, res) => {
  const session = getSession(req);
  if (session) {
    return res.redirect("/dashboard");
  }
  return res.sendFile(path.join(__dirname, "public", "login.html"));
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

    const token = createSession({
      id: user.id,
      username: user.username,
      role: user.role
    });

    res.setHeader(
      "Set-Cookie",
      `${SESSION_COOKIE}=${token}; Path=/; HttpOnly; SameSite=Lax; Max-Age=86400`
    );

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

app.post("/logout", (req, res) => {
  const cookies = parseCookies(req.headers.cookie || "");
  const token = cookies[SESSION_COOKIE];

  if (token) {
    sessions.delete(token);
  }

  res.setHeader(
    "Set-Cookie",
    `${SESSION_COOKIE}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0`
  );

  return res.json({
    success: true,
    redirect: "/login"
  });
});

app.get("/api/me", requireAuth, (req, res) => {
  return res.json({
    success: true,
    user: req.user
  });
});

// Admin-only user creation
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

// Protected dashboards/pages by role
app.get("/dashboard", requireAuth, (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "dashboard.html"));
});

app.get("/index.html", requireAuth, (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "dashboard.html"));
});

app.get("/about", allowRoles("admin"), (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "about.html"));
});

app.get("/tracking", allowRoles("admin", "operations", "customs"), (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "tracking.html"));
});

app.get("/shipment-registry", allowRoles("admin", "operations"), (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "shipment-registry.html"));
});

app.get("/registry-detail", allowRoles("admin", "operations"), (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "registry-detail.html"));
});

app.get("/accounting", allowRoles("admin", "accounts"), (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "accounting.html"));
});

app.get("/system-health", allowRoles("admin", "customs"), (req, res) => {
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