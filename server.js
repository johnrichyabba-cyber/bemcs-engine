const express = require("express");
const path = require("path");
const { Pool } = require("pg");

const app = express();
const PORT = process.env.PORT || 10000;

// Middleware
app.use(express.urlencoded({ extended: true }));
app.use(express.json());

// Static files
app.use(express.static(path.join(__dirname, "public")));

// PostgreSQL connection
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    rejectUnauthorized: false
  }
});

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
    INSERT INTO users (username, password, role)
    VALUES
      ('admin', '1234', 'admin'),
      ('operations', '1234', 'operations'),
      ('accounts', '1234', 'accounts'),
      ('customs', '1234', 'customs'),
      ('admin@bemcs.com', '123456', 'admin')
    ON CONFLICT (username) DO NOTHING;
  `);

  console.log("PostgreSQL initialized successfully.");
}

// Routes
app.get("/", (req, res) => {
  return res.redirect("/login");
});

app.get("/login", (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "login.html"));
});

app.get("/dashboard", (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "index.html"));
});

app.get("/index.html", (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "index.html"));
});

app.get("/about", (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "about.html"));
});

app.get("/tracking", (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "tracking.html"));
});

app.get("/shipment-registry", (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "shipment-registry.html"));
});

app.get("/registry-detail", (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "registry-detail.html"));
});

app.get("/accounting", (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "accounting.html"));
});

app.get("/system-health", (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "system-health.html"));
});

// PostgreSQL login
app.post("/login", async (req, res) => {
  const username = String(req.body.username || "").trim();
  const password = String(req.body.password || "").trim();

  try {
    const result = await pool.query(
      "SELECT id, username, role FROM users WHERE username = $1 AND password = $2 LIMIT 1",
      [username, password]
    );

    if (result.rows.length > 0) {
      return res.json({
        success: true,
        redirect: "/dashboard",
        user: result.rows[0]
      });
    }

    return res.status(401).json({
      success: false,
      error: "Invalid username or password."
    });
  } catch (error) {
    console.error("Login error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Login service unavailable."
    });
  }
});

app.get("/health", async (req, res) => {
  try {
    await pool.query("SELECT 1");
    return res.status(200).json({
      success: true,
      message: "Server and database are healthy."
    });
  } catch (error) {
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