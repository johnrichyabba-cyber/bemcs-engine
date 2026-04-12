const express = require("express");
const path = require("path");
const { Pool } = require("pg");

const app = express();
const PORT = process.env.PORT || 10000;

// Middleware
app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

// PostgreSQL connection via Render DATABASE_URL
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    rejectUnauthorized: false
  }
});

// Initialize database and seed default users
async function initDB() {
  if (!process.env.DATABASE_URL) {
    throw new Error("DATABASE_URL is missing in environment variables.");
  }

  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY,
      username TEXT UNIQUE NOT NULL,
      password TEXT NOT NULL
    );
  `);

  await pool.query(`
    INSERT INTO users (username, password)
    VALUES
      ('admin@bemcs.com', '123456'),
      ('admin', '1234'),
      ('operations', '1234'),
      ('accounts', '1234'),
      ('customs', '1234')
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

app.post("/login", async (req, res) => {
  const username = String(req.body.username || "").trim();
  const password = String(req.body.password || "").trim();

  try {
    const result = await pool.query(
      "SELECT id, username FROM users WHERE username = $1 AND password = $2 LIMIT 1",
      [username, password]
    );

    if (result.rows.length > 0) {
      return res.redirect("/index.html");
    }

    return res.status(401).send("Invalid username or password.");
  } catch (error) {
    console.error("Login error:", error.message);
    return res.status(500).send("Server error");
  }
});

app.get("/health", async (req, res) => {
  try {
    await pool.query("SELECT 1");
    return res.status(200).send("OK");
  } catch (error) {
    console.error("Health check error:", error.message);
    return res.status(500).send("Database error");
  }
});

// Start server only after DB is ready
async function startServer() {
  try {
    await initDB();

    app.listen(PORT, () => {
      console.log(`Server running on port ${PORT}`);
    });
  } catch (error) {
    console.error("Startup error:", error.message);
    process.exit(1);
  }
}

startServer();