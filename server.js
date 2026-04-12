const express = require("express");
const path = require("path");
const { Pool } = require("pg");

const app = express();
const PORT = process.env.PORT || 10000;

// PostgreSQL connection kupitia Render env var
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    rejectUnauthorized: false
  }
});

// Built-in Express middleware
app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

// Initialize database
async function initDB() {
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

  console.log("PostgreSQL initialized successfully ✅");
}

// Routes
app.get("/", (req, res) => {
  res.redirect("/login");
});

app.get("/login", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "login.html"));
});

app.post("/login", async (req, res) => {
  const { username, password } = req.body;

  try {
    const result = await pool.query(
      "SELECT * FROM users WHERE username = $1 AND password = $2",
      [username, password]
    );

    if (result.rows.length > 0) {
      return res.redirect("/index.html");
    } else {
      return res.status(401).send("Invalid username or password.");
    }
  } catch (err) {
    console.error("Login error ❌", err);
    return res.status(500).send("Server error");
  }
});

app.get("/health", async (req, res) => {
  try {
    await pool.query("SELECT 1");
    res.status(200).send("OK");
  } catch (err) {
    res.status(500).send("Database error");
  }
});

// Start server baada ya DB kuwa ready
async function startServer() {
  try {
    await initDB();

    app.listen(PORT, () => {
      console.log(`Server running on port ${PORT}`);
    });
  } catch (err) {
    console.error("Startup error ❌", err);
    process.exit(1);
  }
}

startServer();