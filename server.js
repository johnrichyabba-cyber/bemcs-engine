const express = require("express");
const path = require("path");
const bodyParser = require("body-parser");
const { Pool } = require("pg");

const app = express();
const PORT = process.env.PORT || 10000;

// PostgreSQL connection
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    rejectUnauthorized: false,
  },
});

app.use(bodyParser.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, "public")));

// CREATE TABLE IF NOT EXISTS
async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY,
      username TEXT UNIQUE NOT NULL,
      password TEXT NOT NULL
    );
  `);

  // default user (only once)
  await pool.query(`
    INSERT INTO users (username, password)
    VALUES ('admin@bemcs.com', '123456')
    ON CONFLICT (username) DO NOTHING;
  `);
}

initDB();

// LOGIN ROUTE
app.post("/login", async (req, res) => {
  const { username, password } = req.body;

  try {
    const result = await pool.query(
      "SELECT * FROM users WHERE username=$1 AND password=$2",
      [username, password]
    );

    if (result.rows.length > 0) {
      res.redirect("/index.html");
    } else {
      res.send("Invalid username or password.");
    }
  } catch (err) {
    console.error(err);
    res.send("Server error");
  }
});

// START SERVER
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});