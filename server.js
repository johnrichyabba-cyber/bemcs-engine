const express = require("express");
const path = require("path");

const app = express();
const PORT = process.env.PORT || 10000;

// Middleware
app.use(express.urlencoded({ extended: true }));
app.use(express.json());

// Static files
app.use(express.static(path.join(__dirname, "public")));

// Home
app.get("/", (req, res) => {
  return res.redirect("/login");
});

// Login page
app.get("/login", (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "login.html"));
});

// Test login (stable hardcoded login)
app.post("/login", (req, res) => {
  const username = String(req.body.username || "").trim();
  const password = String(req.body.password || "").trim();

  if (username === "admin" && password === "1234") {
    return res.redirect("/index.html");
  }

  return res.status(401).send("Invalid username or password.");
});

// Health check
app.get("/health", (req, res) => {
  return res.status(200).send("OK");
});

// Start server
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});