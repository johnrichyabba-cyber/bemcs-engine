const express = require("express");
const path = require("path");

const app = express();
const PORT = process.env.PORT || 10000;

// Middleware
app.use(express.urlencoded({ extended: true }));
app.use(express.json());

// Serve static files
app.use(express.static(path.join(__dirname, "public")));

// Home
app.get("/", (req, res) => {
  return res.redirect("/login");
});

// Login page
app.get("/login", (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "login.html"));
});

// Login API - returns JSON
app.post("/login", (req, res) => {
  const username = String(req.body.username || "").trim();
  const password = String(req.body.password || "").trim();

  if (username === "admin" && password === "1234") {
    return res.json({
      success: true,
      redirect: "/index.html"
    });
  }

  return res.status(401).json({
    success: false,
    error: "Invalid username or password."
  });
});

// Health check
app.get("/health", (req, res) => {
  return res.status(200).send("OK");
});

// Start server
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});