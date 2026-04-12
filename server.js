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

// Dashboard routes
app.get("/dashboard", (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "index.html"));
});

app.get("/index.html", (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "index.html"));
});

// Optional extra routes for your other pages
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

// Login API - returns JSON
app.post("/login", (req, res) => {
  const username = String(req.body.username || "").trim();
  const password = String(req.body.password || "").trim();

  if (username === "admin" && password === "1234") {
    return res.json({
      success: true,
      redirect: "/dashboard"
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