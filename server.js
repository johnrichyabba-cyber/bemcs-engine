const express = require("express");
const path = require("path");
const fs = require("fs");
const crypto = require("crypto");
const cookieParser = require("cookie-parser");

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, "public")));

const dbPath = path.join(__dirname, "data", "db.json");

const sessions = new Map();

const USERS = [
  { username: "admin", password: "1234", role: "admin" },
  { username: "operations", password: "1234", role: "operations" },
  { username: "accounts", password: "1234", role: "accounts" },
  { username: "customs", password: "1234", role: "customs" }
];

function readDb() {
  try {
    if (!fs.existsSync(dbPath)) {
      return {
        shipments: [],
        trackingRecords: [],
        marketBoard: [],
        accounting: {
          invoices: [],
          payments: [],
          summary: {
            totalInvoices: 0,
            paidAmount: 0,
            pendingAmount: 0,
            todayCollections: 0
          }
        },
        referenceRegistry: []
      };
    }

    const raw = fs.readFileSync(dbPath, "utf8");
    return JSON.parse(raw);
  } catch (error) {
    console.error("Error reading database:", error.message);
    return {
      shipments: [],
      trackingRecords: [],
      marketBoard: [],
      accounting: {
        invoices: [],
        payments: [],
        summary: {
          totalInvoices: 0,
          paidAmount: 0,
          pendingAmount: 0,
          todayCollections: 0
        }
      },
      referenceRegistry: []
    };
  }
}

function writeDb(data) {
  fs.writeFileSync(dbPath, JSON.stringify(data, null, 2), "utf8");
}

function toNumber(value) {
  const cleaned = String(value || "")
    .replace(/[^0-9.-]/g, "")
    .trim();
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed : 0;
}

function formatTzs(value) {
  return `TZS ${value.toLocaleString()}`;
}

function nowIso() {
  return new Date().toISOString();
}

function textIncludes(source, query) {
  return String(source || "").toLowerCase().includes(String(query || "").toLowerCase());
}

function pushAuditLog(shipment, action, detail, actor = "admin") {
  shipment.auditLog = shipment.auditLog || [];
  shipment.auditLog.unshift({
    id: Date.now() + Math.floor(Math.random() * 1000),
    action,
    detail,
    actor,
    timestamp: nowIso()
  });
}

function rebuildAccountingSummary(db) {
  const invoices = db.accounting?.invoices || [];
  const paid = invoices
    .filter((item) => item.status === "Paid")
    .reduce((sum, item) => sum + toNumber(item.amount), 0);

  const pending = invoices
    .filter((item) => item.status !== "Paid")
    .reduce((sum, item) => sum + toNumber(item.amount), 0);

  const total = invoices.length;
  const todayCollections = invoices
    .filter((item) => item.status === "Paid")
    .slice(0, 3)
    .reduce((sum, item) => sum + toNumber(item.amount), 0);

  db.accounting.summary = {
    totalInvoices: String(total),
    paidAmount: formatTzs(paid),
    pendingAmount: formatTzs(pending),
    todayCollections: formatTzs(todayCollections)
  };
}

function buildAnalytics(shipments, trackingRecords) {
  return {
    totalShipments: shipments.length,
    syncedShipments: shipments.filter((s) => s.registryStatus === "Registered").length,
    pendingStatus: trackingRecords.filter((s) => s.currentStatus === "Pending").length,
    trackedQuery: trackingRecords.length
  };
}

function buildSystemHealth(db) {
  const shipments = db.shipments || [];
  const references = db.referenceRegistry || [];
  const trackingRecords = db.trackingRecords || [];
  const accounting = db.accounting || {};

  return {
    apiStatus: "Active",
    customsFeed: "Connected",
    databaseWrite: "Healthy",
    shipmentRegistryCount: shipments.length,
    referenceRegistryCount: references.length,
    trackingRecordCount: trackingRecords.length,
    lastRefresh: new Date().toLocaleString(),
    accountingStatus: accounting ? "Available" : "Unavailable"
  };
}

function buildAlertSummary(db) {
  const shipments = db.shipments || [];
  const invoices = db.accounting?.invoices || [];

  const pendingShipments = shipments.filter((s) => s.currentStatus === "Pending").length;
  const onHoldShipments = shipments.filter((s) => s.currentStatus === "On Hold").length;
  const unpaidInvoices = invoices.filter((i) => i.status === "Unpaid").length;
  const unlinkedAccounting = shipments.filter((s) => !s.accountingLink).length;
  const missingDocuments = shipments.filter((s) => !s.attachments || s.attachments.length === 0).length;

  return {
    pendingShipments,
    onHoldShipments,
    unpaidInvoices,
    unlinkedAccounting,
    missingDocuments,
    totalOpenAlerts:
      pendingShipments +
      onHoldShipments +
      unpaidInvoices +
      unlinkedAccounting +
      missingDocuments
  };
}

function buildNotifications(db) {
  const notifications = [];
  const shipments = db.shipments || [];
  const invoices = db.accounting?.invoices || [];

  for (const shipment of shipments) {
    if (shipment.currentStatus === "On Hold") {
      notifications.push({
        id: `hold-${shipment.id}`,
        level: "critical",
        title: "Shipment on hold",
        message: `${shipment.referenceNumber} is currently on hold.`,
        link: `/registry-detail?id=${shipment.id}`
      });
    }

    if (shipment.currentStatus === "Pending") {
      notifications.push({
        id: `pending-${shipment.id}`,
        level: "warning",
        title: "Pending shipment status",
        message: `${shipment.referenceNumber} is still pending operational clearance.`,
        link: `/registry-detail?id=${shipment.id}`
      });
    }

    if (!shipment.accountingLink) {
      notifications.push({
        id: `accounting-${shipment.id}`,
        level: "warning",
        title: "Missing accounting linkage",
        message: `${shipment.referenceNumber} has no linked invoice yet.`,
        link: `/registry-detail?id=${shipment.id}`
      });
    }

    if (!shipment.attachments || shipment.attachments.length === 0) {
      notifications.push({
        id: `docs-${shipment.id}`,
        level: "info",
        title: "Missing shipment documents",
        message: `${shipment.referenceNumber} does not have attached operational documents.`,
        link: `/registry-detail?id=${shipment.id}`
      });
    }
  }

  for (const invoice of invoices) {
    if (invoice.status === "Unpaid") {
      notifications.push({
        id: `invoice-${invoice.invoiceNo}`,
        level: "critical",
        title: "Unpaid invoice detected",
        message: `${invoice.invoiceNo} for ${invoice.clientName} is still unpaid.`,
        link: `/registry-detail?id=${invoice.shipmentId}`
      });
    }

    if (invoice.status === "Partially Paid") {
      notifications.push({
        id: `partial-${invoice.invoiceNo}`,
        level: "warning",
        title: "Partial payment status",
        message: `${invoice.invoiceNo} is partially paid and needs follow-up.`,
        link: `/registry-detail?id=${invoice.shipmentId}`
      });
    }
  }

  const order = { critical: 0, warning: 1, info: 2 };
  return notifications.sort((a, b) => order[a.level] - order[b.level]).slice(0, 12);
}

function buildManagementSummary(db) {
  const shipments = db.shipments || [];
  const trackingRecords = db.trackingRecords || [];
  const invoices = db.accounting?.invoices || [];
  const alerts = buildAlertSummary(db);

  return {
    totalShipments: shipments.length,
    registeredShipments: shipments.filter((s) => s.registryStatus === "Registered").length,
    trackedRecords: trackingRecords.length,
    approvedRecords: trackingRecords.filter((t) => t.approvalStatus === "Approved").length,
    unpaidInvoices: invoices.filter((i) => i.status === "Unpaid").length,
    paidInvoices: invoices.filter((i) => i.status === "Paid").length,
    missingDocuments: alerts.missingDocuments,
    accountingGaps: alerts.unlinkedAccounting,
    onHoldShipments: alerts.onHoldShipments,
    openAlerts: alerts.totalOpenAlerts
  };
}

function buildRoleWidgets(role, db) {
  const summary = buildManagementSummary(db);

  if (role === "accounts") {
    return [
      {
        title: "Unpaid Invoices",
        value: summary.unpaidInvoices,
        description: "Invoices requiring collection follow-up."
      },
      {
        title: "Paid Invoices",
        value: summary.paidInvoices,
        description: "Settled invoices already closed."
      },
      {
        title: "Accounting Gaps",
        value: summary.accountingGaps,
        description: "Shipments without invoice linkage."
      },
      {
        title: "Open Alerts",
        value: summary.openAlerts,
        description: "Accounting-related and cross-module alerts."
      }
    ];
  }

  if (role === "operations") {
    return [
      {
        title: "Total Shipments",
        value: summary.totalShipments,
        description: "All registry shipment files."
      },
      {
        title: "On Hold Shipments",
        value: summary.onHoldShipments,
        description: "Shipments needing operational action."
      },
      {
        title: "Missing Documents",
        value: summary.missingDocuments,
        description: "Files without attached documents."
      },
      {
        title: "Open Alerts",
        value: summary.openAlerts,
        description: "Current operational exceptions."
      }
    ];
  }

  if (role === "customs") {
    return [
      {
        title: "Tracked Records",
        value: summary.trackedRecords,
        description: "Tracking intelligence currently captured."
      },
      {
        title: "Approved Records",
        value: summary.approvedRecords,
        description: "Tracking records approved for next stage."
      },
      {
        title: "On Hold Shipments",
        value: summary.onHoldShipments,
        description: "Clearance-sensitive shipments."
      },
      {
        title: "Pending Alerts",
        value: summary.openAlerts,
        description: "Customs and control notifications."
      }
    ];
  }

  return [
    {
      title: "Total Shipments",
      value: summary.totalShipments,
      description: "All shipment files in registry."
    },
    {
      title: "Registered Shipments",
      value: summary.registeredShipments,
      description: "Approved files already transferred."
    },
    {
      title: "Unpaid Invoices",
      value: summary.unpaidInvoices,
      description: "Invoices still awaiting payment."
    },
    {
      title: "Open Alerts",
      value: summary.openAlerts,
      description: "All current dashboard alerts."
    }
  ];
}

function createSession(user) {
  const sessionId = crypto.randomBytes(24).toString("hex");
  sessions.set(sessionId, {
    username: user.username,
    role: user.role,
    createdAt: Date.now()
  });
  return sessionId;
}

function getSessionUser(req) {
  const sessionId = req.cookies.marine_sid;
  if (!sessionId) return null;
  return sessions.get(sessionId) || null;
}

function requireAuth(req, res, next) {
  const user = getSessionUser(req);

  if (!user) {
    if (req.path.startsWith("/api/")) {
      return res.status(401).json({ error: "Unauthorized" });
    }
    return res.redirect("/login");
  }

  req.user = user;
  next();
}

function requirePageAuth(pageFile) {
  return (req, res) => {
    const user = getSessionUser(req);
    if (!user) return res.redirect("/login");
    return res.sendFile(path.join(__dirname, "public", pageFile));
  };
}

function buildShipmentTimeline(shipment, trackingRecord, invoice) {
  const timeline = [];

  if (trackingRecord) {
    timeline.push({
      key: "tracked",
      title: "Fetched from Tracking",
      status: "done",
      date: trackingRecord.createdAt || "",
      description: `Shipment intelligence captured from ${trackingRecord.trackingSource}.`
    });

    timeline.push({
      key: "approved",
      title: "Approval Review",
      status: trackingRecord.approvalStatus === "Approved" ? "done" : "pending",
      date: trackingRecord.createdAt || "",
      description: `Approval status is ${trackingRecord.approvalStatus}.`
    });
  }

  timeline.push({
    key: "registry",
    title: "Transferred to Registry",
    status: shipment.registryStatus === "Registered" ? "done" : "pending",
    date: shipment.transferredAt || "",
    description: "Shipment file created in registry after approval."
  });

  if (invoice) {
    timeline.push({
      key: "invoice",
      title: "Invoice Linked",
      status: "done",
      date: invoice.dueDate || shipment.transferredAt || "",
      description: `Invoice ${invoice.invoiceNo} linked for ${invoice.service}.`
    });

    timeline.push({
      key: "payment",
      title: "Payment State",
      status: invoice.status === "Paid" ? "done" : invoice.status === "Partially Paid" ? "active" : "pending",
      date: invoice.dueDate || "",
      description: `Current payment status is ${invoice.status}.`
    });
  } else {
    timeline.push({
      key: "invoice",
      title: "Invoice Linked",
      status: "pending",
      date: "",
      description: "No accounting linkage has been created yet."
    });
  }

  if (shipment.attachments && shipment.attachments.length) {
    timeline.push({
      key: "attachments",
      title: "Documents Attached",
      status: "done",
      date: shipment.attachments[0].uploadedAt || "",
      description: `${shipment.attachments.length} document(s) attached to shipment file.`
    });
  }

  return timeline;
}

function filterTrackingRecords(records, query) {
  let results = [...records];
  const q = String(query.q || "").trim().toLowerCase();
  const status = String(query.status || "").trim();
  const approvalStatus = String(query.approvalStatus || "").trim();
  const shippingLine = String(query.shippingLine || "").trim();

  if (q) {
    results = results.filter((item) =>
      textIncludes(item.referenceNumber, q) ||
      textIncludes(item.shippingLine, q) ||
      textIncludes(item.cargoDescription, q) ||
      textIncludes(item.trackingSource, q)
    );
  }

  if (status) {
    results = results.filter((item) => String(item.currentStatus || "") === status);
  }

  if (approvalStatus) {
    results = results.filter((item) => String(item.approvalStatus || "") === approvalStatus);
  }

  if (shippingLine) {
    results = results.filter((item) => String(item.shippingLine || "") === shippingLine);
  }

  return results;
}

function filterShipments(shipments, query) {
  let results = [...shipments];
  const q = String(query.q || "").trim().toLowerCase();
  const status = String(query.status || "").trim();
  const approvalStatus = String(query.approvalStatus || "").trim();
  const shippingLine = String(query.shippingLine || "").trim();

  if (q) {
    results = results.filter((item) =>
      textIncludes(item.referenceNumber, q) ||
      textIncludes(item.clientName, q) ||
      textIncludes(item.shippingLine, q) ||
      textIncludes(item.cargoDescription, q)
    );
  }

  if (status) {
    results = results.filter((item) => String(item.currentStatus || "") === status);
  }

  if (approvalStatus) {
    results = results.filter((item) => String(item.approvalStatus || "") === approvalStatus);
  }

  if (shippingLine) {
    results = results.filter((item) => String(item.shippingLine || "") === shippingLine);
  }

  return results;
}

function filterInvoices(invoices, query) {
  let results = [...invoices];
  const q = String(query.q || "").trim().toLowerCase();
  const status = String(query.status || "").trim();
  const service = String(query.service || "").trim();

  if (q) {
    results = results.filter((item) =>
      textIncludes(item.invoiceNo, q) ||
      textIncludes(item.clientName, q) ||
      textIncludes(item.referenceNumber, q) ||
      textIncludes(item.service, q)
    );
  }

  if (status) {
    results = results.filter((item) => String(item.status || "") === status);
  }

  if (service) {
    results = results.filter((item) => String(item.service || "") === service);
  }

  return results;
}

function buildGlobalSearchResults(db, q) {
  const term = String(q || "").trim().toLowerCase();
  if (!term) return [];

  const results = [];

  for (const item of db.trackingRecords || []) {
    if (
      textIncludes(item.referenceNumber, term) ||
      textIncludes(item.shippingLine, term) ||
      textIncludes(item.cargoDescription, term)
    ) {
      results.push({
        type: "tracking",
        title: item.referenceNumber,
        subtitle: `${item.shippingLine} • ${item.currentStatus}`,
        status: item.approvalStatus,
        link: "/tracking"
      });
    }
  }

  for (const item of db.shipments || []) {
    if (
      textIncludes(item.referenceNumber, term) ||
      textIncludes(item.clientName, term) ||
      textIncludes(item.shippingLine, term)
    ) {
      results.push({
        type: "registry",
        title: item.referenceNumber,
        subtitle: `${item.clientName} • ${item.shippingLine}`,
        status: item.currentStatus,
        link: `/registry-detail?id=${item.id}`
      });
    }
  }

  for (const item of db.accounting?.invoices || []) {
    if (
      textIncludes(item.invoiceNo, term) ||
      textIncludes(item.clientName, term) ||
      textIncludes(item.referenceNumber, term) ||
      textIncludes(item.service, term)
    ) {
      results.push({
        type: "accounting",
        title: item.invoiceNo,
        subtitle: `${item.clientName} • ${item.referenceNumber}`,
        status: item.status,
        link: `/registry-detail?id=${item.shipmentId}`
      });
    }
  }

  return results.slice(0, 12);
}

app.get("/", (req, res) => {
  const user = getSessionUser(req);
  if (user) return res.redirect("/dashboard");
  return res.redirect("/login");
});

app.get("/login", (req, res) => {
  const user = getSessionUser(req);
  if (user) return res.redirect("/dashboard");
  return res.sendFile(path.join(__dirname, "public", "login.html"));
});

app.post("/login", (req, res) => {
  const { username, password } = req.body;

  const user = USERS.find(
    (item) => item.username === username && item.password === password
  );

  if (!user) {
    return res.status(401).json({
      error: "Invalid username or password."
    });
  }

  const sessionId = createSession(user);

  res.cookie("marine_sid", sessionId, {
    httpOnly: true,
    sameSite: "lax",
    secure: false,
    maxAge: 1000 * 60 * 60 * 8
  });

  return res.json({ success: true });
});

app.post("/logout", (req, res) => {
  const sessionId = req.cookies.marine_sid;
  if (sessionId) sessions.delete(sessionId);
  res.clearCookie("marine_sid");
  return res.json({ success: true });
});

app.get("/api/me", requireAuth, (req, res) => {
  res.json({
    authenticated: true,
    user: {
      username: req.user.username,
      role: req.user.role
    }
  });
});

app.get("/dashboard", requirePageAuth("index.html"));
app.get("/about-system", requirePageAuth("about.html"));
app.get("/tracking", requirePageAuth("tracking.html"));
app.get("/shipment-registry", requirePageAuth("shipment-registry.html"));
app.get("/registry-detail", requirePageAuth("registry-detail.html"));
app.get("/accounting-system", requirePageAuth("accounting.html"));
app.get("/system-health", requirePageAuth("system-health.html"));

app.get("/api/dashboard-summary", requireAuth, (req, res) => {
  const db = readDb();
  const analytics = buildAnalytics(db.shipments || [], db.trackingRecords || []);
  const health = buildSystemHealth(db);
  const alerts = buildAlertSummary(db);
  const managementSummary = buildManagementSummary(db);
  const roleWidgets = buildRoleWidgets(req.user.role, db);

  res.json({
    analytics,
    health,
    alerts,
    managementSummary,
    roleWidgets,
    boardUpdatedAt: "9 Apr 2026, 16:52"
  });
});

app.get("/api/dashboard-notifications", requireAuth, (req, res) => {
  const db = readDb();
  const notifications = buildNotifications(db);
  res.json(notifications);
});

app.get("/api/global-search", requireAuth, (req, res) => {
  const db = readDb();
  const q = req.query.q || "";
  const results = buildGlobalSearchResults(db, q);
  res.json(results);
});

app.get("/api/tracking-records", requireAuth, (req, res) => {
  const db = readDb();
  const records = filterTrackingRecords(db.trackingRecords || [], req.query);
  res.json(records);
});

app.get("/api/reference-lookup/:referenceNumber", requireAuth, (req, res) => {
  const db = readDb();
  const ref = String(req.params.referenceNumber || "").trim().toUpperCase();

  const item = (db.referenceRegistry || []).find(
    (entry) => String(entry.referenceNumber || "").trim().toUpperCase() === ref
  );

  if (!item) {
    return res.status(404).json({
      error: "Reference not found in tracking sources."
    });
  }

  res.json(item);
});

app.post("/api/tracking-records/fetch", requireAuth, (req, res) => {
  const db = readDb();
  const ref = String(req.body.referenceNumber || "").trim().toUpperCase();

  if (!ref) {
    return res.status(400).json({ error: "Reference number is required." });
  }

  const sourceRecord = (db.referenceRegistry || []).find(
    (entry) => String(entry.referenceNumber || "").trim().toUpperCase() === ref
  );

  if (!sourceRecord) {
    return res.status(404).json({ error: "Reference not found in tracking sources." });
  }

  const existing = (db.trackingRecords || []).find(
    (item) => String(item.referenceNumber || "").trim().toUpperCase() === ref
  );

  if (existing) {
    return res.json(existing);
  }

  const newTrackingRecord = {
    id: Date.now(),
    referenceNumber: sourceRecord.referenceNumber,
    shippingLine: sourceRecord.shippingLine,
    cargoDescription: sourceRecord.cargoDescription,
    noOfPackages: sourceRecord.noOfPackages,
    shipmentWeight: sourceRecord.shipmentWeight,
    currentStatus: sourceRecord.currentStatus,
    approvalStatus: sourceRecord.approvalStatus,
    trackingSource: sourceRecord.trackingSource,
    sourceConfidence: sourceRecord.sourceConfidence || "High",
    fetchedAt: nowIso(),
    transferredToRegistry: false,
    createdAt: nowIso()
  };

  db.trackingRecords = db.trackingRecords || [];
  db.trackingRecords.unshift(newTrackingRecord);
  writeDb(db);

  res.json(newTrackingRecord);
});

app.post("/api/tracking-records/:id/transfer", requireAuth, (req, res) => {
  const db = readDb();
  const trackingId = Number(req.params.id);
  const { clientName, registryNotes } = req.body;

  const trackingRecord = (db.trackingRecords || []).find((item) => item.id === trackingId);

  if (!trackingRecord) {
    return res.status(404).json({ error: "Tracking record not found." });
  }

  if (trackingRecord.approvalStatus !== "Approved") {
    return res.status(400).json({
      error: "Only approved shipments can be transferred to registry."
    });
  }

  if (trackingRecord.transferredToRegistry) {
    return res.status(400).json({
      error: "This shipment has already been transferred to registry."
    });
  }

  if (!clientName) {
    return res.status(400).json({
      error: "Client Name is required before transfer to registry."
    });
  }

  const shipmentRecord = {
    id: Date.now(),
    trackingId: trackingRecord.id,
    clientName,
    referenceNumber: trackingRecord.referenceNumber,
    shippingLine: trackingRecord.shippingLine,
    cargoDescription: trackingRecord.cargoDescription,
    noOfPackages: trackingRecord.noOfPackages,
    shipmentWeight: trackingRecord.shipmentWeight,
    currentStatus: trackingRecord.currentStatus,
    approvalStatus: trackingRecord.approvalStatus,
    trackingSource: trackingRecord.trackingSource,
    sourceConfidence: trackingRecord.sourceConfidence || "High",
    registryStatus: "Registered",
    registryNotes: registryNotes || "",
    operationalNotes: "",
    transferredAt: nowIso(),
    accountingLink: null,
    attachments: [],
    auditLog: []
  };

  pushAuditLog(
    shipmentRecord,
    "TRANSFER_TO_REGISTRY",
    `Shipment transferred from tracking using reference ${shipmentRecord.referenceNumber}.`,
    req.user.username
  );

  db.shipments = db.shipments || [];
  db.shipments.unshift(shipmentRecord);

  trackingRecord.transferredToRegistry = true;

  writeDb(db);

  res.json({
    success: true,
    shipment: shipmentRecord
  });
});

app.get("/api/shipments", requireAuth, (req, res) => {
  const db = readDb();
  const shipments = filterShipments(db.shipments || [], req.query);
  res.json(shipments);
});

app.get("/api/shipments/:id", requireAuth, (req, res) => {
  const db = readDb();
  const shipmentId = Number(req.params.id);
  const shipment = (db.shipments || []).find((item) => item.id === shipmentId);

  if (!shipment) {
    return res.status(404).json({ error: "Shipment not found" });
  }

  res.json(shipment);
});

app.get("/api/shipments/:id/intelligence", requireAuth, (req, res) => {
  const db = readDb();
  const shipmentId = Number(req.params.id);

  const shipment = (db.shipments || []).find((item) => item.id === shipmentId);
  if (!shipment) {
    return res.status(404).json({ error: "Shipment not found" });
  }

  const trackingRecord = (db.trackingRecords || []).find(
    (item) => item.id === shipment.trackingId || item.referenceNumber === shipment.referenceNumber
  ) || null;

  const invoice = (db.accounting?.invoices || []).find(
    (item) => item.shipmentId === shipment.id
  ) || null;

  const intelligence = {
    sourceConfidence: shipment.sourceConfidence || trackingRecord?.sourceConfidence || "High",
    approvalState: shipment.approvalStatus,
    latestStatus: shipment.currentStatus,
    sourceName: shipment.trackingSource,
    fetchedTimestamp: trackingRecord?.fetchedAt || trackingRecord?.createdAt || "",
    transferredTimestamp: shipment.transferredAt || "",
    accountingLinked: !!shipment.accountingLink,
    invoiceStatus: invoice?.status || "Not Linked"
  };

  const timeline = buildShipmentTimeline(shipment, trackingRecord, invoice);

  res.json({
    shipment,
    trackingRecord,
    invoice,
    intelligence,
    timeline
  });
});

app.put("/api/shipments/:id", requireAuth, (req, res) => {
  const db = readDb();
  const shipmentId = Number(req.params.id);

  const index = (db.shipments || []).findIndex((shipment) => shipment.id === shipmentId);

  if (index === -1) {
    return res.status(404).json({ error: "Shipment not found" });
  }

  const { clientName, registryNotes, currentStatus, operationalNotes } = req.body;

  if (!clientName) {
    return res.status(400).json({
      error: "Client Name is required."
    });
  }

  db.shipments[index] = {
    ...db.shipments[index],
    clientName,
    registryNotes: registryNotes || "",
    operationalNotes: operationalNotes || "",
    currentStatus: currentStatus || db.shipments[index].currentStatus
  };

  pushAuditLog(
    db.shipments[index],
    "REGISTRY_UPDATED",
    `Registry file updated. Status: ${db.shipments[index].currentStatus}.`,
    req.user.username
  );

  writeDb(db);

  res.json(db.shipments[index]);
});

app.get("/api/accounting/invoices", requireAuth, (req, res) => {
  const db = readDb();
  const invoices = filterInvoices(db.accounting?.invoices || [], req.query);
  res.json(invoices);
});

app.post("/api/shipments/:id/accounting-link", requireAuth, (req, res) => {
  const db = readDb();
  const shipmentId = Number(req.params.id);

  const shipmentIndex = (db.shipments || []).findIndex((shipment) => shipment.id === shipmentId);

  if (shipmentIndex === -1) {
    return res.status(404).json({ error: "Shipment not found" });
  }

  const {
    invoiceNo,
    service,
    amount,
    status,
    dueDate,
    notes
  } = req.body;

  if (!invoiceNo || !service || !amount || !status) {
    return res.status(400).json({
      error: "Invoice No, Service, Amount, and Status are required."
    });
  }

  db.accounting = db.accounting || { invoices: [], payments: [], summary: {} };
  db.accounting.invoices = db.accounting.invoices || [];

  const existingInvoiceIndex = db.accounting.invoices.findIndex(
    (item) => item.shipmentId === shipmentId || item.invoiceNo === invoiceNo
  );

  const invoiceRecord = {
    shipmentId,
    invoiceNo,
    clientName: db.shipments[shipmentIndex].clientName,
    referenceNumber: db.shipments[shipmentIndex].referenceNumber,
    service,
    amount,
    status,
    dueDate: dueDate || "",
    notes: notes || ""
  };

  if (existingInvoiceIndex >= 0) {
    db.accounting.invoices[existingInvoiceIndex] = invoiceRecord;
  } else {
    db.accounting.invoices.unshift(invoiceRecord);
  }

  db.shipments[shipmentIndex].accountingLink = {
    invoiceNo,
    service,
    amount,
    status,
    dueDate: dueDate || "",
    notes: notes || ""
  };

  pushAuditLog(
    db.shipments[shipmentIndex],
    "ACCOUNTING_LINKED",
    `Accounting linked with invoice ${invoiceNo} (${status}).`,
    req.user.username
  );

  rebuildAccountingSummary(db);
  writeDb(db);

  res.json({
    success: true,
    shipment: db.shipments[shipmentIndex],
    invoice: invoiceRecord
  });
});

app.post("/api/shipments/:id/attachments", requireAuth, (req, res) => {
  const db = readDb();
  const shipmentId = Number(req.params.id);

  const shipmentIndex = (db.shipments || []).findIndex((shipment) => shipment.id === shipmentId);

  if (shipmentIndex === -1) {
    return res.status(404).json({ error: "Shipment not found" });
  }

  const { documentType, documentName, documentStatus, documentRef } = req.body;

  if (!documentType || !documentName) {
    return res.status(400).json({
      error: "Document Type and Document Name are required."
    });
  }

  const attachment = {
    id: Date.now(),
    documentType,
    documentName,
    documentStatus: documentStatus || "Active",
    documentRef: documentRef || "",
    uploadedAt: nowIso()
  };

  db.shipments[shipmentIndex].attachments = db.shipments[shipmentIndex].attachments || [];
  db.shipments[shipmentIndex].attachments.unshift(attachment);

  pushAuditLog(
    db.shipments[shipmentIndex],
    "DOCUMENT_ADDED",
    `Document added: ${documentType} - ${documentName}.`,
    req.user.username
  );

  writeDb(db);

  res.json({
    success: true,
    attachment,
    shipment: db.shipments[shipmentIndex]
  });
});

app.delete("/api/shipments/:id", requireAuth, (req, res) => {
  const db = readDb();
  const shipmentId = Number(req.params.id);

  const originalLength = (db.shipments || []).length;
  db.shipments = (db.shipments || []).filter((shipment) => shipment.id !== shipmentId);

  if (db.shipments.length === originalLength) {
    return res.status(404).json({ error: "Shipment not found" });
  }

  if (db.accounting?.invoices) {
    db.accounting.invoices = db.accounting.invoices.filter((item) => item.shipmentId !== shipmentId);
    rebuildAccountingSummary(db);
  }

  writeDb(db);
  res.json({ success: true });
});

app.get("/api/market-board", requireAuth, (req, res) => {
  const db = readDb();
  res.json(db.marketBoard || []);
});

app.get("/api/accounting", requireAuth, (req, res) => {
  const db = readDb();
  res.json(db.accounting || {});
});

app.get("/api/system-health", requireAuth, (req, res) => {
  const db = readDb();
  res.json(buildSystemHealth(db));
});

app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});