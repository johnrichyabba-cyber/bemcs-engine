const express = require("express");
const path = require("path");
const crypto = require("crypto");
const bcrypt = require("bcryptjs");
const { Pool } = require("pg");
const {
  createReference: createVizionReference,
  listReferenceUpdates: listVizionReferenceUpdates,
  normalizeUpdatesPayload: normalizeVizionUpdatesPayload
} = require("./providers/vizion");

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

function normalizeText(value) {
  return String(value || "").trim();
}

function normalizeNullableText(value) {
  const text = String(value || "").trim();
  return text || null;
}

function escapeHtml(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
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
  const now = new Date();

  if (new Date(session.expires_at) < now) {
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
    if (!session) {
      return res.redirect("/login");
    }

    req.user = session;
    next();
  } catch (error) {
    console.error("Auth middleware error:", error.message);
    return res.redirect("/login");
  }
}

async function requireAdmin(req, res, next) {
  try {
    const session = await getSession(req);

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
  } catch (error) {
    console.error("Admin middleware error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Authorization failed"
    });
  }
}

function allowRoles(...roles) {
  return async (req, res, next) => {
    try {
      const session = await getSession(req);

      if (!session) {
        return res.redirect("/login");
      }

      if (!roles.includes(session.role)) {
        return res.redirect("/403");
      }

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
        return res.status(401).json({
          success: false,
          error: "Unauthorized"
        });
      }

      if (!roles.includes(session.role)) {
        return res.status(403).json({
          success: false,
          error: "Forbidden"
        });
      }

      req.user = session;
      next();
    } catch (error) {
      console.error("API role middleware error:", error.message);
      return res.status(500).json({
        success: false,
        error: "Authorization failed"
      });
    }
  };
}

/* =========================
   TRACKING AUTOMATION CORE
   ========================= */

function mapExternalMilestoneToInternal(status) {
  const raw = String(status || "").trim().toLowerCase();

  const map = {
    registered: "Registered",
    booking_confirmed: "Booking Confirmed",
    booking: "Booking Confirmed",
    container_received: "Container Received",
    gate_in: "Container Received",
    at_pol: "At Port of Loading",
    loaded_on_vessel: "Loaded on Vessel",
    departed_origin: "Vessel Departed",
    vessel_departed: "Vessel Departed",
    in_transit: "In Transit",
    transshipment: "Transshipment",
    arrived_pod: "Arrived at Port of Discharge",
    discharged: "Arrived at Port of Discharge",
    manifest_available: "Manifest Available",
    customs_clearance: "Customs Clearance",
    released_by_shipping_line: "Released by Shipping Line",
    gate_out: "Out of Port",
    out_of_port: "Out of Port",
    arrived_icd: "Arrived at ICD",
    released_icd: "Released from ICD",
    delivered: "Delivered",
    completed: "Completed",
    closed: "Closed"
  };

  return map[raw] || status || "Tracking Update";
}

function mapVizionMilestoneToInternal(status) {
  const raw = String(status || "").trim().toLowerCase();

  const map = {
    "available for release / delivery": "Released by Shipping Line",
    "available for pickup": "Out of Port",
    "carrier release": "Released by Shipping Line",
    "container available for pickup": "Out of Port",
    "customs release": "Customs Clearance",
    "delivered": "Delivered",
    "gate in": "Container Received",
    "gate in at origin port": "At Port of Loading",
    "gate out": "Out of Port",
    "gate out from destination port": "Out of Port",
    "loaded": "Loaded on Vessel",
    "loaded on rail": "Arrived at ICD",
    "loaded on truck": "Out of Port",
    "loaded on vessel": "Loaded on Vessel",
    "loaded on vessel at origin port": "Loaded on Vessel",
    "loaded on vessel at transshipment port": "Transshipment",
    "out for delivery": "Delivered",
    "rail arrived at inland destination": "Arrived at ICD",
    "rail departed": "In Transit",
    "truck departed from destination port": "Out of Port"
  };

  return map[raw] || mapExternalMilestoneToInternal(status);
}

function rankMilestone(status) {
  const normalized = mapExternalMilestoneToInternal(status);

  const order = [
    "Registered",
    "Booking Confirmed",
    "Container Received",
    "At Port of Loading",
    "Loaded on Vessel",
    "Vessel Departed",
    "In Transit",
    "Transshipment",
    "Arrived at Port of Discharge",
    "Manifest Available",
    "Customs Clearance",
    "Released by Shipping Line",
    "Out of Port",
    "Arrived at ICD",
    "Released from ICD",
    "Delivered",
    "Completed",
    "Closed"
  ];

  const index = order.indexOf(normalized);
  return index >= 0 ? index : 0;
}

async function getShipmentById(shipmentId) {
  const result = await pool.query(
    `
    SELECT *
    FROM shipments
    WHERE id = $1
    LIMIT 1
    `,
    [shipmentId]
  );

  return result.rows[0] || null;
}

async function getTrackingSourceByShipmentId(shipmentId) {
  const result = await pool.query(
    `
    SELECT *
    FROM shipment_tracking_sources
    WHERE shipment_id = $1
    LIMIT 1
    `,
    [shipmentId]
  );

  return result.rows[0] || null;
}

async function logTrackingSync({
  shipmentId,
  providerName,
  requestPayload = null,
  responsePayload = null,
  syncStatus,
  message = "",
  syncedBy = "system"
}) {
  await pool.query(
    `
    INSERT INTO tracking_sync_logs (
      shipment_id,
      provider_name,
      request_payload,
      response_payload,
      sync_status,
      message,
      synced_by
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    `,
    [
      shipmentId,
      providerName,
      requestPayload ? JSON.stringify(requestPayload) : null,
      responsePayload ? JSON.stringify(responsePayload) : null,
      syncStatus,
      message,
      syncedBy
    ]
  );
}

async function upsertTrackingSource({
  shipmentId,
  providerName,
  carrierCode,
  billOfLading,
  containerNumber,
  bookingNumber,
  trackingMode,
  sourceLabel,
  webhookSecret,
  configJson,
  createdBy
}) {
  const existing = await getTrackingSourceByShipmentId(shipmentId);

  if (!existing) {
    const insertResult = await pool.query(
      `
      INSERT INTO shipment_tracking_sources (
        shipment_id,
        provider_name,
        carrier_code,
        bill_of_lading,
        container_number,
        booking_number,
        tracking_mode,
        source_label,
        webhook_secret,
        config_json,
        is_active,
        created_by,
        updated_by
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, TRUE, $11, $11)
      RETURNING *
      `,
      [
        shipmentId,
        providerName,
        carrierCode,
        billOfLading,
        containerNumber,
        bookingNumber,
        trackingMode,
        sourceLabel,
        webhookSecret,
        configJson ? JSON.stringify(configJson) : null,
        createdBy
      ]
    );

    return insertResult.rows[0];
  }

  const updateResult = await pool.query(
    `
    UPDATE shipment_tracking_sources
    SET
      provider_name = $2,
      carrier_code = $3,
      bill_of_lading = $4,
      container_number = $5,
      booking_number = $6,
      tracking_mode = $7,
      source_label = $8,
      webhook_secret = $9,
      config_json = $10,
      is_active = TRUE,
      updated_by = $11,
      updated_at = CURRENT_TIMESTAMP
    WHERE shipment_id = $1
    RETURNING *
    `,
    [
      shipmentId,
      providerName,
      carrierCode,
      billOfLading,
      containerNumber,
      bookingNumber,
      trackingMode,
      sourceLabel,
      webhookSecret,
      configJson ? JSON.stringify(configJson) : null,
      createdBy
    ]
  );

  return updateResult.rows[0];
}

async function saveProviderReferenceIds({
  shipmentId,
  externalReferenceId,
  parentReferenceId = null,
  providerStatus = null,
  updatedBy = "system"
}) {
  await pool.query(
    `
    UPDATE shipment_tracking_sources
    SET
      external_reference_id = $2,
      parent_reference_id = $3,
      provider_status = $4,
      updated_by = $5,
      updated_at = CURRENT_TIMESTAMP
    WHERE shipment_id = $1
    `,
    [shipmentId, externalReferenceId, parentReferenceId, providerStatus, updatedBy]
  );
}

async function insertTrackingEventIfNew({
  shipmentId,
  eventStatus,
  locationName,
  remarks,
  eventTime,
  createdBy,
  externalEventId = null,
  sourceName = null,
  sourceStatus = null,
  sourcePayload = null
}) {
  const duplicateCheck = await pool.query(
    `
    SELECT id
    FROM tracking_events
    WHERE shipment_id = $1
      AND (
        (external_event_id IS NOT NULL AND external_event_id = $2)
        OR (
          event_status = $3
          AND location_name = $4
          AND event_time = $5
        )
      )
    LIMIT 1
    `,
    [shipmentId, externalEventId, eventStatus, locationName, eventTime]
  );

  if (duplicateCheck.rows.length) {
    return {
      inserted: false,
      row: null
    };
  }

  const result = await pool.query(
    `
    INSERT INTO tracking_events (
      shipment_id,
      event_status,
      location_name,
      remarks,
      event_time,
      created_by,
      external_event_id,
      source_name,
      source_status,
      source_payload
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
    RETURNING *
    `,
    [
      shipmentId,
      eventStatus,
      locationName,
      remarks,
      eventTime,
      createdBy,
      externalEventId,
      sourceName,
      sourceStatus,
      sourcePayload ? JSON.stringify(sourcePayload) : null
    ]
  );

  return {
    inserted: true,
    row: result.rows[0]
  };
}

async function refreshShipmentCurrentStatus(shipmentId) {
  const result = await pool.query(
    `
    SELECT id, event_status, event_time
    FROM tracking_events
    WHERE shipment_id = $1
    ORDER BY event_time DESC, created_at DESC
    LIMIT 1
    `,
    [shipmentId]
  );

  if (!result.rows.length) {
    return null;
  }

  const latest = result.rows[0];

  await pool.query(
    `
    UPDATE shipments
    SET
      status = $2,
      current_tracking_status = $2,
      current_tracking_time = $3,
      updated_at = CURRENT_TIMESTAMP
    WHERE id = $1
    `,
    [shipmentId, latest.event_status, latest.event_time]
  );

  return latest;
}

async function buildLiveStatus(shipmentId) {
  const shipment = await getShipmentById(shipmentId);
  if (!shipment) return null;

  const source = await getTrackingSourceByShipmentId(shipmentId);

  const latestEventResult = await pool.query(
    `
    SELECT
      id,
      shipment_id,
      event_status,
      location_name,
      remarks,
      event_time,
      created_by,
      created_at,
      external_event_id,
      source_name,
      source_status
    FROM tracking_events
    WHERE shipment_id = $1
    ORDER BY event_time DESC, created_at DESC
    LIMIT 1
    `,
    [shipmentId]
  );

  const latestSyncResult = await pool.query(
    `
    SELECT
      id,
      provider_name,
      sync_status,
      message,
      synced_by,
      created_at
    FROM tracking_sync_logs
    WHERE shipment_id = $1
    ORDER BY created_at DESC
    LIMIT 1
    `,
    [shipmentId]
  );

  return {
    shipment_id: shipment.id,
    reference_number: shipment.reference_number,
    current_status: shipment.current_tracking_status || shipment.status,
    current_status_time: shipment.current_tracking_time,
    tracking_enabled: Boolean(source && source.is_active),
    tracking_source: source
      ? {
          provider_name: source.provider_name,
          carrier_code: source.carrier_code,
          bill_of_lading: source.bill_of_lading,
          container_number: source.container_number,
          booking_number: source.booking_number,
          tracking_mode: source.tracking_mode,
          source_label: source.source_label,
          external_reference_id: source.external_reference_id,
          parent_reference_id: source.parent_reference_id,
          provider_status: source.provider_status,
          last_synced_at: source.last_synced_at
        }
      : null,
    latest_event: latestEventResult.rows[0] || null,
    latest_sync: latestSyncResult.rows[0] || null
  };
}

/**
 * DEMO provider adapter.
 * Fallback ya foundation ikiwa provider sio vizion.
 */
async function fetchExternalTrackingEvents(shipment, source) {
  const baseNow = new Date();

  const fakeEvents = [
    {
      external_event_id: `${shipment.id}-registered`,
      external_status: "registered",
      location_name: shipment.origin_port || "Origin",
      remarks: "Shipment registered in BE-MCs tracking engine.",
      event_time: new Date(baseNow.getTime() - 1000 * 60 * 60 * 72).toISOString()
    },
    {
      external_event_id: `${shipment.id}-booking-confirmed`,
      external_status: "booking_confirmed",
      location_name: shipment.origin_port || "Origin",
      remarks: "Booking confirmed by shipping process.",
      event_time: new Date(baseNow.getTime() - 1000 * 60 * 60 * 48).toISOString()
    },
    {
      external_event_id: `${shipment.id}-at-pol`,
      external_status: "at_pol",
      location_name: shipment.origin_port || "Port of Loading",
      remarks: "Container available at port of loading.",
      event_time: new Date(baseNow.getTime() - 1000 * 60 * 60 * 24).toISOString()
    }
  ];

  if (source.container_number || source.bill_of_lading || source.booking_number) {
    fakeEvents.push({
      external_event_id: `${shipment.id}-in-transit`,
      external_status: "in_transit",
      location_name: "Live Ocean Route",
      remarks: "Automated milestone generated from configured tracking source.",
      event_time: new Date(baseNow.getTime() - 1000 * 60 * 60 * 2).toISOString()
    });
  }

  return {
    provider_name: source.provider_name,
    tracking_reference: source.container_number || source.bill_of_lading || source.booking_number || shipment.reference_number,
    events: fakeEvents
  };
}

async function syncShipmentTracking(shipmentId, actorUsername = "system", actorRole = null) {
  const shipment = await getShipmentById(shipmentId);

  if (!shipment) {
    throw new Error("Shipment not found.");
  }

  const source = await getTrackingSourceByShipmentId(shipmentId);

  if (!source || !source.is_active) {
    throw new Error("Tracking source is not configured for this shipment.");
  }

  const requestPayload = {
    provider_name: source.provider_name,
    carrier_code: source.carrier_code,
    bill_of_lading: source.bill_of_lading,
    container_number: source.container_number,
    booking_number: source.booking_number,
    tracking_mode: source.tracking_mode
  };

  try {
    const external = await fetchExternalTrackingEvents(shipment, source);
    const insertedEvents = [];

    for (const event of external.events) {
      const internalStatus = mapExternalMilestoneToInternal(event.external_status);

      const inserted = await insertTrackingEventIfNew({
        shipmentId,
        eventStatus: internalStatus,
        locationName: event.location_name || "Unknown",
        remarks: event.remarks || "",
        eventTime: event.event_time,
        createdBy: actorUsername || "system",
        externalEventId: event.external_event_id || null,
        sourceName: external.provider_name,
        sourceStatus: event.external_status,
        sourcePayload: event
      });

      if (inserted.inserted && inserted.row) {
        insertedEvents.push(inserted.row);

        await writeAuditLog({
          actorUsername: actorUsername || "system",
          actorRole,
          actionType: "AUTO_TRACKING_EVENT",
          entityType: "TRACKING_EVENT",
          entityId: inserted.row.id,
          shipmentId,
          details: `Automated tracking event "${internalStatus}" received from ${external.provider_name}.`
        });
      }
    }

    const latest = await refreshShipmentCurrentStatus(shipmentId);

    await pool.query(
      `
      UPDATE shipment_tracking_sources
      SET last_synced_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
      WHERE shipment_id = $1
      `,
      [shipmentId]
    );

    await logTrackingSync({
      shipmentId,
      providerName: external.provider_name,
      requestPayload,
      responsePayload: external,
      syncStatus: "SUCCESS",
      message: `Sync completed. Inserted ${insertedEvents.length} new event(s).`,
      syncedBy: actorUsername || "system"
    });

    return {
      success: true,
      provider_name: external.provider_name,
      inserted_events: insertedEvents,
      latest_status: latest ? latest.event_status : shipment.status
    };
  } catch (error) {
    await logTrackingSync({
      shipmentId,
      providerName: source.provider_name,
      requestPayload,
      responsePayload: null,
      syncStatus: "FAILED",
      message: error.message,
      syncedBy: actorUsername || "system"
    });

    throw error;
  }
}

async function subscribeShipmentToVizion(shipmentId, actorUsername = "system", actorRole = null) {
  const shipment = await getShipmentById(shipmentId);
  if (!shipment) {
    throw new Error("Shipment not found.");
  }

  const source = await getTrackingSourceByShipmentId(shipmentId);
  if (!source) {
    throw new Error("Tracking source is not configured for this shipment.");
  }

  const created = await createVizionReference({
    shipmentId,
    source
  });

  const responseBody = created.response_body || {};
  const referenceId =
    responseBody.reference_id ||
    responseBody.id ||
    responseBody.referenceId ||
    null;

  const parentReferenceId =
    responseBody.parent_reference_id ||
    responseBody.parentReferenceId ||
    null;

  const providerStatus =
    responseBody.status ||
    responseBody.reference_status ||
    "REFERENCE_CREATED";

  if (!referenceId) {
    throw new Error("Provider did not return a reference ID.");
  }

  await saveProviderReferenceIds({
    shipmentId,
    externalReferenceId: referenceId,
    parentReferenceId,
    providerStatus,
    updatedBy: actorUsername
  });

  await writeAuditLog({
    actorUsername,
    actorRole,
    actionType: "SUBSCRIBE_TRACKING_PROVIDER",
    entityType: "SHIPMENT_TRACKING_SOURCE",
    shipmentId,
    details: `Subscribed shipment ${shipment.reference_number} to Vizion with reference ${referenceId}.`
  });

  await logTrackingSync({
    shipmentId,
    providerName: "vizion",
    requestPayload: created.request_body,
    responsePayload: responseBody,
    syncStatus: "SUCCESS",
    message: `Tracking reference created: ${referenceId}`,
    syncedBy: actorUsername
  });

  return {
    provider_name: "vizion",
    reference_id: referenceId,
    parent_reference_id: parentReferenceId,
    provider_status: providerStatus
  };
}

async function syncShipmentTrackingFromVizion(shipmentId, actorUsername = "system", actorRole = null) {
  const shipment = await getShipmentById(shipmentId);
  if (!shipment) {
    throw new Error("Shipment not found.");
  }

  const source = await getTrackingSourceByShipmentId(shipmentId);
  if (!source) {
    throw new Error("Tracking source is not configured for this shipment.");
  }

  if (!source.external_reference_id) {
    throw new Error("No provider reference found. Subscribe shipment first.");
  }

  const providerPayload = await listVizionReferenceUpdates(source.external_reference_id);
  const events = normalizeVizionUpdatesPayload(providerPayload);
  const insertedEvents = [];

  for (const event of events) {
    const internalStatus = mapVizionMilestoneToInternal(event.event_status);

    const inserted = await insertTrackingEventIfNew({
      shipmentId,
      eventStatus: internalStatus,
      locationName: event.location_name || "Unknown",
      remarks: event.remarks || "",
      eventTime: event.event_time,
      createdBy: actorUsername || "system",
      externalEventId: event.external_event_id || null,
      sourceName: "vizion",
      sourceStatus: event.source_status || event.event_status,
      sourcePayload: event.raw_payload
    });

    if (inserted.inserted && inserted.row) {
      insertedEvents.push(inserted.row);

      await writeAuditLog({
        actorUsername,
        actorRole,
        actionType: "AUTO_TRACKING_EVENT",
        entityType: "TRACKING_EVENT",
        entityId: inserted.row.id,
        shipmentId,
        details: `Automated tracking event "${internalStatus}" received from Vizion.`
      });
    }
  }

  const latest = await refreshShipmentCurrentStatus(shipmentId);

  await pool.query(
    `
    UPDATE shipment_tracking_sources
    SET
      last_synced_at = CURRENT_TIMESTAMP,
      provider_status = 'SYNCED',
      updated_by = $2,
      updated_at = CURRENT_TIMESTAMP
    WHERE shipment_id = $1
    `,
    [shipmentId, actorUsername]
  );

  await logTrackingSync({
    shipmentId,
    providerName: "vizion",
    requestPayload: { reference_id: source.external_reference_id },
    responsePayload: providerPayload,
    syncStatus: "SUCCESS",
    message: `Vizion sync completed. Inserted ${insertedEvents.length} new event(s).`,
    syncedBy: actorUsername
  });

  return {
    success: true,
    provider_name: "vizion",
    inserted_events: insertedEvents,
    latest_status: latest ? latest.event_status : shipment.status
  };
}

/* =========================
   DATABASE INIT
   ========================= */

async function ensureTrackingSourceColumns() {
  await pool.query(`
    ALTER TABLE shipment_tracking_sources
    ADD COLUMN IF NOT EXISTS external_reference_id TEXT;
  `);

  await pool.query(`
    ALTER TABLE shipment_tracking_sources
    ADD COLUMN IF NOT EXISTS parent_reference_id TEXT;
  `);

  await pool.query(`
    ALTER TABLE shipment_tracking_sources
    ADD COLUMN IF NOT EXISTS provider_status TEXT;
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
      tracking_provider TEXT,
      carrier_code TEXT,
      bill_of_lading TEXT,
      container_number TEXT,
      booking_number TEXT,
      port_of_loading TEXT,
      port_of_discharge TEXT,
      final_destination TEXT,
      current_tracking_status TEXT,
      current_tracking_time TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);

  await pool.query(`
    ALTER TABLE shipments
    ADD COLUMN IF NOT EXISTS tracking_provider TEXT;
  `);

  await pool.query(`
    ALTER TABLE shipments
    ADD COLUMN IF NOT EXISTS carrier_code TEXT;
  `);

  await pool.query(`
    ALTER TABLE shipments
    ADD COLUMN IF NOT EXISTS bill_of_lading TEXT;
  `);

  await pool.query(`
    ALTER TABLE shipments
    ADD COLUMN IF NOT EXISTS container_number TEXT;
  `);

  await pool.query(`
    ALTER TABLE shipments
    ADD COLUMN IF NOT EXISTS booking_number TEXT;
  `);

  await pool.query(`
    ALTER TABLE shipments
    ADD COLUMN IF NOT EXISTS port_of_loading TEXT;
  `);

  await pool.query(`
    ALTER TABLE shipments
    ADD COLUMN IF NOT EXISTS port_of_discharge TEXT;
  `);

  await pool.query(`
    ALTER TABLE shipments
    ADD COLUMN IF NOT EXISTS final_destination TEXT;
  `);

  await pool.query(`
    ALTER TABLE shipments
    ADD COLUMN IF NOT EXISTS current_tracking_status TEXT;
  `);

  await pool.query(`
    ALTER TABLE shipments
    ADD COLUMN IF NOT EXISTS current_tracking_time TIMESTAMP;
  `);

  await pool.query(`
    ALTER TABLE shipments
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS tracking_events (
      id SERIAL PRIMARY KEY,
      shipment_id INTEGER NOT NULL REFERENCES shipments(id) ON DELETE CASCADE,
      event_status TEXT NOT NULL,
      location_name TEXT NOT NULL,
      remarks TEXT,
      event_time TIMESTAMP NOT NULL,
      created_by TEXT NOT NULL,
      external_event_id TEXT,
      source_name TEXT,
      source_status TEXT,
      source_payload JSONB,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);

  await pool.query(`
    ALTER TABLE tracking_events
    ADD COLUMN IF NOT EXISTS external_event_id TEXT;
  `);

  await pool.query(`
    ALTER TABLE tracking_events
    ADD COLUMN IF NOT EXISTS source_name TEXT;
  `);

  await pool.query(`
    ALTER TABLE tracking_events
    ADD COLUMN IF NOT EXISTS source_status TEXT;
  `);

  await pool.query(`
    ALTER TABLE tracking_events
    ADD COLUMN IF NOT EXISTS source_payload JSONB;
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS shipment_tracking_sources (
      id SERIAL PRIMARY KEY,
      shipment_id INTEGER UNIQUE NOT NULL REFERENCES shipments(id) ON DELETE CASCADE,
      provider_name TEXT NOT NULL,
      carrier_code TEXT,
      bill_of_lading TEXT,
      container_number TEXT,
      booking_number TEXT,
      tracking_mode TEXT NOT NULL DEFAULT 'polling',
      source_label TEXT,
      webhook_secret TEXT,
      config_json JSONB,
      is_active BOOLEAN NOT NULL DEFAULT TRUE,
      last_synced_at TIMESTAMP,
      created_by TEXT NOT NULL,
      updated_by TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);

  await ensureTrackingSourceColumns();

  await pool.query(`
    CREATE TABLE IF NOT EXISTS tracking_sync_logs (
      id SERIAL PRIMARY KEY,
      shipment_id INTEGER NOT NULL REFERENCES shipments(id) ON DELETE CASCADE,
      provider_name TEXT NOT NULL,
      request_payload JSONB,
      response_payload JSONB,
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

  await pool.query(`
    DELETE FROM sessions
    WHERE expires_at < NOW()
  `);

  console.log("PostgreSQL initialized successfully.");
}

/* =========================
   AUTH ROUTES
   ========================= */

app.get("/", async (req, res) => {
  try {
    const session = await getSession(req);
    if (session) return res.redirect("/dashboard");
    return res.redirect("/login");
  } catch (error) {
    return res.redirect("/login");
  }
});

app.get("/login", async (req, res) => {
  try {
    const session = await getSession(req);
    if (session) return res.redirect("/dashboard");
    return res.sendFile(path.join(__dirname, "public", "login.html"));
  } catch (error) {
    return res.sendFile(path.join(__dirname, "public", "login.html"));
  }
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

    return res.json({
      success: true,
      redirect: "/login"
    });
  } catch (error) {
    console.error("Logout error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Logout failed"
    });
  }
});

app.get("/api/me", requireAuth, (req, res) => {
  return res.json({
    success: true,
    user: req.user
  });
});

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

    await writeAuditLog({
      actorUsername: req.user.username,
      actorRole: req.user.role,
      actionType: "CREATE_USER",
      entityType: "USER",
      entityId: result.rows[0].id,
      details: `Created user ${result.rows[0].username} with role ${result.rows[0].role}.`
    });

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

/* =========================
   DASHBOARD
   ========================= */

app.get("/api/dashboard-summary", requireAuth, async (req, res) => {
  try {
    const shipmentsResult = await pool.query(`
      SELECT
        COUNT(*)::int AS total_shipments,
        COUNT(*) FILTER (WHERE LOWER(status) NOT IN ('delivered', 'completed', 'closed'))::int AS active_shipments,
        COUNT(*) FILTER (WHERE LOWER(status) IN ('delivered', 'completed', 'closed'))::int AS delivered_shipments
      FROM shipments
    `);

    const invoicesResult = await pool.query(`
      SELECT
        COUNT(*)::int AS total_invoices,
        COALESCE(SUM(amount), 0) AS total_billed
      FROM invoices
    `);

    const paidResult = await pool.query(`
      SELECT
        COALESCE(SUM(amount), 0) AS total_paid
      FROM payments
    `);

    const trackingResult = await pool.query(`
      SELECT
        te.id,
        te.event_status,
        te.location_name,
        te.remarks,
        te.event_time,
        s.reference_number,
        s.client_name
      FROM tracking_events te
      JOIN shipments s ON s.id = te.shipment_id
      ORDER BY te.event_time DESC, te.created_at DESC
      LIMIT 5
    `);

    const recentInvoicesResult = await pool.query(`
      SELECT
        i.id,
        i.invoice_number,
        i.charge_type,
        i.amount,
        i.currency,
        s.reference_number,
        s.client_name,
        i.created_at
      FROM invoices i
      JOIN shipments s ON s.id = i.shipment_id
      ORDER BY i.created_at DESC
      LIMIT 5
    `);

    const recentPaymentsResult = await pool.query(`
      SELECT
        p.id,
        p.amount,
        p.payment_method,
        p.reference_text,
        p.created_at,
        i.invoice_number,
        s.reference_number,
        s.client_name
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
    const outstanding = totalBilled - totalPaid;

    return res.json({
      success: true,
      summary: {
        total_shipments: Number(shipments.total_shipments || 0),
        active_shipments: Number(shipments.active_shipments || 0),
        delivered_shipments: Number(shipments.delivered_shipments || 0),
        total_invoices: Number(invoices.total_invoices || 0),
        total_billed: totalBilled,
        total_paid: totalPaid,
        outstanding
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

/* =========================
   AUDIT LOGS
   ========================= */

app.get("/api/audit-logs", apiAllowRoles("admin", "operations", "accounts", "customs"), async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        id,
        actor_username,
        actor_role,
        action_type,
        entity_type,
        entity_id,
        shipment_id,
        details,
        created_at
      FROM audit_logs
      ORDER BY created_at DESC
      LIMIT 100
    `);

    return res.json({
      success: true,
      logs: result.rows
    });
  } catch (error) {
    console.error("Audit logs error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Could not fetch audit logs."
    });
  }
});

app.get("/api/shipments/:id/audit-logs", apiAllowRoles("admin", "operations", "accounts", "customs"), async (req, res) => {
  const shipmentId = Number(req.params.id);

  try {
    const result = await pool.query(
      `
      SELECT
        id,
        actor_username,
        actor_role,
        action_type,
        entity_type,
        entity_id,
        shipment_id,
        details,
        created_at
      FROM audit_logs
      WHERE shipment_id = $1
      ORDER BY created_at DESC
      `,
      [shipmentId]
    );

    return res.json({
      success: true,
      logs: result.rows
    });
  } catch (error) {
    console.error("Shipment audit logs error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Could not fetch shipment audit logs."
    });
  }
});

/* =========================
   SHIPMENTS
   ========================= */

app.get("/api/shipments", apiAllowRoles("admin", "operations", "accounts", "customs"), async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        id,
        client_name,
        reference_number,
        shipping_line,
        cargo_description,
        status,
        origin_port,
        destination_port,
        created_by,
        tracking_provider,
        carrier_code,
        bill_of_lading,
        container_number,
        booking_number,
        port_of_loading,
        port_of_discharge,
        final_destination,
        current_tracking_status,
        current_tracking_time,
        updated_at,
        created_at
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

app.get("/api/shipments/:id", apiAllowRoles("admin", "operations", "accounts", "customs"), async (req, res) => {
  const id = Number(req.params.id);

  try {
    const shipmentResult = await pool.query(
      `
      SELECT
        id,
        client_name,
        reference_number,
        shipping_line,
        cargo_description,
        status,
        origin_port,
        destination_port,
        created_by,
        tracking_provider,
        carrier_code,
        bill_of_lading,
        container_number,
        booking_number,
        port_of_loading,
        port_of_discharge,
        final_destination,
        current_tracking_status,
        current_tracking_time,
        updated_at,
        created_at
      FROM shipments
      WHERE id = $1
      LIMIT 1
      `,
      [id]
    );

    if (!shipmentResult.rows.length) {
      return res.status(404).json({
        success: false,
        error: "Shipment not found."
      });
    }

    const trackingResult = await pool.query(
      `
      SELECT
        id,
        shipment_id,
        event_status,
        location_name,
        remarks,
        event_time,
        created_by,
        created_at,
        external_event_id,
        source_name,
        source_status
      FROM tracking_events
      WHERE shipment_id = $1
      ORDER BY event_time DESC, created_at DESC
      `,
      [id]
    );

    return res.json({
      success: true,
      shipment: shipmentResult.rows[0],
      tracking_events: trackingResult.rows
    });
  } catch (error) {
    console.error("Get shipment error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Could not fetch shipment."
    });
  }
});

app.get("/api/shipments/:id/full-detail", apiAllowRoles("admin", "operations", "accounts", "customs"), async (req, res) => {
  const shipmentId = Number(req.params.id);

  try {
    const shipmentResult = await pool.query(
      `
      SELECT
        id,
        client_name,
        reference_number,
        shipping_line,
        cargo_description,
        status,
        origin_port,
        destination_port,
        created_by,
        tracking_provider,
        carrier_code,
        bill_of_lading,
        container_number,
        booking_number,
        port_of_loading,
        port_of_discharge,
        final_destination,
        current_tracking_status,
        current_tracking_time,
        updated_at,
        created_at
      FROM shipments
      WHERE id = $1
      LIMIT 1
      `,
      [shipmentId]
    );

    if (!shipmentResult.rows.length) {
      return res.status(404).json({
        success: false,
        error: "Shipment not found."
      });
    }

    const trackingResult = await pool.query(
      `
      SELECT
        id,
        shipment_id,
        event_status,
        location_name,
        remarks,
        event_time,
        created_by,
        created_at,
        external_event_id,
        source_name,
        source_status
      FROM tracking_events
      WHERE shipment_id = $1
      ORDER BY event_time DESC, created_at DESC
      `,
      [shipmentId]
    );

    const invoicesResult = await pool.query(
      `
      SELECT
        i.id,
        i.shipment_id,
        i.invoice_number,
        i.charge_type,
        i.amount,
        i.currency,
        i.notes,
        i.created_by,
        i.created_at,
        COALESCE(SUM(p.amount), 0) AS paid_amount
      FROM invoices i
      LEFT JOIN payments p ON p.invoice_id = i.id
      WHERE i.shipment_id = $1
      GROUP BY
        i.id, i.shipment_id, i.invoice_number, i.charge_type,
        i.amount, i.currency, i.notes, i.created_by, i.created_at
      ORDER BY i.created_at DESC
      `,
      [shipmentId]
    );

    const documentsResult = await pool.query(
      `
      SELECT
        id,
        shipment_id,
        document_name,
        document_category,
        document_url,
        notes,
        uploaded_by,
        created_at
      FROM shipment_documents
      WHERE shipment_id = $1
      ORDER BY created_at DESC
      `,
      [shipmentId]
    );

    const invoiceIds = invoicesResult.rows.map((row) => row.id);
    let payments = [];

    if (invoiceIds.length) {
      const paymentsResult = await pool.query(
        `
        SELECT
          id,
          invoice_id,
          amount,
          payment_method,
          reference_text,
          notes,
          created_by,
          created_at
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
    const outstanding = totalBilled - totalPaid;

    const liveStatus = await buildLiveStatus(shipmentId);

    return res.json({
      success: true,
      shipment: shipmentResult.rows[0],
      tracking_events: trackingResult.rows,
      invoices,
      documents: documentsResult.rows,
      live_status: liveStatus,
      financial_summary: {
        total_billed: totalBilled,
        total_paid: totalPaid,
        outstanding
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

app.post("/api/shipments", apiAllowRoles("admin", "operations"), async (req, res) => {
  const clientName = normalizeText(req.body.client_name);
  const referenceNumber = normalizeText(req.body.reference_number);
  const shippingLine = normalizeText(req.body.shipping_line);
  const cargoDescription = normalizeText(req.body.cargo_description);
  const status = normalizeText(req.body.status || "Pending");
  const originPort = normalizeNullableText(req.body.origin_port);
  const destinationPort = normalizeNullableText(req.body.destination_port);

  const trackingProvider = normalizeNullableText(req.body.tracking_provider);
  const carrierCode = normalizeNullableText(req.body.carrier_code);
  const billOfLading = normalizeNullableText(req.body.bill_of_lading);
  const containerNumber = normalizeNullableText(req.body.container_number);
  const bookingNumber = normalizeNullableText(req.body.booking_number);
  const portOfLoading = normalizeNullableText(req.body.port_of_loading) || originPort;
  const portOfDischarge = normalizeNullableText(req.body.port_of_discharge) || destinationPort;
  const finalDestination = normalizeNullableText(req.body.final_destination) || destinationPort;

  if (!clientName || !referenceNumber || !shippingLine || !cargoDescription) {
    return res.status(400).json({
      success: false,
      error: "Client name, reference number, shipping line, and cargo description are required."
    });
  }

  try {
    const result = await pool.query(
      `
      INSERT INTO shipments (
        client_name,
        reference_number,
        shipping_line,
        cargo_description,
        status,
        origin_port,
        destination_port,
        created_by,
        tracking_provider,
        carrier_code,
        bill_of_lading,
        container_number,
        booking_number,
        port_of_loading,
        port_of_discharge,
        final_destination,
        current_tracking_status,
        current_tracking_time,
        updated_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $5, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
      RETURNING *
      `,
      [
        clientName,
        referenceNumber,
        shippingLine,
        cargoDescription,
        status,
        originPort,
        destinationPort,
        req.user.username,
        trackingProvider,
        carrierCode,
        billOfLading,
        containerNumber,
        bookingNumber,
        portOfLoading,
        portOfDischarge,
        finalDestination
      ]
    );

    const shipment = result.rows[0];

    if (trackingProvider || billOfLading || containerNumber || bookingNumber) {
      await upsertTrackingSource({
        shipmentId: shipment.id,
        providerName: trackingProvider || "vizion",
        carrierCode,
        billOfLading,
        containerNumber,
        bookingNumber,
        trackingMode: "polling",
        sourceLabel: "Primary Tracking Source",
        webhookSecret: null,
        configJson: {
          auto_sync_enabled: true
        },
        createdBy: req.user.username
      });
    }

    await writeAuditLog({
      actorUsername: req.user.username,
      actorRole: req.user.role,
      actionType: "CREATE_SHIPMENT",
      entityType: "SHIPMENT",
      entityId: shipment.id,
      shipmentId: shipment.id,
      details: `Created shipment ${shipment.reference_number} for client ${shipment.client_name}.`
    });

    return res.status(201).json({
      success: true,
      shipment
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

/* =========================
   TRACKING CONFIG + LIVE STATUS
   ========================= */

app.get("/api/tracking/providers", apiAllowRoles("admin", "operations", "accounts", "customs"), async (_req, res) => {
  return res.json({
    success: true,
    providers: [
      {
        code: "vizion",
        name: "Vizion",
        description: "Real ocean visibility provider for BL / booking / container tracking."
      },
      {
        code: "demo",
        name: "Demo Provider",
        description: "Fallback provider for backend automation testing."
      }
    ]
  });
});

app.post("/api/shipments/:id/tracking-config", apiAllowRoles("admin", "operations"), async (req, res) => {
  const shipmentId = Number(req.params.id);

  const providerName = normalizeText(req.body.provider_name || "vizion");
  const carrierCode = normalizeNullableText(req.body.carrier_code);
  const billOfLading = normalizeNullableText(req.body.bill_of_lading);
  const containerNumber = normalizeNullableText(req.body.container_number);
  const bookingNumber = normalizeNullableText(req.body.booking_number);
  const trackingMode = normalizeText(req.body.tracking_mode || "polling");
  const sourceLabel = normalizeNullableText(req.body.source_label) || "Primary Tracking Source";

  try {
    const shipment = await getShipmentById(shipmentId);

    if (!shipment) {
      return res.status(404).json({
        success: false,
        error: "Shipment not found."
      });
    }

    const source = await upsertTrackingSource({
      shipmentId,
      providerName,
      carrierCode,
      billOfLading,
      containerNumber,
      bookingNumber,
      trackingMode,
      sourceLabel,
      webhookSecret: null,
      configJson: {
        auto_sync_enabled: true
      },
      createdBy: req.user.username
    });

    await pool.query(
      `
      UPDATE shipments
      SET
        tracking_provider = $2,
        carrier_code = $3,
        bill_of_lading = $4,
        container_number = $5,
        booking_number = $6,
        updated_at = CURRENT_TIMESTAMP
      WHERE id = $1
      `,
      [shipmentId, providerName, carrierCode, billOfLading, containerNumber, bookingNumber]
    );

    let subscription = null;

    if (providerName === "vizion") {
      subscription = await subscribeShipmentToVizion(shipmentId, req.user.username, req.user.role);
    }

    await writeAuditLog({
      actorUsername: req.user.username,
      actorRole: req.user.role,
      actionType: "CONFIGURE_TRACKING_SOURCE",
      entityType: "SHIPMENT_TRACKING_SOURCE",
      entityId: source.id,
      shipmentId,
      details: `Configured tracking source ${providerName} for shipment ${shipment.reference_number}.`
    });

    return res.status(201).json({
      success: true,
      tracking_source: source,
      subscription
    });
  } catch (error) {
    console.error("Tracking config error:", error.message);
    return res.status(500).json({
      success: false,
      error: error.message || "Could not configure tracking source."
    });
  }
});

app.post("/api/shipments/:id/sync-tracking", apiAllowRoles("admin", "operations", "customs"), async (req, res) => {
  const shipmentId = Number(req.params.id);

  try {
    const source = await getTrackingSourceByShipmentId(shipmentId);

    if (!source) {
      return res.status(404).json({
        success: false,
        error: "Tracking source is not configured for this shipment."
      });
    }

    let result;

    if (source.provider_name === "vizion") {
      result = await syncShipmentTrackingFromVizion(shipmentId, req.user.username, req.user.role);
    } else {
      result = await syncShipmentTracking(shipmentId, req.user.username, req.user.role);
    }

    return res.json({
      success: true,
      sync: result
    });
  } catch (error) {
    console.error("Sync tracking error:", error.message);
    return res.status(500).json({
      success: false,
      error: error.message || "Could not sync shipment tracking."
    });
  }
});

app.get("/api/shipments/:id/live-status", apiAllowRoles("admin", "operations", "accounts", "customs"), async (req, res) => {
  const shipmentId = Number(req.params.id);

  try {
    const liveStatus = await buildLiveStatus(shipmentId);

    if (!liveStatus) {
      return res.status(404).json({
        success: false,
        error: "Shipment not found."
      });
    }

    return res.json({
      success: true,
      live_status: liveStatus
    });
  } catch (error) {
    console.error("Live status error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Could not load live shipment status."
    });
  }
});

app.get("/api/shipments/:id/tracking-source", apiAllowRoles("admin", "operations", "accounts", "customs"), async (req, res) => {
  const shipmentId = Number(req.params.id);

  try {
    const source = await getTrackingSourceByShipmentId(shipmentId);

    return res.json({
      success: true,
      tracking_source: source
    });
  } catch (error) {
    console.error("Tracking source fetch error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Could not load tracking source."
    });
  }
});

app.get("/api/shipments/:id/sync-logs", apiAllowRoles("admin", "operations", "accounts", "customs"), async (req, res) => {
  const shipmentId = Number(req.params.id);

  try {
    const result = await pool.query(
      `
      SELECT
        id,
        provider_name,
        sync_status,
        message,
        synced_by,
        created_at
      FROM tracking_sync_logs
      WHERE shipment_id = $1
      ORDER BY created_at DESC
      LIMIT 20
      `,
      [shipmentId]
    );

    return res.json({
      success: true,
      sync_logs: result.rows
    });
  } catch (error) {
    console.error("Tracking sync logs error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Could not load tracking sync logs."
    });
  }
});

/* =========================
   VIZION WEBHOOK
   ========================= */

app.post("/webhooks/vizion/:shipmentId", async (req, res) => {
  const shipmentId = Number(req.params.shipmentId);
  const token = String(req.query.token || "");
  const secret = String(process.env.VIZION_WEBHOOK_SECRET || "");

  if (!shipmentId || !secret || token !== secret) {
    return res.status(401).json({
      success: false,
      error: "Unauthorized webhook request."
    });
  }

  try {
    const events = normalizeVizionUpdatesPayload(req.body || {});
    const insertedEvents = [];

    for (const event of events) {
      const internalStatus = mapVizionMilestoneToInternal(event.event_status);

      const inserted = await insertTrackingEventIfNew({
        shipmentId,
        eventStatus: internalStatus,
        locationName: event.location_name || "Unknown",
        remarks: event.remarks || "",
        eventTime: event.event_time,
        createdBy: "vizion_webhook",
        externalEventId: event.external_event_id || null,
        sourceName: "vizion",
        sourceStatus: event.source_status || event.event_status,
        sourcePayload: event.raw_payload
      });

      if (inserted.inserted && inserted.row) {
        insertedEvents.push(inserted.row);
      }
    }

    await refreshShipmentCurrentStatus(shipmentId);

    await pool.query(
      `
      UPDATE shipment_tracking_sources
      SET
        last_synced_at = CURRENT_TIMESTAMP,
        provider_status = 'WEBHOOK_RECEIVED',
        updated_by = 'vizion_webhook',
        updated_at = CURRENT_TIMESTAMP
      WHERE shipment_id = $1
      `,
      [shipmentId]
    );

    await logTrackingSync({
      shipmentId,
      providerName: "vizion",
      requestPayload: null,
      responsePayload: req.body || {},
      syncStatus: "SUCCESS",
      message: `Vizion webhook processed. Inserted ${insertedEvents.length} event(s).`,
      syncedBy: "vizion_webhook"
    });

    return res.json({
      success: true
    });
  } catch (error) {
    console.error("Vizion webhook error:", error.message);

    await logTrackingSync({
      shipmentId,
      providerName: "vizion",
      requestPayload: null,
      responsePayload: req.body || {},
      syncStatus: "FAILED",
      message: error.message,
      syncedBy: "vizion_webhook"
    });

    return res.status(500).json({
      success: false,
      error: "Webhook processing failed."
    });
  }
});

/* =========================
   TRACKING EVENTS
   ========================= */

app.get("/api/tracking-events", apiAllowRoles("admin", "operations", "accounts", "customs"), async (req, res) => {
  const shipmentId = Number(req.query.shipment_id || 0);

  try {
    if (shipmentId) {
      const result = await pool.query(
        `
        SELECT
          te.id,
          te.shipment_id,
          te.event_status,
          te.location_name,
          te.remarks,
          te.event_time,
          te.created_by,
          te.created_at,
          te.external_event_id,
          te.source_name,
          te.source_status,
          s.reference_number,
          s.client_name
        FROM tracking_events te
        JOIN shipments s ON s.id = te.shipment_id
        WHERE te.shipment_id = $1
        ORDER BY te.event_time DESC, te.created_at DESC
        LIMIT 100
        `,
        [shipmentId]
      );

      return res.json({
        success: true,
        tracking_events: result.rows
      });
    }

    const result = await pool.query(`
      SELECT
        te.id,
        te.shipment_id,
        te.event_status,
        te.location_name,
        te.remarks,
        te.event_time,
        te.created_by,
        te.created_at,
        te.external_event_id,
        te.source_name,
        te.source_status,
        s.reference_number,
        s.client_name
      FROM tracking_events te
      JOIN shipments s ON s.id = te.shipment_id
      ORDER BY te.event_time DESC, te.created_at DESC
      LIMIT 50
    `);

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

app.get("/api/shipments/:id/tracking-events", apiAllowRoles("admin", "operations", "accounts", "customs"), async (req, res) => {
  const shipmentId = Number(req.params.id);

  try {
    const result = await pool.query(
      `
      SELECT
        id,
        shipment_id,
        event_status,
        location_name,
        remarks,
        event_time,
        created_by,
        created_at,
        external_event_id,
        source_name,
        source_status
      FROM tracking_events
      WHERE shipment_id = $1
      ORDER BY event_time DESC, created_at DESC
      `,
      [shipmentId]
    );

    return res.json({
      success: true,
      tracking_events: result.rows
    });
  } catch (error) {
    console.error("Shipment tracking events error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Could not fetch shipment tracking events."
    });
  }
});

app.post("/api/tracking-events", apiAllowRoles("admin", "operations", "customs"), async (req, res) => {
  const shipmentId = Number(req.body.shipment_id);
  const eventStatus = String(req.body.event_status || "").trim();
  const locationName = String(req.body.location_name || "").trim();
  const remarks = String(req.body.remarks || "").trim();
  const eventTime = String(req.body.event_time || "").trim();

  if (!shipmentId || !eventStatus || !locationName || !eventTime) {
    return res.status(400).json({
      success: false,
      error: "Shipment, event status, location name, and event time are required."
    });
  }

  try {
    const shipmentCheck = await pool.query(
      "SELECT id, reference_number FROM shipments WHERE id = $1 LIMIT 1",
      [shipmentId]
    );

    if (!shipmentCheck.rows.length) {
      return res.status(404).json({
        success: false,
        error: "Shipment not found."
      });
    }

    const inserted = await insertTrackingEventIfNew({
      shipmentId,
      eventStatus,
      locationName,
      remarks,
      eventTime,
      createdBy: req.user.username,
      externalEventId: null,
      sourceName: "manual",
      sourceStatus: eventStatus,
      sourcePayload: {
        mode: "manual"
      }
    });

    if (!inserted.inserted) {
      return res.status(409).json({
        success: false,
        error: "Duplicate tracking event detected."
      });
    }

    await refreshShipmentCurrentStatus(shipmentId);

    await writeAuditLog({
      actorUsername: req.user.username,
      actorRole: req.user.role,
      actionType: "ADD_TRACKING_EVENT",
      entityType: "TRACKING_EVENT",
      entityId: inserted.row.id,
      shipmentId,
      details: `Added tracking event "${eventStatus}" at "${locationName}" for shipment ${shipmentCheck.rows[0].reference_number}.`
    });

    return res.status(201).json({
      success: true,
      tracking_event: inserted.row
    });
  } catch (error) {
    console.error("Create tracking event error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Could not create tracking event."
    });
  }
});

/* =========================
   ACCOUNTING
   ========================= */

app.get("/api/invoices", apiAllowRoles("admin", "accounts", "operations"), async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        i.id,
        i.shipment_id,
        i.invoice_number,
        i.charge_type,
        i.amount,
        i.currency,
        i.notes,
        i.created_by,
        i.created_at,
        s.reference_number,
        s.client_name,
        COALESCE(SUM(p.amount), 0) AS paid_amount
      FROM invoices i
      JOIN shipments s ON s.id = i.shipment_id
      LEFT JOIN payments p ON p.invoice_id = i.id
      GROUP BY
        i.id, i.shipment_id, i.invoice_number, i.charge_type, i.amount,
        i.currency, i.notes, i.created_by, i.created_at, s.reference_number, s.client_name
      ORDER BY i.created_at DESC
    `);

    const invoices = result.rows.map((row) => {
      const amount = toMoney(row.amount);
      const paid = toMoney(row.paid_amount);
      return {
        ...row,
        amount,
        paid_amount: paid,
        balance: amount - paid
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

app.get("/api/invoices/:id", apiAllowRoles("admin", "accounts", "operations"), async (req, res) => {
  const invoiceId = Number(req.params.id);

  try {
    const invoiceResult = await pool.query(
      `
      SELECT
        i.id,
        i.shipment_id,
        i.invoice_number,
        i.charge_type,
        i.amount,
        i.currency,
        i.notes,
        i.created_by,
        i.created_at,
        s.reference_number,
        s.client_name
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
      SELECT
        id,
        invoice_id,
        amount,
        payment_method,
        reference_text,
        notes,
        created_by,
        created_at
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
    const amount = toMoney(invoice.amount);

    return res.json({
      success: true,
      invoice: {
        ...invoice,
        amount,
        paid_amount: paid,
        balance: amount - paid
      },
      payments
    });
  } catch (error) {
    console.error("Get invoice error:", error.message);
    return res.status(500).json({
      success: false,
      error: "Could not fetch invoice."
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
      "SELECT id, reference_number FROM shipments WHERE id = $1 LIMIT 1",
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
        shipment_id,
        invoice_number,
        charge_type,
        amount,
        currency,
        notes,
        created_by
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      RETURNING
        id,
        shipment_id,
        invoice_number,
        charge_type,
        amount,
        currency,
        notes,
        created_by,
        created_at
      `,
      [shipmentId, invoiceNumber, chargeType, amount, currency, notes, req.user.username]
    );

    await writeAuditLog({
      actorUsername: req.user.username,
      actorRole: req.user.role,
      actionType: "CREATE_INVOICE",
      entityType: "INVOICE",
      entityId: result.rows[0].id,
      shipmentId,
      details: `Created invoice ${invoiceNumber} for shipment ${shipmentCheck.rows[0].reference_number}.`
    });

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
      `
      SELECT i.id, i.invoice_number, i.shipment_id
      FROM invoices i
      WHERE i.id = $1
      LIMIT 1
      `,
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
        invoice_id,
        amount,
        payment_method,
        reference_text,
        notes,
        created_by
      )
      VALUES ($1, $2, $3, $4, $5, $6)
      RETURNING
        id,
        invoice_id,
        amount,
        payment_method,
        reference_text,
        notes,
        created_by,
        created_at
      `,
      [invoiceId, amount, paymentMethod, referenceText, notes, req.user.username]
    );

    await writeAuditLog({
      actorUsername: req.user.username,
      actorRole: req.user.role,
      actionType: "ADD_PAYMENT",
      entityType: "PAYMENT",
      entityId: result.rows[0].id,
      shipmentId: invoiceCheck.rows[0].shipment_id,
      details: `Added payment to invoice ${invoiceCheck.rows[0].invoice_number} via ${paymentMethod}.`
    });

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

/* =========================
   DOCUMENTS
   ========================= */

app.get("/api/shipments/:id/documents", apiAllowRoles("admin", "operations", "accounts", "customs"), async (req, res) => {
  const shipmentId = Number(req.params.id);

  try {
    const result = await pool.query(
      `
      SELECT
        id,
        shipment_id,
        document_name,
        document_category,
        document_url,
        notes,
        uploaded_by,
        created_at
      FROM shipment_documents
      WHERE shipment_id = $1
      ORDER BY created_at DESC
      `,
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
      "SELECT id, reference_number FROM shipments WHERE id = $1 LIMIT 1",
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
        shipment_id,
        document_name,
        document_category,
        document_url,
        notes,
        uploaded_by
      )
      VALUES ($1, $2, $3, $4, $5, $6)
      RETURNING
        id,
        shipment_id,
        document_name,
        document_category,
        document_url,
        notes,
        uploaded_by,
        created_at
      `,
      [
        shipmentId,
        documentName,
        documentCategory,
        documentUrl,
        notes,
        req.user.username
      ]
    );

    await writeAuditLog({
      actorUsername: req.user.username,
      actorRole: req.user.role,
      actionType: "ADD_DOCUMENT",
      entityType: "SHIPMENT_DOCUMENT",
      entityId: result.rows[0].id,
      shipmentId,
      details: `Added document "${documentName}" (${documentCategory}) to shipment ${shipmentCheck.rows[0].reference_number}.`
    });

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
      `
      SELECT
        id,
        shipment_id,
        document_name,
        document_category
      FROM shipment_documents
      WHERE id = $1
      LIMIT 1
      `,
      [documentId]
    );

    if (!existing.rows.length) {
      return res.status(404).json({
        success: false,
        error: "Document not found."
      });
    }

    const doc = existing.rows[0];

    await pool.query(
      "DELETE FROM shipment_documents WHERE id = $1",
      [documentId]
    );

    await writeAuditLog({
      actorUsername: req.user.username,
      actorRole: req.user.role,
      actionType: "DELETE_DOCUMENT",
      entityType: "SHIPMENT_DOCUMENT",
      entityId: documentId,
      shipmentId: doc.shipment_id,
      details: `Deleted document "${doc.document_name}" (${doc.document_category}).`
    });

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

/* =========================
   PAGES
   ========================= */

app.get("/dashboard", requireAuth, (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "dashboard.html"));
});

app.get("/index.html", requireAuth, (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "dashboard.html"));
});

app.get("/about", allowRoles("admin"), (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "about.html"));
});

app.get("/tracking", allowRoles("admin", "operations", "customs", "accounts"), (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "tracking.html"));
});

app.get("/shipment-registry", allowRoles("admin", "operations", "accounts", "customs"), (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "shipment-registry.html"));
});

app.get("/registry-detail", allowRoles("admin", "operations", "accounts", "customs"), (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "registry-detail.html"));
});

app.get("/accounting", allowRoles("admin", "accounts", "operations"), (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "accounting.html"));
});

app.get("/system-health", allowRoles("admin", "customs", "operations", "accounts"), (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "system-health.html"));
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

/* =========================
   START SERVER
   ========================= */

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