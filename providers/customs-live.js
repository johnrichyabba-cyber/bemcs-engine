"use strict";

const crypto = require("crypto");
const {
  createReference,
  listReferenceUpdates,
  normalizeUpdatesPayload
} = require("./vizion");

/**
 * Customs Live backend module
 *
 * What this file does:
 * 1. Accepts tracking lookups from Customs Live UI
 * 2. Searches existing internal shipment files
 * 3. Creates or refreshes provider tracking references for registered shipments
 * 4. Normalizes provider updates into engine tracking events
 * 5. Feeds live shipment status back to the UI
 *
 * Expected dependencies from server.js:
 * - app: Express app instance
 * - pool: pg Pool instance
 * - requireAuth: auth middleware
 *
 * Usage from server.js:
 * const { attachCustomsLiveRoutes } = require("./providers/customs-live");
 * attachCustomsLiveRoutes({ app, pool, requireAuth });
 */

function escapeLikeValue(value) {
  return String(value || "").replace(/[%_]/g, "\\$&");
}

function normalizeLookupInput(mode, number) {
  const normalizedMode = String(mode || "").trim().toLowerCase();
  const normalizedNumber = String(number || "").trim().toUpperCase();

  const patterns = {
    container: /^[A-Z]{4}\d{6,7}$/,
    air_cargo: /^\d{3}-?\d{8}$/,
    post_ems: /^[A-Z]{2}\d{9}[A-Z]{2}$/,
    bill_of_lading: /^.{6,}$/,
    express: /^.{6,}$/
  };

  const detectedValid = patterns[normalizedMode]
    ? patterns[normalizedMode].test(normalizedNumber)
    : normalizedNumber.length >= 4;

  let hint = "Accepted for operational lookup.";

  if (normalizedMode === "container" && !detectedValid) {
    hint = "Container format usually requires 4 letters followed by 6 or 7 digits.";
  } else if (normalizedMode === "air_cargo" && !detectedValid) {
    hint = "Air cargo format usually appears as 3 digits followed by 8 digits.";
  } else if (normalizedMode === "post_ems" && !detectedValid) {
    hint = "Post / EMS format usually appears as 2 letters + 9 digits + 2 letters.";
  } else if (normalizedMode === "bill_of_lading" && !detectedValid) {
    hint = "Bill of Lading value looks too short for operational lookup.";
  } else if (normalizedMode === "express" && !detectedValid) {
    hint = "Express tracking reference looks too short.";
  }

  return {
    mode: normalizedMode,
    number: normalizedNumber,
    valid: detectedValid,
    hint
  };
}

async function findInternalShipmentMatch(pool, mode, number) {
  const value = String(number || "").trim().toUpperCase();

  let query = "";
  let params = [];

  if (mode === "container") {
    query = `
      SELECT *
      FROM shipments
      WHERE UPPER(COALESCE(container_number, '')) = $1
      ORDER BY id DESC
      LIMIT 1
    `;
    params = [value];
  } else if (mode === "bill_of_lading") {
    query = `
      SELECT *
      FROM shipments
      WHERE UPPER(COALESCE(bill_of_lading, '')) = $1
      ORDER BY id DESC
      LIMIT 1
    `;
    params = [value];
  } else if (mode === "express") {
    query = `
      SELECT *
      FROM shipments
      WHERE UPPER(COALESCE(reference_number, '')) = $1
         OR UPPER(COALESCE(booking_number, '')) = $1
      ORDER BY id DESC
      LIMIT 1
    `;
    params = [value];
  } else {
    query = `
      SELECT *
      FROM shipments
      WHERE UPPER(COALESCE(reference_number, '')) = $1
         OR UPPER(COALESCE(bill_of_lading, '')) = $1
         OR UPPER(COALESCE(container_number, '')) = $1
         OR UPPER(COALESCE(booking_number, '')) = $1
      ORDER BY id DESC
      LIMIT 1
    `;
    params = [value];
  }

  const result = await pool.query(query, params);
  return result.rows[0] || null;
}

async function getTrackingSourceByShipmentId(pool, shipmentId) {
  try {
    const result = await pool.query(
      `
      SELECT *
      FROM tracking_sources
      WHERE shipment_id = $1
      ORDER BY id DESC
      LIMIT 1
      `,
      [shipmentId]
    );

    return result.rows[0] || null;
  } catch (_error) {
    return null;
  }
}

async function getLatestSyncLogByShipmentId(pool, shipmentId) {
  try {
    const result = await pool.query(
      `
      SELECT *
      FROM sync_logs
      WHERE shipment_id = $1
      ORDER BY created_at DESC, id DESC
      LIMIT 1
      `,
      [shipmentId]
    );

    return result.rows[0] || null;
  } catch (_error) {
    return null;
  }
}

async function getLatestTrackingEventByShipmentId(pool, shipmentId) {
  try {
    const result = await pool.query(
      `
      SELECT *
      FROM tracking_events
      WHERE shipment_id = $1
      ORDER BY event_time DESC NULLS LAST, id DESC
      LIMIT 1
      `,
      [shipmentId]
    );

    return result.rows[0] || null;
  } catch (_error) {
    return null;
  }
}

async function getTrackingEventsByShipmentId(pool, shipmentId) {
  try {
    const result = await pool.query(
      `
      SELECT *
      FROM tracking_events
      WHERE shipment_id = $1
      ORDER BY event_time DESC NULLS LAST, id DESC
      `,
      [shipmentId]
    );

    return result.rows || [];
  } catch (_error) {
    return [];
  }
}

async function buildLiveStatus(pool, shipment) {
  if (!shipment) return null;

  const [trackingSource, latestSync, latestEvent] = await Promise.all([
    getTrackingSourceByShipmentId(pool, shipment.id),
    getLatestSyncLogByShipmentId(pool, shipment.id),
    getLatestTrackingEventByShipmentId(pool, shipment.id)
  ]);

  return {
    shipment_id: shipment.id,
    reference_number: shipment.reference_number,
    current_status: shipment.current_tracking_status || shipment.status || null,
    current_status_time: shipment.current_tracking_time || null,
    tracking_enabled: Boolean(
      shipment.tracking_provider ||
      shipment.bill_of_lading ||
      shipment.container_number ||
      shipment.booking_number
    ),
    tracking_source: trackingSource,
    latest_sync: latestSync,
    latest_event: latestEvent
  };
}

function chooseTrackingIdentity(shipment) {
  const providerName = String(shipment.tracking_provider || "vizion").trim().toLowerCase();

  return {
    provider_name: providerName,
    carrier_code: shipment.carrier_code || null,
    bill_of_lading: shipment.bill_of_lading || null,
    container_number: shipment.container_number || null,
    booking_number: shipment.booking_number || null
  };
}

function buildProviderCreatePayload(shipment) {
  const identity = chooseTrackingIdentity(shipment);

  return {
    provider_name: identity.provider_name,
    carrier_code: identity.carrier_code || undefined,
    bill_of_lading: identity.bill_of_lading || undefined,
    container_number: identity.container_number || undefined,
    booking_number: identity.booking_number || undefined,
    source_label: "Customs Live Primary Source"
  };
}

async function upsertTrackingSource(pool, shipment, providerReference) {
  const identity = chooseTrackingIdentity(shipment);

  const existing = await getTrackingSourceByShipmentId(pool, shipment.id);

  const externalReferenceId =
    providerReference?.referenceId ||
    providerReference?.reference_id ||
    providerReference?.id ||
    providerReference?.uuid ||
    null;

  const providerStatus =
    providerReference?.status ||
    providerReference?.providerStatus ||
    "active";

  if (existing) {
    await pool.query(
      `
      UPDATE tracking_sources
      SET provider_name = $2,
          carrier_code = $3,
          bill_of_lading = $4,
          container_number = $5,
          booking_number = $6,
          tracking_mode = $7,
          source_label = $8,
          external_reference_id = $9,
          provider_status = $10,
          last_synced_at = NOW()
      WHERE id = $1
      `,
      [
        existing.id,
        identity.provider_name,
        identity.carrier_code,
        identity.bill_of_lading,
        identity.container_number,
        identity.booking_number,
        "polling",
        "Primary Tracking Source",
        externalReferenceId,
        providerStatus
      ]
    );

    return {
      ...existing,
      provider_name: identity.provider_name,
      carrier_code: identity.carrier_code,
      bill_of_lading: identity.bill_of_lading,
      container_number: identity.container_number,
      booking_number: identity.booking_number,
      tracking_mode: "polling",
      source_label: "Primary Tracking Source",
      external_reference_id: externalReferenceId,
      provider_status: providerStatus
    };
  }

  const insertResult = await pool.query(
    `
    INSERT INTO tracking_sources (
      shipment_id,
      provider_name,
      carrier_code,
      bill_of_lading,
      container_number,
      booking_number,
      tracking_mode,
      source_label,
      external_reference_id,
      provider_status,
      last_synced_at
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW())
    RETURNING *
    `,
    [
      shipment.id,
      identity.provider_name,
      identity.carrier_code,
      identity.bill_of_lading,
      identity.container_number,
      identity.booking_number,
      "polling",
      "Primary Tracking Source",
      externalReferenceId,
      providerStatus
    ]
  );

  return insertResult.rows[0];
}

function toEventHash(shipmentId, event) {
  const raw = [
    shipmentId,
    event.event_status || "",
    event.location_name || "",
    event.event_time || "",
    event.source_name || ""
  ].join("|");

  return crypto.createHash("sha1").update(raw).digest("hex");
}

function normalizeSingleUpdate(update, shipment) {
  const eventStatus =
    update.event_status ||
    update.status ||
    update.milestone ||
    update.description ||
    update.title ||
    "Provider Update";

  const locationName =
    update.location_name ||
    update.location ||
    update.place ||
    update.city ||
    update.port ||
    null;

  const eventTime =
    update.event_time ||
    update.timestamp ||
    update.time ||
    update.date ||
    new Date().toISOString();

  const remarks =
    update.remarks ||
    update.message ||
    update.details ||
    update.description ||
    null;

  return {
    shipment_id: shipment.id,
    event_status: eventStatus,
    location_name: locationName,
    event_time: eventTime,
    remarks,
    source_name: shipment.tracking_provider || "vizion",
    created_by: "system"
  };
}

async function insertTrackingEventsIfMissing(pool, shipment, rawUpdates) {
  const updates = Array.isArray(rawUpdates) ? rawUpdates : [];
  const inserted = [];

  for (const rawUpdate of updates) {
    const event = normalizeSingleUpdate(rawUpdate, shipment);
    const dedupeHash = toEventHash(shipment.id, event);

    let exists = false;

    try {
      const check = await pool.query(
        `
        SELECT id
        FROM tracking_events
        WHERE shipment_id = $1
          AND md5(
            COALESCE(event_status, '') || '|' ||
            COALESCE(location_name, '') || '|' ||
            COALESCE(CAST(event_time AS TEXT), '') || '|' ||
            COALESCE(source_name, '')
          ) = md5($2)
        LIMIT 1
        `,
        [
          shipment.id,
          [
            event.event_status || "",
            event.location_name || "",
            event.event_time || "",
            event.source_name || ""
          ].join("|")
        ]
      );

      exists = check.rowCount > 0;
    } catch (_error) {
      exists = false;
    }

    if (exists) {
      continue;
    }

    const insertResult = await pool.query(
      `
      INSERT INTO tracking_events (
        shipment_id,
        event_status,
        location_name,
        event_time,
        remarks,
        source_name,
        created_by
      )
      VALUES ($1,$2,$3,$4,$5,$6,$7)
      RETURNING *
      `,
      [
        shipment.id,
        event.event_status,
        event.location_name,
        event.event_time,
        event.remarks,
        event.source_name,
        event.created_by
      ]
    );

    inserted.push(insertResult.rows[0]);
  }

  return inserted;
}

async function updateShipmentCurrentStatus(pool, shipmentId, latestEvent) {
  if (!latestEvent) return;

  await pool.query(
    `
    UPDATE shipments
    SET current_tracking_status = $2,
        current_tracking_time = $3
    WHERE id = $1
    `,
    [
      shipmentId,
      latestEvent.event_status || null,
      latestEvent.event_time || null
    ]
  );
}

async function insertSyncLog(pool, shipmentId, payload) {
  const result = await pool.query(
    `
    INSERT INTO sync_logs (
      shipment_id,
      provider_name,
      sync_status,
      message,
      synced_by,
      created_at
    )
    VALUES ($1,$2,$3,$4,$5,NOW())
    RETURNING *
    `,
    [
      shipmentId,
      payload.provider_name || "vizion",
      payload.sync_status || "success",
      payload.message || "",
      payload.synced_by || "system"
    ]
  );

  return result.rows[0];
}

async function ensureProviderReference(pool, shipment) {
  let trackingSource = await getTrackingSourceByShipmentId(pool, shipment.id);

  if (trackingSource?.external_reference_id) {
    return trackingSource;
  }

  const providerPayload = buildProviderCreatePayload(shipment);
  const providerReference = await createReference(providerPayload);
  trackingSource = await upsertTrackingSource(pool, shipment, providerReference);

  await insertSyncLog(pool, shipment.id, {
    provider_name: shipment.tracking_provider || "vizion",
    sync_status: "registered",
    message: "Provider reference created for live tracking.",
    synced_by: "system"
  });

  return trackingSource;
}

async function syncShipmentFromProvider(pool, shipment) {
  const trackingSource = await ensureProviderReference(pool, shipment);

  if (!trackingSource?.external_reference_id) {
    throw new Error("External provider reference is missing after registration.");
  }

  const providerPayload = {
    reference_id: trackingSource.external_reference_id,
    external_reference_id: trackingSource.external_reference_id
  };

  const updatesPayload = await listReferenceUpdates(providerPayload);
  const normalizedUpdates = normalizeUpdatesPayload(updatesPayload) || [];
  const insertedEvents = await insertTrackingEventsIfMissing(pool, shipment, normalizedUpdates);

  const allEvents = await getTrackingEventsByShipmentId(pool, shipment.id);
  const latestEvent = allEvents[0] || null;

  await updateShipmentCurrentStatus(pool, shipment.id, latestEvent);

  await pool.query(
    `
    UPDATE tracking_sources
    SET provider_status = $2,
        last_synced_at = NOW()
    WHERE shipment_id = $1
    `,
    [shipment.id, "synced"]
  );

  const syncLog = await insertSyncLog(pool, shipment.id, {
    provider_name: shipment.tracking_provider || "vizion",
    sync_status: "success",
    message: `Provider sync completed. ${insertedEvents.length} new event(s) inserted.`,
    synced_by: "system"
  });

  const shipmentResult = await pool.query(
    `SELECT * FROM shipments WHERE id = $1 LIMIT 1`,
    [shipment.id]
  );
  const updatedShipment = shipmentResult.rows[0] || shipment;

  return {
    shipment: updatedShipment,
    tracking_source: await getTrackingSourceByShipmentId(pool, shipment.id),
    latest_event: latestEvent,
    inserted_events: insertedEvents,
    sync_log: syncLog,
    normalized_updates_count: normalizedUpdates.length
  };
}

async function tryPassiveExternalPreview(pool, lookup) {
  /**
   * Safe preview behavior:
   * - We do NOT force provider registration for every random number at lookup stage.
   * - We only return a structured intake preview.
   * - External provider sync becomes authoritative once the shipment is formally registered.
   *
   * This keeps Customs Live operational and avoids uncontrolled provider subscriptions.
   */

  return {
    mode: lookup.mode,
    number: lookup.number,
    valid: lookup.valid,
    hint: lookup.hint,
    external_preview_supported: ["container", "bill_of_lading"].includes(lookup.mode),
    preview_status: "intake_only",
    message:
      "External provider sync is activated after the shipment file is formally registered in Front Desk Registry."
  };
}

function attachCustomsLiveRoutes({ app, pool, requireAuth }) {
  if (!app || !pool || !requireAuth) {
    throw new Error("attachCustomsLiveRoutes requires app, pool, and requireAuth.");
  }

  /**
   * POST /api/customs-live/lookup
   * body: { mode, number }
   *
   * Returns:
   * - normalized lookup result
   * - internal shipment match (if found)
   * - passive external preview info
   * - live status/timeline if shipment is already registered
   */
  app.post("/api/customs-live/lookup", requireAuth, async (req, res) => {
    try {
      const { mode, number } = req.body || {};

      if (!mode || !number) {
        return res.status(400).json({
          success: false,
          error: "Tracking mode and tracking number are required."
        });
      }

      const lookup = normalizeLookupInput(mode, number);
      const matchedShipment = await findInternalShipmentMatch(pool, lookup.mode, lookup.number);

      if (matchedShipment) {
        const [liveStatus, timeline] = await Promise.all([
          buildLiveStatus(pool, matchedShipment),
          getTrackingEventsByShipmentId(pool, matchedShipment.id)
        ]);

        return res.json({
          success: true,
          lookup,
          matched: true,
          shipment: matchedShipment,
          live_status: liveStatus,
          timeline
        });
      }

      const preview = await tryPassiveExternalPreview(pool, lookup);

      return res.json({
        success: true,
        lookup,
        matched: false,
        shipment: null,
        external_preview: preview
      });
    } catch (error) {
      return res.status(500).json({
        success: false,
        error: error.message || "Customs Live lookup failed."
      });
    }
  });

  /**
   * GET /api/customs-live/lookup/:mode/:number
   * Same as POST lookup but browser-friendly.
   */
  app.get("/api/customs-live/lookup/:mode/:number", requireAuth, async (req, res) => {
    try {
      const lookup = normalizeLookupInput(req.params.mode, req.params.number);
      const matchedShipment = await findInternalShipmentMatch(pool, lookup.mode, lookup.number);

      if (matchedShipment) {
        const [liveStatus, timeline] = await Promise.all([
          buildLiveStatus(pool, matchedShipment),
          getTrackingEventsByShipmentId(pool, matchedShipment.id)
        ]);

        return res.json({
          success: true,
          lookup,
          matched: true,
          shipment: matchedShipment,
          live_status: liveStatus,
          timeline
        });
      }

      const preview = await tryPassiveExternalPreview(pool, lookup);

      return res.json({
        success: true,
        lookup,
        matched: false,
        shipment: null,
        external_preview: preview
      });
    } catch (error) {
      return res.status(500).json({
        success: false,
        error: error.message || "Customs Live lookup failed."
      });
    }
  });

  /**
   * POST /api/shipments/:id/live-refresh
   * On-demand refresh from provider after shipment has been registered.
   */
  app.post("/api/shipments/:id/live-refresh", requireAuth, async (req, res) => {
    try {
      const shipmentId = Number(req.params.id);

      if (!shipmentId) {
        return res.status(400).json({
          success: false,
          error: "Valid shipment ID is required."
        });
      }

      const shipmentResult = await pool.query(
        `SELECT * FROM shipments WHERE id = $1 LIMIT 1`,
        [shipmentId]
      );

      const shipment = shipmentResult.rows[0];

      if (!shipment) {
        return res.status(404).json({
          success: false,
          error: "Shipment not found."
        });
      }

      const syncResult = await syncShipmentFromProvider(pool, shipment);
      const liveStatus = await buildLiveStatus(pool, syncResult.shipment);

      return res.json({
        success: true,
        shipment: syncResult.shipment,
        live_status: liveStatus,
        latest_event: syncResult.latest_event,
        inserted_events: syncResult.inserted_events,
        sync_log: syncResult.sync_log
      });
    } catch (error) {
      return res.status(500).json({
        success: false,
        error: error.message || "Live refresh failed."
      });
    }
  });

  /**
   * GET /api/shipments/:id/live-updates
   * Read-only endpoint for UI polling.
   */
  app.get("/api/shipments/:id/live-updates", requireAuth, async (req, res) => {
    try {
      const shipmentId = Number(req.params.id);

      if (!shipmentId) {
        return res.status(400).json({
          success: false,
          error: "Valid shipment ID is required."
        });
      }

      const shipmentResult = await pool.query(
        `SELECT * FROM shipments WHERE id = $1 LIMIT 1`,
        [shipmentId]
      );

      const shipment = shipmentResult.rows[0];

      if (!shipment) {
        return res.status(404).json({
          success: false,
          error: "Shipment not found."
        });
      }

      const [liveStatus, timeline, syncLogs] = await Promise.all([
        buildLiveStatus(pool, shipment),
        getTrackingEventsByShipmentId(pool, shipment.id),
        pool.query(
          `
          SELECT *
          FROM sync_logs
          WHERE shipment_id = $1
          ORDER BY created_at DESC, id DESC
          LIMIT 20
          `,
          [shipment.id]
        )
      ]);

      return res.json({
        success: true,
        shipment,
        live_status: liveStatus,
        timeline,
        sync_logs: syncLogs.rows || []
      });
    } catch (error) {
      return res.status(500).json({
        success: false,
        error: error.message || "Could not load live updates."
      });
    }
  });

  /**
   * POST /api/shipments/:id/enable-live-tracking
   * Ensures the provider reference exists immediately after a file is registered.
   */
  app.post("/api/shipments/:id/enable-live-tracking", requireAuth, async (req, res) => {
    try {
      const shipmentId = Number(req.params.id);

      if (!shipmentId) {
        return res.status(400).json({
          success: false,
          error: "Valid shipment ID is required."
        });
      }

      const shipmentResult = await pool.query(
        `SELECT * FROM shipments WHERE id = $1 LIMIT 1`,
        [shipmentId]
      );

      const shipment = shipmentResult.rows[0];

      if (!shipment) {
        return res.status(404).json({
          success: false,
          error: "Shipment not found."
        });
      }

      const trackingSource = await ensureProviderReference(pool, shipment);

      return res.json({
        success: true,
        shipment_id: shipment.id,
        tracking_source: trackingSource,
        message: "Live tracking has been enabled for this shipment."
      });
    } catch (error) {
      return res.status(500).json({
        success: false,
        error: error.message || "Could not enable live tracking."
      });
    }
  });
}

module.exports = {
  attachCustomsLiveRoutes,
  normalizeLookupInput,
  findInternalShipmentMatch,
  buildLiveStatus,
  syncShipmentFromProvider
};