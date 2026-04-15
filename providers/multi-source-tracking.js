"use strict";

const { createReference, listReferenceUpdates, normalizeUpdatesPayload } = require("./vizion");
const { lookupExpressShipment } = require("./source-adapters/demo-express");
const { lookupAirShipment } = require("./source-adapters/demo-air");

function normalizeMode(mode) {
  return String(mode || "").trim().toLowerCase();
}

function normalizeTrackingNumber(number) {
  return String(number || "").trim().toUpperCase();
}

async function lookupOceanContainer(trackingNumber) {
  const number = normalizeTrackingNumber(trackingNumber);

  if (!number) {
    return {
      supported: false,
      source_name: "vizion-container",
      preview_status: "invalid_reference",
      message: "Container number is missing.",
      events: []
    };
  }

  try {
    const ref = await createReference({ container_number: number });

    const referenceId =
      ref?.referenceId ||
      ref?.reference_id ||
      ref?.id ||
      null;

    if (!referenceId) {
      return {
        supported: true,
        source_name: "vizion-container",
        preview_status: "reference_created_no_id",
        message: "Container lookup started but external source did not return a usable reference ID.",
        events: []
      };
    }

    const updatesPayload = await listReferenceUpdates({ reference_id: referenceId });
    const events = normalizeUpdatesPayload(updatesPayload) || [];

    return {
      supported: true,
      source_name: "vizion-container",
      provider_reference_id: referenceId,
      preview_status: events.length ? "live_updates_found" : "no_updates_yet",
      message: events.length
        ? "Container source returned shipment visibility."
        : "Container source accepted the reference but no live events are available yet.",
      events
    };
  } catch (error) {
    return {
      supported: true,
      source_name: "vizion-container",
      preview_status: "lookup_failed",
      message: error.message || "Container lookup failed.",
      events: []
    };
  }
}

async function lookupBillOfLading(trackingNumber) {
  const number = normalizeTrackingNumber(trackingNumber);

  if (!number) {
    return {
      supported: false,
      source_name: "vizion-bill-of-lading",
      preview_status: "invalid_reference",
      message: "Bill of Lading is missing.",
      events: []
    };
  }

  try {
    const ref = await createReference({ bill_of_lading: number });

    const referenceId =
      ref?.referenceId ||
      ref?.reference_id ||
      ref?.id ||
      null;

    if (!referenceId) {
      return {
        supported: true,
        source_name: "vizion-bill-of-lading",
        preview_status: "reference_created_no_id",
        message: "Bill of Lading lookup started but external source did not return a usable reference ID.",
        events: []
      };
    }

    const updatesPayload = await listReferenceUpdates({ reference_id: referenceId });
    const events = normalizeUpdatesPayload(updatesPayload) || [];

    return {
      supported: true,
      source_name: "vizion-bill-of-lading",
      provider_reference_id: referenceId,
      preview_status: events.length ? "live_updates_found" : "no_updates_yet",
      message: events.length
        ? "Bill of Lading source returned shipment visibility."
        : "Bill of Lading source accepted the reference but no live events are available yet.",
      events
    };
  } catch (error) {
    return {
      supported: true,
      source_name: "vizion-bill-of-lading",
      preview_status: "lookup_failed",
      message: error.message || "Bill of Lading lookup failed.",
      events: []
    };
  }
}

async function lookupPostEms(trackingNumber) {
  const number = normalizeTrackingNumber(trackingNumber);

  if (!number) {
    return {
      supported: false,
      source_name: "demo-post-ems",
      preview_status: "invalid_reference",
      message: "EMS / post tracking number is missing.",
      events: []
    };
  }

  return {
    supported: true,
    source_name: "demo-post-ems",
    preview_status: "live_updates_found",
    message: "Postal source returned shipment updates.",
    events: [
      {
        event_status: "Item Accepted",
        location_name: "Origin Post Office",
        event_time: new Date(Date.now() - 1000 * 60 * 60 * 50).toISOString(),
        remarks: `Postal system accepted ${number}.`,
        source_name: "demo-post-ems"
      },
      {
        event_status: "Item Dispatched",
        location_name: "International Mail Exchange",
        event_time: new Date(Date.now() - 1000 * 60 * 60 * 14).toISOString(),
        remarks: "Item dispatched to destination country.",
        source_name: "demo-post-ems"
      }
    ]
  };
}

function rankSourceResult(result) {
  if (!result) return 0;
  const events = Array.isArray(result.events) ? result.events.length : 0;
  let score = 0;

  if (result.supported) score += 5;
  if (result.preview_status === "live_updates_found") score += 10;
  if (result.provider_reference_id) score += 3;
  score += events;

  return score;
}

function sortEventsNewestFirst(events) {
  return [...events].sort((a, b) => {
    const ta = new Date(a.event_time || 0).getTime();
    const tb = new Date(b.event_time || 0).getTime();
    return tb - ta;
  });
}

async function runMultiSourceTrackingLookup({ mode, trackingNumber }) {
  const normalizedMode = normalizeMode(mode);
  const normalizedNumber = normalizeTrackingNumber(trackingNumber);

  let results = [];

  if (normalizedMode === "container") {
    const oceanResult = await lookupOceanContainer(normalizedNumber);
    results.push(oceanResult);
  } else if (normalizedMode === "bill_of_lading") {
    const blResult = await lookupBillOfLading(normalizedNumber);
    results.push(blResult);
  } else if (normalizedMode === "air_cargo") {
    const airResult = await lookupAirShipment({ trackingNumber: normalizedNumber });
    results.push(airResult);
  } else if (normalizedMode === "post_ems") {
    const postResult = await lookupPostEms(normalizedNumber);
    results.push(postResult);
  } else if (normalizedMode === "express") {
    const expressResult = await lookupExpressShipment({ trackingNumber: normalizedNumber });
    results.push(expressResult);
  } else {
    results = [
      await lookupExpressShipment({ trackingNumber: normalizedNumber }),
      await lookupAirShipment({ trackingNumber: normalizedNumber }),
      await lookupPostEms(normalizedNumber)
    ];
  }

  const validResults = results.filter(Boolean);
  const best = [...validResults].sort((a, b) => rankSourceResult(b) - rankSourceResult(a))[0] || null;

  const mergedEvents = sortEventsNewestFirst(
    validResults.flatMap((item) => Array.isArray(item.events) ? item.events : [])
  );

  return {
    requested_mode: normalizedMode,
    tracking_number: normalizedNumber,
    best_source: best ? {
      source_name: best.source_name || "-",
      preview_status: best.preview_status || "-",
      provider_reference_id: best.provider_reference_id || null,
      message: best.message || "-"
    } : null,
    checked_sources: validResults.map((item) => ({
      source_name: item.source_name || "-",
      supported: Boolean(item.supported),
      preview_status: item.preview_status || "-",
      provider_reference_id: item.provider_reference_id || null,
      events_count: Array.isArray(item.events) ? item.events.length : 0,
      message: item.message || "-"
    })),
    events: mergedEvents
  };
}

function attachMultiSourceTrackingRoutes({ app, requireAuth }) {
  app.post("/api/tracking-sources/check", requireAuth, async (req, res) => {
    try {
      const mode = String(req.body.mode || "").trim();
      const trackingNumber = String(req.body.tracking_number || req.body.number || "").trim();

      if (!mode || !trackingNumber) {
        return res.status(400).json({
          success: false,
          error: "mode and tracking_number are required."
        });
      }

      const result = await runMultiSourceTrackingLookup({
        mode,
        trackingNumber
      });

      return res.json({
        success: true,
        result
      });
    } catch (error) {
      return res.status(500).json({
        success: false,
        error: error.message || "Multi-source lookup failed."
      });
    }
  });

  app.post("/api/customs-live/multi-lookup", requireAuth, async (req, res) => {
    try {
      const mode = String(req.body.mode || "").trim();
      const number = String(req.body.number || "").trim();

      if (!mode || !number) {
        return res.status(400).json({
          success: false,
          error: "mode and number are required."
        });
      }

      const result = await runMultiSourceTrackingLookup({
        mode,
        trackingNumber: number
      });

      return res.json({
        success: true,
        lookup: {
          mode,
          number
        },
        external_truth: result
      });
    } catch (error) {
      return res.status(500).json({
        success: false,
        error: error.message || "Customs multi lookup failed."
      });
    }
  });
}

module.exports = {
  runMultiSourceTrackingLookup,
  attachMultiSourceTrackingRoutes
};