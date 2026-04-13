const crypto = require("crypto");

const VIZION_BASE_URL = "https://api.vizionapi.com";

function getHeaders() {
  const apiKey = process.env.VIZION_API_KEY;
  if (!apiKey) {
    throw new Error("VIZION_API_KEY is missing.");
  }

  return {
    "Content-Type": "application/json",
    "X-API-Key": apiKey
  };
}

async function vizionFetch(path, options = {}) {
  const response = await fetch(`${VIZION_BASE_URL}${path}`, {
    ...options,
    headers: {
      ...getHeaders(),
      ...(options.headers || {})
    }
  });

  const text = await response.text();
  let data = null;

  try {
    data = text ? JSON.parse(text) : null;
  } catch (_error) {
    data = { raw: text };
  }

  if (!response.ok) {
    const message =
      (data && (data.message || data.error || data.detail)) ||
      `Vizion request failed with status ${response.status}`;
    const err = new Error(message);
    err.status = response.status;
    err.payload = data;
    throw err;
  }

  return data;
}

function buildCallbackUrl(shipmentId) {
  const baseUrl = String(process.env.APP_BASE_URL || "").trim();
  const secret = String(process.env.VIZION_WEBHOOK_SECRET || "").trim();

  if (!baseUrl) {
    throw new Error("APP_BASE_URL is missing.");
  }

  if (!secret) {
    throw new Error("VIZION_WEBHOOK_SECRET is missing.");
  }

  const normalizedBase = baseUrl.replace(/\/+$/, "");
  return `${normalizedBase}/webhooks/vizion/${shipmentId}?token=${encodeURIComponent(secret)}`;
}

function decideReferenceInput(source) {
  const containerId = String(source.container_number || "").trim();
  const billOfLading = String(source.bill_of_lading || "").trim();
  const bookingNumber = String(source.booking_number || "").trim();
  const carrierCode = String(source.carrier_code || "").trim();

  if (containerId) {
    return {
      container_id: containerId,
      carrier_code: carrierCode || undefined
    };
  }

  if (billOfLading) {
    return {
      bill_of_lading: billOfLading,
      carrier_code: carrierCode || undefined
    };
  }

  if (bookingNumber) {
    return {
      booking_number: bookingNumber,
      carrier_code: carrierCode || undefined
    };
  }

  throw new Error("No valid Vizion tracking identifier found. Add container number, bill of lading, or booking number.");
}

async function createReference({ shipmentId, source }) {
  const body = {
    ...decideReferenceInput(source),
    callback_url: buildCallbackUrl(shipmentId)
  };

  const response = await vizionFetch("/references", {
    method: "POST",
    body: JSON.stringify(body)
  });

  return {
    request_body: body,
    response_body: response
  };
}

async function listReferenceUpdates(referenceId) {
  if (!referenceId) {
    throw new Error("referenceId is required.");
  }

  const response = await vizionFetch(`/references/${encodeURIComponent(referenceId)}/updates`, {
    method: "GET"
  });

  return response;
}

function normalizeEvent(update) {
  const eventName =
    update.standardized_event ||
    update.standardized_milestone ||
    update.milestone ||
    update.event_name ||
    update.status ||
    "Tracking Update";

  const locationName =
    update.location_name ||
    update.location?.name ||
    update.location?.city ||
    update.location?.port_name ||
    update.location?.unlocode ||
    "Unknown";

  const eventTime =
    update.event_time ||
    update.occurred_at ||
    update.timestamp ||
    update.created_at ||
    new Date().toISOString();

  const remarksParts = [];

  if (update.description) remarksParts.push(update.description);
  if (update.mode) remarksParts.push(`Mode: ${update.mode}`);
  if (update.vessel_name) remarksParts.push(`Vessel: ${update.vessel_name}`);
  if (update.voyage_number) remarksParts.push(`Voyage: ${update.voyage_number}`);

  return {
    external_event_id:
      update.update_id ||
      update.event_id ||
      crypto.createHash("sha256").update(JSON.stringify(update)).digest("hex"),
    source_status: eventName,
    event_status: eventName,
    location_name: locationName,
    remarks: remarksParts.join(" | "),
    event_time: eventTime,
    raw_payload: update
  };
}

function normalizeUpdatesPayload(payload) {
  const updates = Array.isArray(payload)
    ? payload
    : Array.isArray(payload?.updates)
      ? payload.updates
      : Array.isArray(payload?.data)
        ? payload.data
        : [];

  return updates.map(normalizeEvent);
}

module.exports = {
  createReference,
  listReferenceUpdates,
  normalizeUpdatesPayload
};