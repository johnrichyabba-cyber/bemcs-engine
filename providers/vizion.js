"use strict";

/**
 * Vizion provider wrapper
 *
 * Supported capabilities:
 * - createReference()
 * - listReferenceUpdates()
 * - getReference()
 * - deactivateReference()
 * - normalizeUpdatesPayload()
 *
 * Env vars:
 * - VIZION_API_KEY
 * - VIZION_API_BASE_URL   (optional, defaults to prod)
 * - APP_BASE_URL          (optional, used for callback_url)
 * - VIZION_WEBHOOK_SECRET (optional, appended to callback URL as query param)
 */

const DEFAULT_BASE_URL = "https://prod.vizionapi.com";

function getConfig() {
  const apiKey = String(process.env.VIZION_API_KEY || "").trim();
  const baseUrl = String(process.env.VIZION_API_BASE_URL || DEFAULT_BASE_URL).trim().replace(/\/+$/, "");
  const appBaseUrl = String(process.env.APP_BASE_URL || "").trim().replace(/\/+$/, "");
  const webhookSecret = String(process.env.VIZION_WEBHOOK_SECRET || "").trim();

  return {
    apiKey,
    baseUrl,
    appBaseUrl,
    webhookSecret
  };
}

function assertConfigured() {
  const { apiKey } = getConfig();

  if (!apiKey) {
    throw new Error("VIZION_API_KEY is missing. Set it in your environment before using live provider tracking.");
  }
}

function buildHeaders(extra = {}) {
  const { apiKey } = getConfig();

  return {
    "Content-Type": "application/json",
    "X-API-Key": apiKey,
    ...extra
  };
}

async function request(method, endpoint, body) {
  assertConfigured();

  const { baseUrl } = getConfig();
  const url = `${baseUrl}${endpoint}`;

  const options = {
    method,
    headers: buildHeaders()
  };

  if (body !== undefined) {
    options.body = JSON.stringify(body);
  }

  const response = await fetch(url, options);
  const text = await response.text();

  let data = null;
  try {
    data = text ? JSON.parse(text) : null;
  } catch (_error) {
    data = { raw: text };
  }

  if (!response.ok) {
    const message =
      data?.message ||
      data?.error ||
      data?.raw ||
      `Vizion request failed with status ${response.status}`;
    throw new Error(message);
  }

  return data;
}

function buildCallbackUrl() {
  const { appBaseUrl, webhookSecret } = getConfig();

  if (!appBaseUrl) return undefined;

  const url = new URL(`${appBaseUrl}/api/webhooks/vizion`);
  if (webhookSecret) {
    url.searchParams.set("secret", webhookSecret);
  }

  return url.toString();
}

function chooseReferenceBody(payload = {}) {
  const providerName = String(payload.provider_name || "vizion").trim().toLowerCase();
  const carrierCode = String(payload.carrier_code || "").trim() || undefined;
  const billOfLading = String(payload.bill_of_lading || "").trim() || undefined;
  const containerNumber =
    String(payload.container_id || payload.container_number || "").trim() || undefined;
  const bookingNumber = String(payload.booking_number || "").trim() || undefined;
  const callbackUrl =
    String(payload.callback_url || "").trim() || buildCallbackUrl() || undefined;

  const body = {};

  if (containerNumber) {
    body.container_id = containerNumber;
  }

  if (billOfLading) {
    body.bill_of_lading = billOfLading;
  }

  if (bookingNumber) {
    body.booking_number = bookingNumber;
  }

  if (carrierCode) {
    body.carrier_code = carrierCode;
  }

  if (callbackUrl) {
    body.callback_url = callbackUrl;
  }

  if (!body.container_id && !body.bill_of_lading && !body.booking_number) {
    throw new Error("Vizion createReference requires one of: container_number, bill_of_lading, or booking_number.");
  }

  return {
    provider_name: providerName,
    body
  };
}

function normalizeCreateResponse(data) {
  if (!data) return null;

  return {
    id: data.id || data.reference_id || data.uuid || null,
    referenceId: data.reference_id || data.id || data.uuid || null,
    reference_id: data.reference_id || data.id || data.uuid || null,
    status:
      data.last_update_status ||
      data.status ||
      data.provider_status ||
      "registered",
    raw: data
  };
}

async function createReference(payload = {}) {
  const { body } = chooseReferenceBody(payload);
  const data = await request("POST", "/references", body);
  return normalizeCreateResponse(data);
}

async function getReference(referenceIdInput) {
  const referenceId =
    String(
      referenceIdInput?.reference_id ||
      referenceIdInput?.external_reference_id ||
      referenceIdInput?.referenceId ||
      referenceIdInput?.id ||
      referenceIdInput ||
      ""
    ).trim();

  if (!referenceId) {
    throw new Error("Vizion getReference requires a reference id.");
  }

  const data = await request("GET", `/references/${encodeURIComponent(referenceId)}`);
  return data;
}

async function listReferenceUpdates(referenceIdInput) {
  const referenceId =
    String(
      referenceIdInput?.reference_id ||
      referenceIdInput?.external_reference_id ||
      referenceIdInput?.referenceId ||
      referenceIdInput?.id ||
      referenceIdInput ||
      ""
    ).trim();

  if (!referenceId) {
    throw new Error("Vizion listReferenceUpdates requires a reference id.");
  }

  const data = await request(
    "GET",
    `/references/${encodeURIComponent(referenceId)}/updates`
  );

  return data;
}

async function deactivateReference(referenceIdInput) {
  const referenceId =
    String(
      referenceIdInput?.reference_id ||
      referenceIdInput?.external_reference_id ||
      referenceIdInput?.referenceId ||
      referenceIdInput?.id ||
      referenceIdInput ||
      ""
    ).trim();

  if (!referenceId) {
    throw new Error("Vizion deactivateReference requires a reference id.");
  }

  return request("DELETE", `/references/${encodeURIComponent(referenceId)}`);
}

function readNested(obj, paths = []) {
  for (const path of paths) {
    const value = path.split(".").reduce((acc, key) => acc?.[key], obj);
    if (value !== undefined && value !== null && value !== "") {
      return value;
    }
  }
  return null;
}

function toArrayPayload(payload) {
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload?.updates)) return payload.updates;
  if (Array.isArray(payload?.data)) return payload.data;
  if (Array.isArray(payload?.results)) return payload.results;
  if (Array.isArray(payload?.items)) return payload.items;
  if (payload?.update) return [payload.update];
  return [];
}

function normalizeMilestone(update = {}) {
  const eventStatus =
    readNested(update, [
      "event_status",
      "status",
      "milestone_name",
      "milestone.name",
      "milestone",
      "description",
      "title",
      "message"
    ]) || "Provider Update";

  const eventTime =
    readNested(update, [
      "event_time",
      "timestamp",
      "created_at",
      "occurred_at",
      "event_at",
      "time",
      "date",
      "milestone_time",
      "milestone.timestamp"
    ]) || new Date().toISOString();

  const locationName =
    readNested(update, [
      "location_name",
      "location.name",
      "location",
      "port_name",
      "port",
      "city",
      "place"
    ]) || null;

  const remarks =
    readNested(update, [
      "remarks",
      "details",
      "description",
      "message",
      "event_description"
    ]) || null;

  const sourceName =
    readNested(update, [
      "source_name",
      "provider_name"
    ]) || "vizion";

  return {
    event_status: String(eventStatus),
    event_time: eventTime,
    location_name: locationName ? String(locationName) : null,
    remarks: remarks ? String(remarks) : null,
    source_name: String(sourceName)
  };
}

function normalizeUpdatesPayload(payload) {
  const updates = toArrayPayload(payload);
  return updates.map(normalizeMilestone);
}

module.exports = {
  createReference,
  getReference,
  listReferenceUpdates,
  deactivateReference,
  normalizeUpdatesPayload
};