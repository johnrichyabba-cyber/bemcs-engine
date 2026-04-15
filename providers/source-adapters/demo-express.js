"use strict";

function buildExpressDemoEvents(trackingNumber) {
  const number = String(trackingNumber || "").trim().toUpperCase();

  return [
    {
      event_status: "Shipment Information Received",
      location_name: "Origin Processing Center",
      event_time: new Date(Date.now() - 1000 * 60 * 60 * 36).toISOString(),
      remarks: `Express source accepted tracking ${number}.`,
      source_name: "demo-express"
    },
    {
      event_status: "Departed Origin Facility",
      location_name: "Export Dispatch Hub",
      event_time: new Date(Date.now() - 1000 * 60 * 60 * 18).toISOString(),
      remarks: "Shipment departed for line-haul movement.",
      source_name: "demo-express"
    },
    {
      event_status: "In Transit",
      location_name: "Regional Transit Hub",
      event_time: new Date(Date.now() - 1000 * 60 * 60 * 4).toISOString(),
      remarks: "Shipment is moving through the courier network.",
      source_name: "demo-express"
    }
  ];
}

async function lookupExpressShipment({ trackingNumber }) {
  const number = String(trackingNumber || "").trim().toUpperCase();

  if (!number || number.length < 6) {
    return {
      supported: false,
      source_name: "demo-express",
      preview_status: "invalid_reference",
      message: "Tracking number is too short for express source lookup.",
      events: []
    };
  }

  const events = buildExpressDemoEvents(number);

  return {
    supported: true,
    source_name: "demo-express",
    preview_status: "live_updates_found",
    message: "Express source returned shipment updates.",
    events
  };
}

module.exports = {
  lookupExpressShipment
};