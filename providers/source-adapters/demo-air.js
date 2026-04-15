"use strict";

function buildAirDemoEvents(trackingNumber) {
  const number = String(trackingNumber || "").trim().toUpperCase();

  return [
    {
      event_status: "Air Waybill Registered",
      location_name: "Origin Airline Desk",
      event_time: new Date(Date.now() - 1000 * 60 * 60 * 48).toISOString(),
      remarks: `Air cargo reference ${number} registered in external air source.`,
      source_name: "demo-air"
    },
    {
      event_status: "Cargo Received at Terminal",
      location_name: "Export Cargo Terminal",
      event_time: new Date(Date.now() - 1000 * 60 * 60 * 20).toISOString(),
      remarks: "Cargo received and prepared for uplift.",
      source_name: "demo-air"
    },
    {
      event_status: "Flight Departed",
      location_name: "Origin Airport",
      event_time: new Date(Date.now() - 1000 * 60 * 60 * 7).toISOString(),
      remarks: "Cargo departed on scheduled flight.",
      source_name: "demo-air"
    }
  ];
}

async function lookupAirShipment({ trackingNumber }) {
  const number = String(trackingNumber || "").trim().toUpperCase();

  if (!number || number.length < 6) {
    return {
      supported: false,
      source_name: "demo-air",
      preview_status: "invalid_reference",
      message: "Tracking number is too short for air cargo lookup.",
      events: []
    };
  }

  const events = buildAirDemoEvents(number);

  return {
    supported: true,
    source_name: "demo-air",
    preview_status: "live_updates_found",
    message: "Air cargo source returned shipment updates.",
    events
  };
}

module.exports = {
  lookupAirShipment
};