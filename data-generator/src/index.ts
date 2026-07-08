import { Kafka, logLevel } from "kafkajs";

// ---------------------------------------------------------------------------
// Config — single KEG virtual cluster (Zion_Mainframe), two topics
// ---------------------------------------------------------------------------
const BROKER = process.env.KAFKA_BROKER ?? "localhost:19092";
const SENTINEL_INTERVAL_MS = parseInt(process.env.SENTINEL_INTERVAL_MS ?? "3000", 10);
const ORACLE_INTERVAL_MS = parseInt(process.env.ORACLE_INTERVAL_MS ?? "5000", 10);

const SENTINEL_TOPIC = "sentinel.readings";
const ORACLE_TOPIC = "oracle.profile.changes";

// ---------------------------------------------------------------------------
// Kafka client — single virtual cluster, passthrough mode
// ---------------------------------------------------------------------------
const kafka = new Kafka({
  clientId: "data-generator",
  brokers: [BROKER],
  logLevel: logLevel.WARN,
});

const producer = kafka.producer();

// ---------------------------------------------------------------------------
// sentinel.readings — sector stability telemetry ("Agent Smith Protocol" source)
// ---------------------------------------------------------------------------
const SECTORS = [
  "Sector-7", "Downtown-Grid", "Subway-Loop-9",
  "Power-Nexus", "Residential-Block-12", "Construct-Node-3",
];

let sentinelCount = 0;

function randomSentinelReading() {
  sentinelCount++;
  const sector = SECTORS[Math.floor(Math.random() * SECTORS.length)];

  // Roughly 1-in-4 readings are anomalous so demos reliably see anomalies
  // within ~15-20s at the default 3s interval.
  const isAnomaly = sentinelCount % 4 === 0 || Math.random() < 0.1;

  let stability_index: number;
  let status: "STABLE" | "FLUCTUATING" | "GLITCH";

  if (isAnomaly) {
    stability_index = parseFloat((Math.random() * 30).toFixed(1)); // 0-30
    status = Math.random() < 0.5 ? "GLITCH" : "FLUCTUATING";
  } else {
    stability_index = parseFloat((60 + Math.random() * 40).toFixed(1)); // 60-100
    status = "STABLE";
  }

  return {
    reading_id: `SR-${Date.now()}-${Math.floor(Math.random() * 1000)}`,
    sector,
    stability_index,
    status,
    timestamp: new Date().toISOString(),
  };
}

// ---------------------------------------------------------------------------
// oracle.profile.changes — simulated CDC stream ("The Oracle")
// ---------------------------------------------------------------------------
const SUBJECTS: Array<{ name: string; role: "Operator" | "Redpill" | "Bluepill" | "Unknown" }> = [
  { name: "Trinity", role: "Operator" },
  { name: "Cypher", role: "Redpill" },
  { name: "Niobe", role: "Operator" },
  { name: "Link", role: "Operator" },
  { name: "Switch", role: "Redpill" },
  { name: "Tank", role: "Operator" },
  { name: "Dozer", role: "Operator" },
  { name: "Mouse", role: "Redpill" },
  { name: "Apoc", role: "Redpill" },
];

const LOCATIONS = [
  "Zion-Command-Deck", "The-Nebuchadnezzar", "Machine-City-Outskirts",
  "Sector-7-Safehouse", "The-Matrix-Construct-Loading-Program", "Zion-Docking-Bay-3",
];

const CLEARANCE_LEVELS = ["Bluepill-Standard", "Redpill-Field", "Zion-Command", "Councilor-Only"];

type UpdatedField = "status" | "location" | "clearance_level" | "last_seen";

function randomProfileChange() {
  const subject = SUBJECTS[Math.floor(Math.random() * SUBJECTS.length)];
  const op: "INSERT" | "UPDATE" | "DELETE" =
    Math.random() < 0.05 ? "DELETE" : Math.random() < 0.15 ? "INSERT" : "UPDATE";

  const fields: UpdatedField[] = ["status", "location", "clearance_level", "last_seen"];
  const updated_field = fields[Math.floor(Math.random() * fields.length)];

  let updated_value: string;
  let note: string;

  switch (updated_field) {
    case "status": {
      const statusOptions = [
        { value: "awakened", note: `${subject.name} was just awakened from the Matrix and is being onboarded aboard Zion's fleet.` },
        { value: "flagged_rogue", note: `${subject.name} was flagged rogue after intercepted comms revealed unauthorized contact with an Agent.` },
        { value: "reinserted", note: `${subject.name} was reinserted into the Matrix for a deep-cover reconnaissance operation.` },
        { value: "missing", note: `${subject.name} has been marked missing after losing hardline contact during a routine extraction.` },
        { value: "active", note: `${subject.name} is confirmed active and back on active duty following a medical clearance.` },
      ];
      const pick = statusOptions[Math.floor(Math.random() * statusOptions.length)];
      updated_value = pick.value;
      note = pick.note;
      break;
    }
    case "location": {
      const location = LOCATIONS[Math.floor(Math.random() * LOCATIONS.length)];
      updated_value = location;
      note = `${subject.name}'s last known location was updated to ${location.replace(/-/g, " ")}.`;
      break;
    }
    case "clearance_level": {
      const clearance = CLEARANCE_LEVELS[Math.floor(Math.random() * CLEARANCE_LEVELS.length)];
      updated_value = clearance;
      note = `${subject.name}'s clearance level was updated to ${clearance.replace(/-/g, " ")} following a review by Zion Command.`;
      break;
    }
    case "last_seen": {
      const when = new Date(Date.now() - Math.floor(Math.random() * 3600_000)).toISOString();
      updated_value = when;
      note = `${subject.name} was last seen at ${when}, reported by a fellow crew member during a routine check-in.`;
      break;
    }
  }

  if (op === "INSERT") {
    note = `${subject.name} was newly registered in the Zion database as a ${subject.role.toLowerCase()}, with ${updated_field.replace(/_/g, " ")} set to ${updated_value}.`;
  } else if (op === "DELETE") {
    note = `${subject.name}'s profile record was purged from the Zion database; last known ${updated_field.replace(/_/g, " ")} was ${updated_value}.`;
  }

  return {
    change_id: `PC-${Date.now()}-${Math.floor(Math.random() * 1000)}`,
    op,
    subject_name: subject.name,
    subject_role: subject.role,
    updated_field,
    updated_value,
    note,
    timestamp: new Date().toISOString(),
  };
}

// ---------------------------------------------------------------------------
// Main loop — produce to both topics on independent timers
// ---------------------------------------------------------------------------
async function main() {
  console.log("🔴 Data Generator initializing...");
  console.log(`   Broker            : ${BROKER}`);
  console.log(`   Sentinel readings : ${SENTINEL_TOPIC} every ${SENTINEL_INTERVAL_MS}ms`);
  console.log(`   Oracle CDC        : ${ORACLE_TOPIC} every ${ORACLE_INTERVAL_MS}ms\n`);

  await producer.connect();
  console.log("🟢 Connected to Zion_Mainframe virtual cluster. Generating events...\n");

  let sentinelSeq = 0;
  setInterval(async () => {
    sentinelSeq++;
    try {
      const reading = randomSentinelReading();
      await producer.send({
        topic: SENTINEL_TOPIC,
        messages: [{ key: reading.reading_id, value: JSON.stringify(reading) }],
      });
      const marker = reading.status === "STABLE" ? "🟢" : "🚨";
      console.log(`#${sentinelSeq} ${marker} ${reading.sector} → ${reading.status} (stability ${reading.stability_index})`);
    } catch (err) {
      console.error("❌ Error producing sentinel reading:", err);
    }
  }, SENTINEL_INTERVAL_MS);

  let oracleSeq = 0;
  setInterval(async () => {
    oracleSeq++;
    try {
      const change = randomProfileChange();
      await producer.send({
        topic: ORACLE_TOPIC,
        messages: [{ key: change.change_id, value: JSON.stringify(change) }],
      });
      console.log(`#${oracleSeq} 📇 [${change.op}] ${change.subject_name} (${change.subject_role}) → ${change.updated_field}=${change.updated_value}`);
    } catch (err) {
      console.error("❌ Error producing profile change:", err);
    }
  }, ORACLE_INTERVAL_MS);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
