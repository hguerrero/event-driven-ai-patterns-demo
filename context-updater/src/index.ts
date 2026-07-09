import dns from "node:dns";
import { Kafka, logLevel } from "kafkajs";

// Prefer IPv4 when resolving the Kong Admin API host etc. — see
// anomaly-detector-agent for why (avoids spurious "fetch failed" /
// AggregateError on networks with broken IPv6 routing).
dns.setDefaultResultOrder("ipv4first");

// ---------------------------------------------------------------------------
// Config — connects through the single Zion_Mainframe virtual cluster
// ---------------------------------------------------------------------------
const BROKER = process.env.KAFKA_BROKER ?? "localhost:19092";

const INPUT_TOPIC = "oracle.profile.changes";
const ARCHIVE_TOPIC = "zion.archive.log";

const AGENT_NAME = "context-updater";
const CONSUMER_GROUP = "context-updater-group";

// ---------------------------------------------------------------------------
// AI Gateway RAG Injector — ingest CDC notes into the vector store
// ---------------------------------------------------------------------------
const KONG_ADMIN_URL = process.env.KONG_ADMIN_URL ?? "http://localhost:8001";
const RAG_COLLECTION = process.env.RAG_COLLECTION ?? "oracle-knowledge";

// ---------------------------------------------------------------------------
// Kafka client (KafkaJS) — passthrough, anonymous auth
// ---------------------------------------------------------------------------
const kafka = new Kafka({
  clientId: AGENT_NAME,
  brokers: [BROKER],
  logLevel: logLevel.WARN,
});

const consumer = kafka.consumer({ groupId: CONSUMER_GROUP });
const producer = kafka.producer();

function randomId(prefix: string): string {
  return `${prefix}-${Date.now()}-${Math.floor(Math.random() * 1000)}`;
}

// ---------------------------------------------------------------------------
// RAG Injector — discover plugin ID and ingest CDC change notes
// ---------------------------------------------------------------------------
let ragIngestUrl: string | null = null;

async function discoverRagPluginId(): Promise<string | null> {
  try {
    const res = await fetch(`${KONG_ADMIN_URL}/plugins`);
    if (!res.ok) {
      console.warn(`⚠️  Could not reach Kong Admin API (${res.status}). RAG ingestion disabled.`);
      return null;
    }
    const body = (await res.json()) as { data: Array<{ id: string; name: string }> };
    const plugin = body.data.find((p) => p.name === "ai-rag-injector");
    if (!plugin) {
      console.warn("⚠️  ai-rag-injector plugin not found. RAG ingestion disabled.");
      return null;
    }
    return plugin.id;
  } catch (err) {
    console.warn("⚠️  Kong Admin API unreachable. RAG ingestion disabled.", err);
    return null;
  }
}

async function ingestToRag(change: Record<string, unknown>): Promise<boolean> {
  if (!ragIngestUrl) return false;

  const chunk = {
    content: change.note,
    metadata: {
      collection: RAG_COLLECTION,
      source: AGENT_NAME,
      change_id: change.change_id,
      subject_name: change.subject_name,
      subject_role: change.subject_role,
      updated_field: change.updated_field,
      timestamp: change.timestamp,
    },
  };

  try {
    const res = await fetch(ragIngestUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(chunk),
    });

    if (!res.ok) {
      const text = await res.text();
      console.error(`❌ RAG ingest failed (${res.status}): ${text}`);
      return false;
    }

    console.log(`📦 RAG ingest OK → "${change.note}"`);
    return true;
  } catch (err) {
    console.error("❌ RAG ingest request failed:", err);
    return false;
  }
}

// ---------------------------------------------------------------------------
// Main loop — consume CDC changes → embed into RAG → archive
// ---------------------------------------------------------------------------
async function main() {
  console.log("🔴 Context Updater initializing (The Oracle)...");
  console.log(`   Broker    : ${BROKER}`);
  console.log(`   Group     : ${CONSUMER_GROUP}`);
  console.log(`   Input     : ${INPUT_TOPIC}`);
  console.log(`   Output    : ${ARCHIVE_TOPIC}`);
  console.log(`   Collection: ${RAG_COLLECTION}`);

  // Discover RAG Injector plugin for vector store ingestion
  const pluginId = await discoverRagPluginId();
  if (pluginId) {
    ragIngestUrl = `${KONG_ADMIN_URL}/ai-rag-injector/${pluginId}/ingest_chunk`;
    console.log(`   RAG       : ${ragIngestUrl}`);
  }

  await consumer.connect();
  await producer.connect();
  await consumer.subscribe({ topic: INPUT_TOPIC, fromBeginning: false });

  console.log("🟢 Listening for profile change (CDC) events...\n");

  await consumer.run({
    eachMessage: async ({ message }) => {
      const raw = message.value?.toString();
      if (!raw) return;

      let change: Record<string, unknown>;
      try {
        change = JSON.parse(raw);
      } catch (err) {
        console.error("❌ Failed to parse profile change event:", err);
        return;
      }

      console.log(`⚡ Received CDC event: ${raw}`);

      try {
        const ingested = await ingestToRag(change);

        const archiveEntry = {
          entry_id: randomId("ZA"),
          agent: AGENT_NAME,
          action: "context_updated",
          ref_id: String(change.change_id ?? "unknown"),
          summary: ingested
            ? `Embedded ${change.subject_name}'s ${change.updated_field} change into the ${RAG_COLLECTION} vector store.`
            : `RAG ingestion was skipped or failed for ${change.subject_name}'s ${change.updated_field} change.`,
          timestamp: new Date().toISOString(),
        };

        await producer.send({
          topic: ARCHIVE_TOPIC,
          messages: [{ key: archiveEntry.entry_id, value: JSON.stringify(archiveEntry) }],
        });

        console.log(`✅ Context update archived → ${archiveEntry.entry_id}\n`);
      } catch (err) {
        console.error("❌ Failed to process CDC event:", err);
      }
    },
  });
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
