import dns from "node:dns";
import { Kafka, logLevel } from "kafkajs";
import { agent, llmOpenAI } from "volcano-sdk";

// Prefer IPv4 when resolving api.openai.com etc. Some networks (corporate
// Wi-Fi/VPN in particular) advertise broken/unreachable IPv6 routes; Node's
// undici-based fetch then fails with a generic "fetch failed" wrapping an
// AggregateError instead of falling back to IPv4 cleanly. This is a no-op
// on networks where IPv6 works fine.
dns.setDefaultResultOrder("ipv4first");

// ---------------------------------------------------------------------------
// Config — connects through the single Zion_Mainframe virtual cluster
// ---------------------------------------------------------------------------
const BROKER = process.env.KAFKA_BROKER ?? "localhost:19092";

const INPUT_TOPIC = "sentinel.readings";
const ORCHESTRATION_TOPIC = "agent.orchestration.events";
const ARCHIVE_TOPIC = "zion.archive.log";

const AGENT_NAME = "anomaly-detector-agent";
const CONSUMER_GROUP = "anomaly-detector-group";

// ---------------------------------------------------------------------------
// LLM — uses OpenAI via Volcano SDK (set OPENAI_API_KEY env var)
// ---------------------------------------------------------------------------
const llm = llmOpenAI({
  apiKey: process.env.OPENAI_API_KEY!,
  model: process.env.OPENAI_MODEL ?? "gpt-4o-mini",
});

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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function randomId(prefix: string): string {
  return `${prefix}-${Date.now()}-${Math.floor(Math.random() * 1000)}`;
}

// ---------------------------------------------------------------------------
// Anomaly analysis — single LLM step powered by Volcano SDK, strict JSON out
// ---------------------------------------------------------------------------
interface AnomalyJudgement {
  is_anomaly: boolean;
  severity: "low" | "medium" | "high" | "critical";
  anomaly_summary: string;
  recommended_action: string;
  archive_summary: string;
}

async function analyzeReading(reading: Record<string, unknown>): Promise<AnomalyJudgement> {
  const results = await agent({ llm, hideProgress: true })
    .then({
      prompt: `You are a Matrix anomaly-detection sentinel monitoring sector stability
telemetry. Analyze the following sentinel reading and determine if it represents
an anomaly worth escalating to the Agent Smith Protocol (a fan-out to independent
responder agents).

Reading payload:
${JSON.stringify(reading)}

A reading with status STABLE and a high stability_index is normal. A reading with
status FLUCTUATING or GLITCH and a low stability_index is likely an anomaly.

Respond STRICTLY as JSON with these fields and nothing else:
{
  "is_anomaly": boolean,
  "severity": "low" | "medium" | "high" | "critical",
  "anomaly_summary": "<one-sentence summary of what is happening>",
  "recommended_action": "<what responding agents should do next>",
  "archive_summary": "<one human-readable sentence describing that this reading was evaluated, for an audit log>"
}`,
    })
    .run();

  const llmOutput = results[results.length - 1]?.llmOutput ?? "{}";
  const cleaned = llmOutput.replace(/```json\n?/g, "").replace(/```/g, "").trim();
  return JSON.parse(cleaned) as AnomalyJudgement;
}

// ---------------------------------------------------------------------------
// Main loop — consume → analyze → fan-out (orchestration) → archive
// ---------------------------------------------------------------------------
async function main() {
  console.log("🔴 Anomaly Detector Agent initializing (Agent Smith Protocol)...");
  console.log(`   Broker : ${BROKER}`);
  console.log(`   Group  : ${CONSUMER_GROUP}`);
  console.log(`   Input  : ${INPUT_TOPIC}`);
  console.log(`   Outputs: ${ORCHESTRATION_TOPIC}, ${ARCHIVE_TOPIC}`);

  await consumer.connect();
  await producer.connect();
  await consumer.subscribe({ topic: INPUT_TOPIC, fromBeginning: false });

  console.log("🟢 Listening for sentinel readings...\n");

  await consumer.run({
    eachMessage: async ({ message }) => {
      const raw = message.value?.toString();
      if (!raw) return;

      let reading: Record<string, unknown>;
      try {
        reading = JSON.parse(raw);
      } catch (err) {
        console.error("❌ Failed to parse sentinel reading:", err);
        return;
      }

      console.log(`⚡ Received reading: ${raw}`);

      try {
        const judgement = await analyzeReading(reading);
        const readingId = String(reading.reading_id ?? "unknown");
        const now = new Date().toISOString();

        if (judgement.is_anomaly) {
          const orchestrationEvent = {
            event_id: randomId("AO"),
            source: AGENT_NAME,
            severity: judgement.severity,
            sector: reading.sector,
            anomaly_summary: judgement.anomaly_summary,
            recommended_action: judgement.recommended_action,
            original_reading: reading,
            timestamp: now,
          };

          await producer.send({
            topic: ORCHESTRATION_TOPIC,
            messages: [{ key: orchestrationEvent.event_id, value: JSON.stringify(orchestrationEvent) }],
          });

          console.log(`🕶️  Agent Smith Protocol triggered → ${orchestrationEvent.event_id} (${judgement.severity})`);
        }

        const archiveEntry = {
          entry_id: randomId("ZA"),
          agent: AGENT_NAME,
          action: judgement.is_anomaly ? "flagged" : "evaluated",
          ref_id: readingId,
          summary: judgement.archive_summary ?? judgement.anomaly_summary,
          timestamp: now,
        };

        await producer.send({
          topic: ARCHIVE_TOPIC,
          messages: [{ key: archiveEntry.entry_id, value: JSON.stringify(archiveEntry) }],
        });

        console.log(`✅ Processed ${readingId} → ${archiveEntry.action}\n`);
      } catch (err) {
        console.error("❌ Failed to process reading:", err);
      }
    },
  });
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
