import dns from "node:dns";
import { Kafka, logLevel } from "kafkajs";
import { agent, llmOpenAI } from "volcano-sdk";

// Prefer IPv4 when resolving api.openai.com etc. — see anomaly-detector-agent
// for why (avoids spurious "fetch failed" / AggregateError on networks with
// broken IPv6 routing).
dns.setDefaultResultOrder("ipv4first");

// ---------------------------------------------------------------------------
// Config — connects through the single Zion_Mainframe virtual cluster
// ---------------------------------------------------------------------------
const BROKER = process.env.KAFKA_BROKER ?? "localhost:19092";

const INPUT_TOPIC = "agent.orchestration.events";
const ARCHIVE_TOPIC = "zion.archive.log";

const AGENT_NAME = "sentinel-agent";
const CONSUMER_GROUP = "sentinel-response-group";

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

function randomId(prefix: string): string {
  return `${prefix}-${Date.now()}-${Math.floor(Math.random() * 1000)}`;
}

// ---------------------------------------------------------------------------
// Investigation — single LLM step powered by Volcano SDK
// ---------------------------------------------------------------------------
async function investigate(orchestrationEvent: Record<string, unknown>): Promise<string> {
  const results = await agent({ llm, name: "sentinel", hideProgress: true })
    .then({
      prompt: `You are a Sentinel Agent for Zion, independently investigating an
Agent Smith Protocol orchestration event (one of two agents reacting to this
same event in parallel).

Orchestration event:
${JSON.stringify(orchestrationEvent)}

Write a short investigation response (2-3 sentences) covering:
1. What is happening
2. Whether it is serious
3. What Zion should do about it

Respond with plain text only, no JSON, no markdown formatting.`,
    })
    .run();

  return results[results.length - 1]?.llmOutput?.trim() ?? "(no response)";
}

// ---------------------------------------------------------------------------
// Main loop — consume orchestration events → investigate → archive
// ---------------------------------------------------------------------------
async function main() {
  console.log("🔴 Sentinel Agent activated (Agent Smith Protocol responder)");
  console.log(`   Broker : ${BROKER}`);
  console.log(`   Group  : ${CONSUMER_GROUP}`);
  console.log(`   Input  : ${INPUT_TOPIC}`);
  console.log(`   Output : ${ARCHIVE_TOPIC}\n`);

  await consumer.connect();
  await producer.connect();
  await consumer.subscribe({ topic: INPUT_TOPIC, fromBeginning: false });

  console.log("🟢 Listening for orchestration events...\n");

  await consumer.run({
    eachMessage: async ({ message }) => {
      const raw = message.value?.toString();
      if (!raw) return;

      let event: Record<string, unknown>;
      try {
        event = JSON.parse(raw);
      } catch (err) {
        console.error("❌ Failed to parse orchestration event:", err);
        return;
      }

      console.log(`⚡ Received orchestration event: ${raw}`);

      try {
        const report = await investigate(event);
        console.log(`\n🟢 Investigation:\n${report}\n`);

        const archiveEntry = {
          entry_id: randomId("ZA"),
          agent: AGENT_NAME,
          action: "investigated",
          ref_id: String(event.event_id ?? "unknown"),
          summary: report,
          timestamp: new Date().toISOString(),
        };

        await producer.send({
          topic: ARCHIVE_TOPIC,
          messages: [{ key: archiveEntry.entry_id, value: JSON.stringify(archiveEntry) }],
        });

        console.log(`✅ Investigation archived → ${archiveEntry.entry_id}\n`);
      } catch (err) {
        console.error("❌ Sentinel investigation failed:", err);
      }
    },
  });
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
