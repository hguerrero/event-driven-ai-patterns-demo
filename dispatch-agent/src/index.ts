import { Kafka, logLevel } from "kafkajs";
import { agent, llmOpenAI } from "volcano-sdk";

// ---------------------------------------------------------------------------
// Config — connects through the single Zion_Mainframe virtual cluster
// ---------------------------------------------------------------------------
const BROKER = process.env.KAFKA_BROKER ?? "localhost:19092";

const INPUT_TOPIC = "agent.orchestration.events";
const ARCHIVE_TOPIC = "zion.archive.log";

const AGENT_NAME = "dispatch-agent";
const CONSUMER_GROUP = "dispatch-response-group";

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
// Dispatch drafting — single LLM step powered by Volcano SDK
// ---------------------------------------------------------------------------
async function draftDispatch(orchestrationEvent: Record<string, unknown>): Promise<string> {
  const results = await agent({ llm, name: "dispatch", hideProgress: true })
    .then({
      prompt: `You are a Dispatch Agent for Zion, independently reacting to an
Agent Smith Protocol orchestration event (one of two agents reacting to this
same event in parallel — you handle operational deployment, not investigation).

Orchestration event:
${JSON.stringify(orchestrationEvent)}

Draft a short operational dispatch order (1-2 sentences), e.g. "Deploying
tactical unit to Sector-7 to stabilize the power grid." Be concrete about
what unit or resource is being deployed and to which sector.

Respond with plain text only, no JSON, no markdown formatting.`,
    })
    .run();

  return results[results.length - 1]?.llmOutput?.trim() ?? "(no response)";
}

// ---------------------------------------------------------------------------
// Main loop — consume orchestration events → draft dispatch → archive
// ---------------------------------------------------------------------------
async function main() {
  console.log("🔴 Dispatch Agent activated (Agent Smith Protocol responder)");
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
        const dispatchOrder = await draftDispatch(event);
        console.log(`\n🟢 Dispatch order:\n${dispatchOrder}\n`);

        const archiveEntry = {
          entry_id: randomId("ZA"),
          agent: AGENT_NAME,
          action: "dispatched",
          ref_id: String(event.event_id ?? "unknown"),
          summary: dispatchOrder,
          timestamp: new Date().toISOString(),
        };

        await producer.send({
          topic: ARCHIVE_TOPIC,
          messages: [{ key: archiveEntry.entry_id, value: JSON.stringify(archiveEntry) }],
        });

        console.log(`✅ Dispatch archived → ${archiveEntry.entry_id}\n`);
      } catch (err) {
        console.error("❌ Dispatch drafting failed:", err);
      }
    },
  });
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
