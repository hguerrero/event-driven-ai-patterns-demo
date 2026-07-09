require("dotenv").config();

const dns = require("node:dns");
// Prefer IPv4 when resolving external hosts (Kong AI Gateway is local, but
// this also protects any future outbound calls) — avoids spurious
// "fetch failed" / AggregateError on networks with broken IPv6 routing.
dns.setDefaultResultOrder("ipv4first");

const express = require("express");
const cors = require("cors");
const WebSocket = require("ws");
const http = require("http");
const path = require("path");
const { spawn } = require("child_process");
const { Kafka, logLevel } = require("kafkajs");

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(cors());
app.use(express.json());

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const KAFKA_BROKER = process.env.KAFKA_BROKER || "localhost:19092";
const KONG_AI_GATEWAY_URL = process.env.KONG_AI_GATEWAY_URL || "http://localhost:8000";
const SERVER_PORT = process.env.SERVER_PORT || 3001;

const TOPICS = {
  SENTINEL_READINGS: "sentinel.readings",
  ORCHESTRATION_EVENTS: "agent.orchestration.events",
  PROFILE_CHANGES: "oracle.profile.changes",
  ARCHIVE_LOG: "zion.archive.log",
};

const RING_BUFFER_SIZE = 200;

// ---------------------------------------------------------------------------
// In-memory ring buffers (per topic) so freshly-loaded clients get history
// ---------------------------------------------------------------------------

const buffers = {
  sentinelReadings: [],
  orchestrationEvents: [],
  profileChanges: [],
  archiveEntries: [],
};

function pushToBuffer(bufferName, item) {
  const buf = buffers[bufferName];
  buf.unshift(item);
  if (buf.length > RING_BUFFER_SIZE) buf.length = RING_BUFFER_SIZE;
}

// ---------------------------------------------------------------------------
// WebSocket broadcast helpers
// ---------------------------------------------------------------------------

function broadcast(type, data) {
  const payload = JSON.stringify({ type, data });
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(payload);
    }
  });
}

function buildInitPayload() {
  return {
    orchestrationEvents: [...buffers.orchestrationEvents],
    profileChanges: [...buffers.profileChanges],
    archiveEntries: [...buffers.archiveEntries],
    services: buildServicesStatusPayload(),
  };
}

wss.on("connection", (ws) => {
  console.log("[ws] client connected");
  ws.send(JSON.stringify({ type: "init", data: buildInitPayload() }));

  ws.on("close", () => {
    console.log("[ws] client disconnected");
  });

  ws.on("error", (err) => {
    console.error("[ws] client error:", err.message);
  });
});

// ---------------------------------------------------------------------------
// Kafka consumer (defensive: retries with backoff, never crashes the process)
// ---------------------------------------------------------------------------

const kafka = new Kafka({
  clientId: "matrix-ui-dashboard",
  brokers: [KAFKA_BROKER],
  logLevel: logLevel.NOTHING,
  retry: {
    initialRetryTime: 500,
    retries: 3,
  },
});

let kafkaConnected = false;
let consumerStartAttempts = 0;

function setKafkaConnected(connected) {
  if (kafkaConnected === connected) return;
  kafkaConnected = connected;
  broadcast("services_status", buildServicesStatusPayload());
}

function safeParseJson(buffer) {
  if (!buffer) return null;
  try {
    return JSON.parse(buffer.toString());
  } catch (err) {
    console.warn("[kafka] failed to parse message JSON:", err.message);
    return null;
  }
}

async function startDashboardConsumer() {
  const consumer = kafka.consumer({ groupId: "matrix-ui-dashboard" });

  const connectWithRetry = async () => {
    while (true) {
      try {
        await consumer.connect();
        console.log(`[kafka] connected to ${KAFKA_BROKER}`);
        setKafkaConnected(true);
        return;
      } catch (err) {
        consumerStartAttempts += 1;
        const delay = Math.min(2000 * consumerStartAttempts, 30000);
        console.warn(
          `[kafka] connection failed (attempt ${consumerStartAttempts}): ${err.message}. Retrying in ${delay}ms...`
        );
        setKafkaConnected(false);
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }
  };

  await connectWithRetry();

  try {
    await consumer.subscribe({ topic: TOPICS.SENTINEL_READINGS, fromBeginning: false });
    await consumer.subscribe({ topic: TOPICS.ORCHESTRATION_EVENTS, fromBeginning: false });
    await consumer.subscribe({ topic: TOPICS.PROFILE_CHANGES, fromBeginning: false });
    await consumer.subscribe({ topic: TOPICS.ARCHIVE_LOG, fromBeginning: false });
  } catch (err) {
    console.error("[kafka] subscribe failed:", err.message);
    setKafkaConnected(false);
    // Retry the whole setup after a delay rather than crashing.
    setTimeout(startDashboardConsumer, 5000);
    return;
  }

  try {
    await consumer.run({
      eachMessage: async ({ topic, message }) => {
        const value = safeParseJson(message.value);
        if (!value) return;

        switch (topic) {
          case TOPICS.SENTINEL_READINGS:
            pushToBuffer("sentinelReadings", value);
            broadcast("sentinel_reading", value);
            break;
          case TOPICS.ORCHESTRATION_EVENTS:
            pushToBuffer("orchestrationEvents", value);
            broadcast("orchestration_event", value);
            break;
          case TOPICS.PROFILE_CHANGES:
            pushToBuffer("profileChanges", value);
            broadcast("profile_change", value);
            break;
          case TOPICS.ARCHIVE_LOG:
            pushToBuffer("archiveEntries", value);
            broadcast("archive_entry", value);
            break;
          default:
            break;
        }
      },
    });
  } catch (err) {
    console.error("[kafka] consumer run error:", err.message);
    setKafkaConnected(false);
    setTimeout(startDashboardConsumer, 5000);
  }
}

// Kick off the consumer, but never let a Kafka outage crash the server.
startDashboardConsumer().catch((err) => {
  console.error("[kafka] fatal consumer setup error (will not crash server):", err.message);
  setKafkaConnected(false);
});

process.on("unhandledRejection", (reason) => {
  console.error("[process] unhandled rejection (ignored to keep server alive):", reason);
});

// ---------------------------------------------------------------------------
// Child-process management for the 5 sibling backend services
// ---------------------------------------------------------------------------

const SERVICE_NAMES = [
  "data-generator",
  "anomaly-detector-agent",
  "sentinel-agent",
  "dispatch-agent",
  "context-updater",
];

const MAX_SERVICE_LOG_LINES = 200;

// name -> { process, status, startTime, pid, log: string[] }
const services = {};
SERVICE_NAMES.forEach((name) => {
  services[name] = {
    process: null,
    status: "stopped",
    startTime: null,
    pid: null,
    log: [],
  };
});

function appendServiceLog(name, line) {
  const svc = services[name];
  if (!svc) return;
  svc.log.push(line);
  if (svc.log.length > MAX_SERVICE_LOG_LINES) svc.log.shift();
}

function isServiceActuallyRunning(name) {
  const svc = services[name];
  return !!(svc.process && svc.process.exitCode === null && !svc.process.killed);
}

function buildServicesStatusPayload() {
  return {
    kafka: {
      connected: kafkaConnected,
      brokers: [KAFKA_BROKER],
    },
    services: SERVICE_NAMES.map((name) => {
      const svc = services[name];
      const running = isServiceActuallyRunning(name);
      // Reconcile stored status with reality.
      if (!running && (svc.status === "running" || svc.status === "starting")) {
        svc.status = "stopped";
        svc.process = null;
        svc.pid = null;
      }
      return {
        name,
        status: svc.status,
        pid: running ? svc.pid : null,
        uptimeMs: running && svc.startTime ? Date.now() - svc.startTime : 0,
      };
    }),
  };
}

function startServiceProcess(name) {
  const svc = services[name];
  if (isServiceActuallyRunning(name)) {
    return { alreadyRunning: true };
  }

  const servicePath = path.join(__dirname, "..", "..", name);

  let child;
  try {
    child = spawn("npm", ["start"], {
      cwd: servicePath,
      stdio: ["ignore", "pipe", "pipe"],
      env: { ...process.env },
    });
  } catch (err) {
    console.error(`[services] failed to spawn ${name}:`, err.message);
    svc.status = "error";
    appendServiceLog(name, `[spawn-error] ${err.message}`);
    broadcast("services_status", buildServicesStatusPayload());
    return { error: err.message };
  }

  svc.process = child;
  svc.pid = child.pid;
  svc.startTime = Date.now();
  svc.status = "starting";

  child.stdout.on("data", (data) => {
    const text = data.toString();
    appendServiceLog(name, text);
    console.log(`[${name}] ${text.trim()}`);
  });

  child.stderr.on("data", (data) => {
    const text = data.toString();
    appendServiceLog(name, `[stderr] ${text}`);
    console.error(`[${name}] ${text.trim()}`);
  });

  child.on("spawn", () => {
    // Give the process a moment to actually initialize before calling it "running".
    setTimeout(() => {
      if (isServiceActuallyRunning(name) && svc.status === "starting") {
        svc.status = "running";
        broadcast("services_status", buildServicesStatusPayload());
      }
    }, 2000);
  });

  child.on("error", (err) => {
    console.error(`[services] ${name} process error:`, err.message);
    appendServiceLog(name, `[process-error] ${err.message}`);
    svc.status = "error";
    svc.process = null;
    svc.pid = null;
    broadcast("services_status", buildServicesStatusPayload());
  });

  child.on("close", (code) => {
    console.log(`[services] ${name} exited with code ${code}`);
    appendServiceLog(name, `[exit] code ${code}`);
    svc.process = null;
    svc.pid = null;
    svc.status = "stopped";
    broadcast("services_status", buildServicesStatusPayload());
  });

  broadcast("services_status", buildServicesStatusPayload());
  return { pid: child.pid, status: svc.status };
}

function stopServiceProcess(name) {
  const svc = services[name];
  if (!isServiceActuallyRunning(name)) {
    return { notRunning: true };
  }

  svc.status = "stopping";
  broadcast("services_status", buildServicesStatusPayload());

  try {
    svc.process.kill("SIGTERM");
  } catch (err) {
    console.error(`[services] failed to send SIGTERM to ${name}:`, err.message);
  }

  const proc = svc.process;
  setTimeout(() => {
    if (proc && proc.exitCode === null && !proc.killed) {
      console.warn(`[services] force killing ${name}`);
      try {
        proc.kill("SIGKILL");
      } catch (err) {
        console.error(`[services] failed to SIGKILL ${name}:`, err.message);
      }
    }
  }, 5000);

  return { status: svc.status };
}

// ---------------------------------------------------------------------------
// REST API
// ---------------------------------------------------------------------------

app.get("/health", (req, res) => {
  res.json({ status: "ok", uptime: process.uptime(), timestamp: new Date().toISOString() });
});

app.get("/api/services/status", (req, res) => {
  res.json(buildServicesStatusPayload());
});

app.post("/api/services/:name/start", (req, res) => {
  const { name } = req.params;
  if (!SERVICE_NAMES.includes(name)) {
    return res.status(400).json({ error: `Unknown service '${name}'`, validNames: SERVICE_NAMES });
  }

  const result = startServiceProcess(name);
  if (result.error) {
    return res.status(500).json({ error: `Failed to start ${name}: ${result.error}` });
  }
  if (result.alreadyRunning) {
    return res.json({ message: `${name} is already running`, status: services[name].status });
  }
  res.json({ message: `${name} starting`, pid: result.pid, status: result.status });
});

app.post("/api/services/:name/stop", (req, res) => {
  const { name } = req.params;
  if (!SERVICE_NAMES.includes(name)) {
    return res.status(400).json({ error: `Unknown service '${name}'`, validNames: SERVICE_NAMES });
  }

  const result = stopServiceProcess(name);
  if (result.notRunning) {
    return res.json({ message: `${name} is not running`, status: services[name].status });
  }
  res.json({ message: `${name} stop signal sent`, status: result.status });
});

app.get("/api/services/:name/logs", (req, res) => {
  const { name } = req.params;
  if (!SERVICE_NAMES.includes(name)) {
    return res.status(400).json({ error: `Unknown service '${name}'`, validNames: SERVICE_NAMES });
  }
  res.json({ name, log: services[name].log });
});

app.get("/api/audit", (req, res) => {
  res.json(buffers.archiveEntries);
});

app.get("/api/orchestration-events", (req, res) => {
  res.json(buffers.orchestrationEvents);
});

app.get("/api/profile-changes", (req, res) => {
  res.json(buffers.profileChanges);
});

app.get("/api/sentinel-readings", (req, res) => {
  res.json(buffers.sentinelReadings);
});

// Oracle chat: forwards to Kong AI Gateway (ai-proxy + ai-rag-injector route).
app.post("/api/oracle/chat", async (req, res) => {
  const { question } = req.body || {};

  if (!question || typeof question !== "string" || !question.trim()) {
    return res.status(400).json({ error: "Request body must include a non-empty 'question' string." });
  }

  const url = `${KONG_AI_GATEWAY_URL}/chat`;

  let response;
  try {
    // Use global fetch (Node 18+) — avoids adding an extra dependency.
    response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        messages: [{ role: "user", content: question }],
      }),
      signal: AbortSignal.timeout(20000),
    });
  } catch (err) {
    console.error("[oracle] failed to reach Kong AI Gateway:", err.message);
    return res.status(502).json({
      error: `Could not reach Kong AI Gateway at ${url}. Is it running? (${err.message})`,
    });
  }

  if (!response.ok) {
    const bodyText = await response.text().catch(() => "");
    console.error(`[oracle] Kong AI Gateway returned ${response.status}: ${bodyText}`);
    return res.status(502).json({
      error: `Kong AI Gateway returned status ${response.status}`,
      details: bodyText,
    });
  }

  let data;
  try {
    data = await response.json();
  } catch (err) {
    return res.status(502).json({ error: "Kong AI Gateway returned a non-JSON response." });
  }

  const answer =
    data?.choices?.[0]?.message?.content ??
    data?.choices?.[0]?.text ??
    data?.answer ??
    data?.message ??
    null;

  if (!answer) {
    console.warn("[oracle] Could not extract answer from Kong AI Gateway response:", JSON.stringify(data));
    return res.status(502).json({
      error: "Kong AI Gateway response did not contain a recognizable answer field.",
      raw: data,
    });
  }

  res.json({ answer });
});

// ---------------------------------------------------------------------------
// Startup
// ---------------------------------------------------------------------------

server.listen(SERVER_PORT, () => {
  console.log(`Matrix UI (Event-Driven AI Patterns) server running on port ${SERVER_PORT}`);
  console.log(`  HTTP API:  http://localhost:${SERVER_PORT}`);
  console.log(`  WebSocket: ws://localhost:${SERVER_PORT}`);
  console.log(`  Kafka broker: ${KAFKA_BROKER}`);
  console.log(`  Kong AI Gateway: ${KONG_AI_GATEWAY_URL}`);
});
