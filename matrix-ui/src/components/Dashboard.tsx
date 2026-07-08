import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { AnimatePresence, motion } from 'framer-motion';
import {
  Activity,
  Archive,
  Bot,
  Brain,
  Database,
  Eye,
  Loader2,
  Play,
  Send,
  Server,
  Shield,
  Square,
  Zap,
} from 'lucide-react';
import { useWebSocket, ServerMessage } from '../hooks/useWebSocket';
import {
  getAuditLog,
  getOrchestrationEvents,
  getProfileChanges,
  getServicesStatus,
  sendOracleChat,
  startAllServices,
  startService,
  stopAllServices,
  stopService,
  SERVER_WS_URL,
} from '../services/api';
import {
  ArchiveEntry,
  OrchestrationEvent,
  ProfileChange,
  ServiceName,
  ServicesStatusPayload,
  SERVICE_NAMES,
} from '../types';

const MAX_LIST_LENGTH = 100;

const SEVERITY_COLOR: Record<string, string> = {
  low: 'text-matrix-green border-matrix-green/50',
  medium: 'text-matrix-yellow border-matrix-yellow/50',
  high: 'text-orange-400 border-orange-400/50',
  critical: 'text-matrix-red border-matrix-red/50',
};

const AGENT_COLOR: Record<string, string> = {
  'anomaly-detector-agent': 'bg-matrix-yellow/20 text-matrix-yellow border-matrix-yellow/50',
  'sentinel-agent': 'bg-sky-400/20 text-sky-300 border-sky-400/50',
  'dispatch-agent': 'bg-purple-400/20 text-purple-300 border-purple-400/50',
  'context-updater': 'bg-matrix-green/20 text-matrix-green border-matrix-green/50',
};

function agentColor(agent: string): string {
  return AGENT_COLOR[agent] || 'bg-matrix-darkgreen/20 text-matrix-darkgreen border-matrix-darkgreen/50';
}

function formatTime(ts: string): string {
  try {
    return new Date(ts).toLocaleTimeString();
  } catch {
    return ts;
  }
}

function prepend<T>(list: T[], item: T, max = MAX_LIST_LENGTH): T[] {
  return [item, ...list].slice(0, max);
}

interface OrchestrationRow extends OrchestrationEvent {
  responses: ArchiveEntry[];
}

const SERVICE_LABELS: Record<ServiceName, string> = {
  'data-generator': 'Data Generator',
  'anomaly-detector-agent': 'Anomaly Detector',
  'sentinel-agent': 'Sentinel Agent',
  'dispatch-agent': 'Dispatch Agent',
  'context-updater': 'Context Updater',
};

const Dashboard: React.FC = () => {
  const [orchestrationEvents, setOrchestrationEvents] = useState<OrchestrationEvent[]>([]);
  const [profileChanges, setProfileChanges] = useState<ProfileChange[]>([]);
  const [archiveEntries, setArchiveEntries] = useState<ArchiveEntry[]>([]);
  const [servicesStatus, setServicesStatus] = useState<ServicesStatusPayload | null>(null);
  const [busyServices, setBusyServices] = useState<Record<string, boolean>>({});
  const [bulkBusy, setBulkBusy] = useState<'start' | 'stop' | null>(null);

  const [question, setQuestion] = useState('');
  const [answer, setAnswer] = useState<string | null>(null);
  const [chatError, setChatError] = useState<string | null>(null);
  const [isAsking, setIsAsking] = useState(false);
  const chatEndRef = useRef<HTMLDivElement | null>(null);

  const handleMessage = useCallback((message: ServerMessage) => {
    switch (message.type) {
      case 'init': {
        const data = message.data || {};
        if (Array.isArray(data.orchestrationEvents)) setOrchestrationEvents(data.orchestrationEvents);
        if (Array.isArray(data.profileChanges)) setProfileChanges(data.profileChanges);
        if (Array.isArray(data.archiveEntries)) setArchiveEntries(data.archiveEntries);
        if (data.services) setServicesStatus(data.services);
        break;
      }
      case 'orchestration_event':
        setOrchestrationEvents((prev) => prepend(prev, message.data));
        break;
      case 'profile_change':
        setProfileChanges((prev) => prepend(prev, message.data));
        break;
      case 'archive_entry':
        setArchiveEntries((prev) => prepend(prev, message.data));
        break;
      case 'services_status':
        setServicesStatus(message.data);
        break;
      default:
        break;
    }
  }, []);

  const { isConnected } = useWebSocket(SERVER_WS_URL, { onMessage: handleMessage });

  // Fallback REST fetch in case the WS `init` message is missed (e.g. reconnect race).
  useEffect(() => {
    getOrchestrationEvents().then(setOrchestrationEvents).catch(() => undefined);
    getProfileChanges().then(setProfileChanges).catch(() => undefined);
    getAuditLog().then(setArchiveEntries).catch(() => undefined);
    getServicesStatus().then(setServicesStatus).catch(() => undefined);
  }, []);

  // Poll services status as a safety net (WS should push it too).
  useEffect(() => {
    const interval = setInterval(() => {
      getServicesStatus().then(setServicesStatus).catch(() => undefined);
    }, 8000);
    return () => clearInterval(interval);
  }, []);

  const orchestrationRows: OrchestrationRow[] = useMemo(() => {
    return orchestrationEvents.slice(0, 20).map((evt) => ({
      ...evt,
      responses: archiveEntries.filter(
        (a) =>
          a.ref_id === evt.event_id &&
          (a.agent === 'sentinel-agent' || a.agent === 'dispatch-agent')
      ),
    }));
  }, [orchestrationEvents, archiveEntries]);

  const kafkaConnected = servicesStatus?.kafka?.connected ?? false;
  const serviceList = servicesStatus?.services ?? [];

  const runningCount = serviceList.filter((s) => s.status === 'running').length;

  const doStart = async (name: ServiceName) => {
    setBusyServices((b) => ({ ...b, [name]: true }));
    try {
      await startService(name);
    } catch (e) {
      console.error(`Failed to start ${name}`, e);
    } finally {
      getServicesStatus().then(setServicesStatus).catch(() => undefined);
      setBusyServices((b) => ({ ...b, [name]: false }));
    }
  };

  const doStop = async (name: ServiceName) => {
    setBusyServices((b) => ({ ...b, [name]: true }));
    try {
      await stopService(name);
    } catch (e) {
      console.error(`Failed to stop ${name}`, e);
    } finally {
      getServicesStatus().then(setServicesStatus).catch(() => undefined);
      setBusyServices((b) => ({ ...b, [name]: false }));
    }
  };

  const doStartAll = async () => {
    setBulkBusy('start');
    try {
      await startAllServices();
    } finally {
      getServicesStatus().then(setServicesStatus).catch(() => undefined);
      setBulkBusy(null);
    }
  };

  const doStopAll = async () => {
    setBulkBusy('stop');
    try {
      await stopAllServices();
    } finally {
      getServicesStatus().then(setServicesStatus).catch(() => undefined);
      setBulkBusy(null);
    }
  };

  const askOracle = async () => {
    if (!question.trim() || isAsking) return;
    setIsAsking(true);
    setChatError(null);
    setAnswer(null);
    try {
      const res = await sendOracleChat(question.trim());
      setAnswer(res.answer);
    } catch (e: any) {
      setChatError(e?.message || 'Failed to reach the Oracle (Kong AI Gateway).');
    } finally {
      setIsAsking(false);
    }
  };

  return (
    <div className="min-h-screen flex flex-col">
      {/* Header */}
      <header className="border-b border-matrix-darkgreen bg-matrix-black/80 backdrop-blur sticky top-0 z-20">
        <div className="px-6 py-3 flex flex-wrap items-center justify-between gap-3">
          <div className="flex items-center gap-3">
            <Zap className="w-6 h-6 text-matrix-red animate-glow" />
            <div>
              <h1 className="text-xl font-bold tracking-wide">EVENT-DRIVEN AI PATTERNS</h1>
              <p className="text-xs text-matrix-darkgreen">
                Agent Smith Protocol &middot; The Oracle &middot; The Zion Archive
              </p>
            </div>
          </div>

          <div className="flex items-center gap-4 text-sm">
            <div className="flex items-center gap-2">
              <div className={`status-indicator ${isConnected ? 'status-active' : 'status-inactive'}`} />
              <span>{isConnected ? 'WS LIVE' : 'WS OFFLINE'}</span>
            </div>
            <div className="flex items-center gap-2">
              <div className={`status-indicator ${kafkaConnected ? 'status-active' : 'status-error'}`} />
              <span>{kafkaConnected ? 'KAFKA UP' : 'KAFKA DOWN'}</span>
            </div>
            <div className="flex items-center gap-2">
              <Server className="w-4 h-4" />
              <span>{runningCount}/{SERVICE_NAMES.length} services</span>
            </div>
            <div className="flex items-center gap-2">
              <button
                onClick={doStartAll}
                disabled={bulkBusy !== null}
                className="matrix-button text-xs flex items-center gap-1 disabled:opacity-50"
              >
                {bulkBusy === 'start' ? <Loader2 className="w-3 h-3 animate-spin" /> : <Play className="w-3 h-3" />}
                START ALL
              </button>
              <button
                onClick={doStopAll}
                disabled={bulkBusy !== null}
                className="matrix-button text-xs flex items-center gap-1 border-matrix-red text-matrix-red hover:bg-matrix-red hover:text-matrix-black disabled:opacity-50"
              >
                {bulkBusy === 'stop' ? <Loader2 className="w-3 h-3 animate-spin" /> : <Square className="w-3 h-3" />}
                STOP ALL
              </button>
            </div>
          </div>
        </div>

        {/* Per-service status strip */}
        <div className="px-6 pb-3 flex flex-wrap gap-2">
          {SERVICE_NAMES.map((name) => {
            const svc = serviceList.find((s) => s.name === name);
            const status = svc?.status ?? 'stopped';
            const busy = !!busyServices[name];
            return (
              <div
                key={name}
                className="flex items-center gap-2 px-2 py-1 border border-matrix-darkgreen/50 rounded text-xs"
              >
                <div
                  className={`status-indicator ${
                    status === 'running'
                      ? 'status-active'
                      : status === 'starting' || status === 'stopping'
                      ? 'bg-matrix-yellow animate-pulse'
                      : status === 'error'
                      ? 'status-error'
                      : 'status-inactive'
                  }`}
                />
                <span className="text-matrix-darkgreen">{SERVICE_LABELS[name]}</span>
                <span className="uppercase">{status}</span>
                {status === 'running' || status === 'starting' ? (
                  <button
                    onClick={() => doStop(name)}
                    disabled={busy}
                    className="text-matrix-red hover:underline disabled:opacity-50"
                  >
                    stop
                  </button>
                ) : (
                  <button
                    onClick={() => doStart(name)}
                    disabled={busy}
                    className="text-matrix-green hover:underline disabled:opacity-50"
                  >
                    start
                  </button>
                )}
              </div>
            );
          })}
        </div>
      </header>

      {/* Three-column layout */}
      <main className="flex-1 grid grid-cols-1 lg:grid-cols-3 gap-4 p-4">
        {/* Column 1: Agent Smith Protocol */}
        <section className="terminal-window flex flex-col min-h-[70vh] max-h-[85vh]">
          <div className="terminal-header">
            <Bot className="w-4 h-4 text-matrix-green" />
            <span className="ml-2 font-semibold">AGENT SMITH PROTOCOL</span>
            <span className="ml-auto text-xs text-matrix-darkgreen">fan-out: 1 event -&gt; 2 agents</span>
          </div>
          <div className="terminal-content overflow-y-auto flex-1 space-y-3">
            {orchestrationRows.length === 0 ? (
              <EmptyState icon={Activity} text="Waiting for anomaly events on agent.orchestration.events..." />
            ) : (
              <AnimatePresence initial={false}>
                {orchestrationRows.map((evt) => (
                  <motion.div
                    key={evt.event_id}
                    initial={{ opacity: 0, y: -12 }}
                    animate={{ opacity: 1, y: 0 }}
                    exit={{ opacity: 0 }}
                    transition={{ duration: 0.25 }}
                    className={`border rounded p-3 ${SEVERITY_COLOR[evt.severity] || 'border-matrix-darkgreen'} bg-matrix-black/40`}
                  >
                    <div className="flex items-center justify-between mb-1">
                      <div className="flex items-center gap-2 text-xs">
                        <Shield className="w-3 h-3" />
                        <span className="font-mono">{evt.sector}</span>
                      </div>
                      <span className="text-xs uppercase font-bold">{evt.severity}</span>
                    </div>
                    <div className="text-sm mb-1">{evt.anomaly_summary}</div>
                    <div className="text-xs text-matrix-darkgreen mb-2">
                      recommended: {evt.recommended_action}
                    </div>
                    <div className="text-[10px] text-matrix-darkgreen mb-2">
                      {formatTime(evt.timestamp)} &middot; {evt.event_id}
                    </div>

                    {/* Fan-out responses, side by side */}
                    <div className="grid grid-cols-2 gap-2">
                      <ResponseChip
                        label="sentinel-agent"
                        icon={Eye}
                        entry={evt.responses.find((r) => r.agent === 'sentinel-agent')}
                      />
                      <ResponseChip
                        label="dispatch-agent"
                        icon={Send}
                        entry={evt.responses.find((r) => r.agent === 'dispatch-agent')}
                      />
                    </div>
                  </motion.div>
                ))}
              </AnimatePresence>
            )}
          </div>
        </section>

        {/* Column 2: The Oracle */}
        <section className="terminal-window flex flex-col min-h-[70vh] max-h-[85vh]">
          <div className="terminal-header">
            <Eye className="w-4 h-4 text-matrix-green" />
            <span className="ml-2 font-semibold">THE ORACLE</span>
            <span className="ml-auto text-xs text-matrix-darkgreen">CDC -&gt; RAG -&gt; chat</span>
          </div>

          <div className="overflow-y-auto flex-1 border-b border-matrix-darkgreen/40">
            <div className="terminal-content space-y-2">
              {profileChanges.length === 0 ? (
                <EmptyState icon={Database} text="Waiting for profile changes on oracle.profile.changes..." />
              ) : (
                <AnimatePresence initial={false}>
                  {profileChanges.slice(0, 30).map((chg) => (
                    <motion.div
                      key={chg.change_id}
                      initial={{ opacity: 0, y: -10 }}
                      animate={{ opacity: 1, y: 0 }}
                      exit={{ opacity: 0 }}
                      transition={{ duration: 0.2 }}
                      className="border border-matrix-darkgreen/50 rounded p-2 text-xs bg-matrix-black/40"
                    >
                      <div className="flex items-center justify-between mb-1">
                        <span className="font-mono text-matrix-green">{chg.subject_name}</span>
                        <span
                          className={`uppercase font-bold ${
                            chg.op === 'DELETE'
                              ? 'text-matrix-red'
                              : chg.op === 'INSERT'
                              ? 'text-matrix-green'
                              : 'text-matrix-yellow'
                          }`}
                        >
                          {chg.op}
                        </span>
                      </div>
                      <div className="text-matrix-darkgreen">
                        {chg.updated_field} <span className="text-matrix-green">&rarr; {chg.updated_value}</span>
                      </div>
                      <div className="text-matrix-darkgreen/80 italic mt-1">{chg.note}</div>
                      <div className="text-[10px] text-matrix-darkgreen mt-1">
                        {chg.subject_role} &middot; {formatTime(chg.timestamp)}
                      </div>
                    </motion.div>
                  ))}
                </AnimatePresence>
              )}
            </div>
          </div>

          {/* Chat box */}
          <div className="terminal-content space-y-2">
            <div className="flex items-center gap-2 text-xs text-matrix-darkgreen">
              <Brain className="w-4 h-4 text-matrix-green" />
              Ask the Oracle (answers with freshest RAG context)
            </div>
            <div className="flex gap-2">
              <input
                type="text"
                value={question}
                onChange={(e) => setQuestion(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') askOracle();
                }}
                placeholder="e.g. Where is Trinity right now?"
                className="matrix-input flex-1 text-sm"
              />
              <button
                onClick={askOracle}
                disabled={isAsking || !question.trim()}
                className="matrix-button text-xs flex items-center gap-1 disabled:opacity-50"
              >
                {isAsking ? <Loader2 className="w-3 h-3 animate-spin" /> : <Send className="w-3 h-3" />}
                ASK
              </button>
            </div>
            <div ref={chatEndRef} className="min-h-[3rem] text-sm">
              {isAsking && <div className="text-matrix-darkgreen animate-pulse">Consulting the Oracle...</div>}
              {chatError && <div className="text-matrix-red text-xs">{chatError}</div>}
              {answer && !isAsking && (
                <div className="border border-matrix-green/40 rounded p-2 bg-matrix-green/5 whitespace-pre-wrap">
                  {answer}
                </div>
              )}
            </div>
          </div>
        </section>

        {/* Column 3: The Zion Archive */}
        <section className="terminal-window flex flex-col min-h-[70vh] max-h-[85vh]">
          <div className="terminal-header">
            <Archive className="w-4 h-4 text-matrix-green" />
            <span className="ml-2 font-semibold">THE ZION ARCHIVE</span>
            <span className="ml-auto text-xs text-matrix-darkgreen">append-only commit log</span>
          </div>
          <div className="terminal-content overflow-y-auto flex-1 font-mono text-xs">
            {archiveEntries.length === 0 ? (
              <EmptyState icon={Archive} text="Ledger empty. Waiting for entries on zion.archive.log..." />
            ) : (
              <div className="divide-y divide-matrix-darkgreen/20">
                <AnimatePresence initial={false}>
                  {archiveEntries.map((entry, idx) => (
                    <motion.div
                      key={entry.entry_id}
                      initial={{ opacity: 0 }}
                      animate={{ opacity: 1 }}
                      exit={{ opacity: 0 }}
                      transition={{ duration: 0.15 }}
                      className="py-1.5 flex items-start gap-2"
                    >
                      <span className="text-matrix-darkgreen/70 w-10 text-right shrink-0">
                        {String(archiveEntries.length - idx).padStart(4, '0')}
                      </span>
                      <span className="text-matrix-darkgreen shrink-0">{formatTime(entry.timestamp)}</span>
                      <span
                        className={`shrink-0 px-1.5 py-0.5 border rounded text-[10px] uppercase ${agentColor(
                          entry.agent
                        )}`}
                      >
                        {entry.agent}
                      </span>
                      <span className="text-matrix-yellow shrink-0">{entry.action}</span>
                      <span className="text-matrix-green truncate">{entry.summary}</span>
                    </motion.div>
                  ))}
                </AnimatePresence>
              </div>
            )}
          </div>
        </section>
      </main>
    </div>
  );
};

const ResponseChip: React.FC<{
  label: string;
  icon: React.ComponentType<{ className?: string }>;
  entry?: ArchiveEntry;
}> = ({ label, icon: Icon, entry }) => {
  return (
    <div
      className={`border rounded p-2 text-[11px] ${
        entry ? agentColor(label) : 'border-matrix-darkgreen/30 text-matrix-darkgreen'
      }`}
    >
      <div className="flex items-center gap-1 mb-1">
        <Icon className="w-3 h-3" />
        <span className="font-semibold">{label}</span>
      </div>
      {entry ? (
        <>
          <div className="uppercase font-bold">{entry.action}</div>
          <div className="truncate">{entry.summary}</div>
        </>
      ) : (
        <div className="flex items-center gap-1 animate-pulse">
          <Loader2 className="w-3 h-3 animate-spin" />
          awaiting response...
        </div>
      )}
    </div>
  );
};

const EmptyState: React.FC<{ icon: React.ComponentType<{ className?: string }>; text: string }> = ({
  icon: Icon,
  text,
}) => (
  <div className="text-center py-10 text-matrix-darkgreen">
    <Icon className="w-8 h-8 mx-auto mb-2 opacity-50" />
    <p className="text-sm">{text}</p>
  </div>
);

export default Dashboard;
