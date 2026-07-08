import {
  ArchiveEntry,
  OrchestrationEvent,
  ProfileChange,
  ServiceName,
  ServiceRunState,
  ServicesStatusPayload,
  SERVICE_NAMES,
} from '../types';

// Base URL for the Express/WS backend-for-frontend server (matrix-ui/server).
// In dev, CRA proxies are not configured, so we talk to it directly on SERVER_PORT (default 3001).
export const SERVER_HTTP_BASE =
  (typeof process !== 'undefined' && (process as any).env?.REACT_APP_SERVER_URL) ||
  'http://localhost:3001';

export const SERVER_WS_URL = SERVER_HTTP_BASE.replace(/^http/, 'ws');

async function handleJson<T>(res: Response): Promise<T> {
  const text = await res.text();
  let body: any = null;
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    body = { raw: text };
  }
  if (!res.ok) {
    const message = body?.error || body?.message || `Request failed with status ${res.status}`;
    throw new Error(message);
  }
  return body as T;
}

export async function getServicesStatus(): Promise<ServicesStatusPayload> {
  const res = await fetch(`${SERVER_HTTP_BASE}/api/services/status`);
  return handleJson<ServicesStatusPayload>(res);
}

export async function startService(name: ServiceName): Promise<{ message: string; status: ServiceRunState }> {
  const res = await fetch(`${SERVER_HTTP_BASE}/api/services/${name}/start`, { method: 'POST' });
  return handleJson(res);
}

export async function stopService(name: ServiceName): Promise<{ message: string; status: ServiceRunState }> {
  const res = await fetch(`${SERVER_HTTP_BASE}/api/services/${name}/stop`, { method: 'POST' });
  return handleJson(res);
}

export async function startAllServices(): Promise<void> {
  await Promise.all(SERVICE_NAMES.map((name) => startService(name).catch(() => undefined)));
}

export async function stopAllServices(): Promise<void> {
  await Promise.all(SERVICE_NAMES.map((name) => stopService(name).catch(() => undefined)));
}

export async function getAuditLog(): Promise<ArchiveEntry[]> {
  const res = await fetch(`${SERVER_HTTP_BASE}/api/audit`);
  return handleJson<ArchiveEntry[]>(res);
}

export async function getOrchestrationEvents(): Promise<OrchestrationEvent[]> {
  const res = await fetch(`${SERVER_HTTP_BASE}/api/orchestration-events`);
  return handleJson<OrchestrationEvent[]>(res);
}

export async function getProfileChanges(): Promise<ProfileChange[]> {
  const res = await fetch(`${SERVER_HTTP_BASE}/api/profile-changes`);
  return handleJson<ProfileChange[]>(res);
}

export interface OracleChatResponse {
  answer: string;
}

export async function sendOracleChat(question: string): Promise<OracleChatResponse> {
  const res = await fetch(`${SERVER_HTTP_BASE}/api/oracle/chat`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ question }),
  });
  return handleJson<OracleChatResponse>(res);
}

export async function getHealth(): Promise<{ status: string }> {
  const res = await fetch(`${SERVER_HTTP_BASE}/health`);
  return handleJson(res);
}
