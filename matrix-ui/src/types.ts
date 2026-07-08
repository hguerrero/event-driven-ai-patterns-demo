// Shared message shapes — mirrors the contract in the top-level demo spec.
// Kept in one place so components/services/hooks all agree on field names.

export interface SentinelReading {
  reading_id: string;
  sector: string;
  stability_index: number;
  status: 'STABLE' | 'FLUCTUATING' | 'GLITCH';
  timestamp: string;
}

export type Severity = 'low' | 'medium' | 'high' | 'critical';

export interface OrchestrationEvent {
  event_id: string;
  source: string;
  severity: Severity;
  sector: string;
  anomaly_summary: string;
  recommended_action: string;
  original_reading?: SentinelReading;
  timestamp: string;
}

export type ProfileOp = 'INSERT' | 'UPDATE' | 'DELETE';
export type SubjectRole = 'Operator' | 'Redpill' | 'Bluepill' | 'Unknown';
export type ProfileField = 'status' | 'location' | 'clearance_level' | 'last_seen';

export interface ProfileChange {
  change_id: string;
  op: ProfileOp;
  subject_name: string;
  subject_role: SubjectRole;
  updated_field: ProfileField;
  updated_value: string;
  note: string;
  timestamp: string;
}

export type ArchiveAgent =
  | 'anomaly-detector-agent'
  | 'sentinel-agent'
  | 'dispatch-agent'
  | 'context-updater'
  | string;

export type ArchiveAction =
  | 'evaluated'
  | 'flagged'
  | 'investigated'
  | 'dispatched'
  | 'context_updated'
  | string;

export interface ArchiveEntry {
  entry_id: string;
  agent: ArchiveAgent;
  action: ArchiveAction;
  ref_id: string;
  summary: string;
  timestamp: string;
}

export const SERVICE_NAMES = [
  'data-generator',
  'anomaly-detector-agent',
  'sentinel-agent',
  'dispatch-agent',
  'context-updater',
] as const;

export type ServiceName = typeof SERVICE_NAMES[number];

export type ServiceRunState = 'stopped' | 'starting' | 'running' | 'stopping' | 'error';

export interface ServiceStatus {
  name: ServiceName;
  status: ServiceRunState;
  pid: number | null;
  uptimeMs: number;
}

export interface ServicesStatusPayload {
  kafka: {
    connected: boolean;
    brokers: string[];
  };
  services: ServiceStatus[];
}

export interface InitPayload {
  orchestrationEvents: OrchestrationEvent[];
  profileChanges: ProfileChange[];
  archiveEntries: ArchiveEntry[];
  services: ServicesStatusPayload;
}
