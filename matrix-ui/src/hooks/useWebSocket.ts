import { useEffect, useRef, useState, useCallback } from 'react';

export type ServerMessageType =
  | 'init'
  | 'orchestration_event'
  | 'profile_change'
  | 'archive_entry'
  | 'services_status'
  | 'sentinel_reading';

export interface ServerMessage<T = any> {
  type: ServerMessageType;
  data: T;
}

export interface UseWebSocketOptions {
  /** Called for every parsed message received from the server. */
  onMessage?: (message: ServerMessage) => void;
  /** Base delay (ms) for exponential-backoff reconnect attempts. */
  reconnectBaseDelayMs?: number;
  /** Max delay (ms) between reconnect attempts. */
  reconnectMaxDelayMs?: number;
}

export interface UseWebSocketResult {
  isConnected: boolean;
}

/**
 * Reconnecting WebSocket hook, adapted from the sko-fy27-se-enablement-keg
 * matrix-ui baseline. Connects to the Express/WS server (SERVER_PORT, default
 * 3001) and invokes onMessage for every parsed JSON message. Automatically
 * retries with exponential backoff if the connection drops or the server is
 * not up yet.
 */
export function useWebSocket(url: string, options: UseWebSocketOptions = {}): UseWebSocketResult {
  const { onMessage, reconnectBaseDelayMs = 1000, reconnectMaxDelayMs = 15000 } = options;
  const [isConnected, setIsConnected] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectAttemptRef = useRef(0);
  const reconnectTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const onMessageRef = useRef(onMessage);
  const unmountedRef = useRef(false);

  onMessageRef.current = onMessage;

  const connect = useCallback(() => {
    if (unmountedRef.current) return;

    let ws: WebSocket;
    try {
      ws = new WebSocket(url);
    } catch (err) {
      scheduleReconnect();
      return;
    }
    wsRef.current = ws;

    ws.onopen = () => {
      setIsConnected(true);
      reconnectAttemptRef.current = 0;
    };

    ws.onmessage = (event) => {
      try {
        const message = JSON.parse(event.data);
        onMessageRef.current?.(message);
      } catch (e) {
        console.error('Failed to parse WebSocket message:', e);
      }
    };

    ws.onclose = () => {
      setIsConnected(false);
      wsRef.current = null;
      scheduleReconnect();
    };

    ws.onerror = () => {
      ws.close();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [url]);

  function scheduleReconnect() {
    if (unmountedRef.current) return;
    if (reconnectTimerRef.current) return;

    const attempt = reconnectAttemptRef.current;
    const delay = Math.min(reconnectBaseDelayMs * Math.pow(2, attempt), reconnectMaxDelayMs);
    reconnectAttemptRef.current = attempt + 1;

    reconnectTimerRef.current = setTimeout(() => {
      reconnectTimerRef.current = null;
      connect();
    }, delay);
  }

  useEffect(() => {
    unmountedRef.current = false;
    connect();

    return () => {
      unmountedRef.current = true;
      if (reconnectTimerRef.current) {
        clearTimeout(reconnectTimerRef.current);
        reconnectTimerRef.current = null;
      }
      wsRef.current?.close();
      wsRef.current = null;
    };
  }, [connect]);

  return { isConnected };
}

export default useWebSocket;
