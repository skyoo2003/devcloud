"use client";

import { useEffect, useRef, useState } from "react";

export interface WSEvent {
  source: string;
  type: string;
  detail: Record<string, unknown>;
  timestamp: string;
}

export function useWebSocket(url: string) {
  const wsRef = useRef<WebSocket | null>(null);
  const [lastEvent, setLastEvent] = useState<WSEvent | null>(null);
  const [connected, setConnected] = useState(false);

  useEffect(() => {
    const ws = new WebSocket(url);
    wsRef.current = ws;

    ws.onopen = () => setConnected(true);
    ws.onclose = () => setConnected(false);
    ws.onmessage = (e) => {
      try {
        const event = JSON.parse(e.data) as WSEvent;
        setLastEvent(event);
      } catch {}
    };

    return () => {
      ws.close();
    };
  }, [url]);

  return { lastEvent, connected };
}
