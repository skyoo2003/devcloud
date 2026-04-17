import { ServiceInfo, Resource, Metrics, ServiceMetrics, RequestLog } from "./types";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:4747";

export async function fetchAPI<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...options?.headers,
    },
  });
  if (!res.ok) {
    throw new Error(`API error: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

export function getServices() {
  return fetchAPI<ServiceInfo[]>("/devcloud/api/services");
}

export function getServiceResources(serviceId: string) {
  return fetchAPI<Resource[]>(`/devcloud/api/services/${serviceId}/resources`);
}

export function getMetrics() {
  return fetchAPI<Metrics>("/devcloud/api/metrics");
}

export function getServiceMetrics(serviceId: string) {
  return fetchAPI<ServiceMetrics>(`/devcloud/api/metrics/${serviceId}`);
}

export function getLogs(limit?: number) {
  const params = limit ? `?limit=${limit}` : "";
  return fetchAPI<RequestLog[]>(`/devcloud/api/logs${params}`);
}
