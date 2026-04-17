export interface ServiceInfo {
  id: string;
  name: string;
  status: string;
  resourceCount: number;
}

export interface Resource {
  type: string;
  id: string;
  name: string;
}

export interface Metrics {
  totalRequests: number;
  errorCount: number;
  services: number;
}

export interface ServiceMetrics {
  totalRequests: number;
  errorCount: number;
  resourceCount: number;
}

export interface RequestLog {
  method: string;
  path: string;
  status: number;
  duration: string;
  timestamp: string;
  service: string;
}
