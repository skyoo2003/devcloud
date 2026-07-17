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

export interface RequestLog {
  method: string;
  path: string;
  status: number;
  duration: string;
  timestamp: string;
  service: string;
}
