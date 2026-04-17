"use client";

import { Badge } from "@/components/ui/badge";
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from "@/components/ui/table";
import type { RequestLog } from "@/lib/types";

interface LogViewerProps {
  logs: RequestLog[];
}

function statusColor(status: number): "default" | "destructive" | "secondary" {
  if (status >= 200 && status < 300) return "default";
  if (status >= 400) return "destructive";
  return "secondary";
}

export function LogViewer({ logs }: LogViewerProps) {
  if (logs.length === 0) {
    return <p className="text-muted-foreground text-center py-8">No API calls recorded yet.</p>;
  }

  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Method</TableHead>
          <TableHead>Path</TableHead>
          <TableHead>Status</TableHead>
          <TableHead>Duration</TableHead>
          <TableHead>Service</TableHead>
          <TableHead>Time</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {logs.map((log, i) => (
          <TableRow key={i}>
            <TableCell>
              <Badge variant="outline">{log.method}</Badge>
            </TableCell>
            <TableCell className="font-mono text-sm">{log.path}</TableCell>
            <TableCell>
              <Badge variant={statusColor(log.status)}>{log.status}</Badge>
            </TableCell>
            <TableCell className="text-sm">{log.duration}</TableCell>
            <TableCell className="text-sm">{log.service || "-"}</TableCell>
            <TableCell className="text-sm text-muted-foreground">
              {new Date(log.timestamp).toLocaleTimeString()}
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}
