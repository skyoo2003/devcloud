"use client";

import { useQuery } from "@tanstack/react-query";
import { getLogs } from "@/lib/api";
import { Card } from "@/components/ui/card";
import { LogViewer } from "@/components/log-viewer";

export default function LogsPage() {
  const { data: logs, isLoading } = useQuery({
    queryKey: ["logs"],
    queryFn: () => getLogs(100),
    refetchInterval: 2000,
  });

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold">API Logs</h1>
        <span className="text-sm text-muted-foreground">
          {logs?.length ?? 0} entries (auto-refreshing)
        </span>
      </div>
      <Card>
        {isLoading ? (
          <p className="text-muted-foreground p-8 text-center">Loading...</p>
        ) : (
          <LogViewer logs={logs ?? []} />
        )}
      </Card>
    </div>
  );
}
