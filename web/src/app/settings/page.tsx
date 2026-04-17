"use client";

import { useQuery } from "@tanstack/react-query";
import { getServices } from "@/lib/api";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";

export default function SettingsPage() {
  const { data: services, isLoading } = useQuery({
    queryKey: ["services"],
    queryFn: getServices,
  });

  return (
    <div>
      <h1 className="text-2xl font-bold mb-6">Settings</h1>

      <Card>
        <CardHeader>
          <CardTitle>Service Configuration</CardTitle>
        </CardHeader>
        <CardContent>
          {isLoading ? (
            <p className="text-muted-foreground">Loading...</p>
          ) : (
            <div className="space-y-3">
              {services?.map((svc) => (
                <div key={svc.id} className="flex items-center justify-between py-2 border-b last:border-0">
                  <div>
                    <span className="font-medium">{svc.name}</span>
                    <span className="text-sm text-muted-foreground ml-2">({svc.id})</span>
                  </div>
                  <Badge variant={svc.status === "active" ? "default" : "secondary"}>
                    {svc.status}
                  </Badge>
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>

      <Card className="mt-4">
        <CardHeader>
          <CardTitle>Server Info</CardTitle>
        </CardHeader>
        <CardContent className="text-sm text-muted-foreground">
          <p>Endpoint: http://localhost:4747</p>
          <p>Dashboard: http://localhost:3000 (dev) / http://localhost:4747 (production)</p>
        </CardContent>
      </Card>
    </div>
  );
}
