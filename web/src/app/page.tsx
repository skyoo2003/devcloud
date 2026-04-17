"use client";

import { useQuery } from "@tanstack/react-query";
import { getServices } from "@/lib/api";
import { ServiceCard } from "@/components/service-card";

export default function HomePage() {
  const { data: services, isLoading } = useQuery({
    queryKey: ["services"],
    queryFn: getServices,
  });

  return (
    <div>
      <h1 className="text-2xl font-bold mb-6">Dashboard</h1>
      {isLoading ? (
        <p className="text-muted-foreground">Loading services...</p>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {services?.map((svc) => (
            <ServiceCard
              key={svc.id}
              id={svc.id}
              name={svc.name}
              status={svc.status}
              resourceCount={svc.resourceCount}
            />
          ))}
        </div>
      )}
    </div>
  );
}
