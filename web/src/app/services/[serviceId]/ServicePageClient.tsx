"use client";

import { useParams } from "next/navigation";
import { useQuery } from "@tanstack/react-query";

interface Resource {
  type: string;
  id: string;
  name: string;
}

interface ServiceInfo {
  id: string;
  name: string;
  status: string;
  resourceCount: number;
}

async function getResources(serviceId: string): Promise<Resource[]> {
  const res = await fetch(`/devcloud/api/services/${serviceId}/resources`);
  if (!res.ok) return [];
  return res.json();
}

async function getServiceInfo(): Promise<ServiceInfo[]> {
  const res = await fetch("/devcloud/api/services");
  if (!res.ok) return [];
  return res.json();
}

export default function ServicePageClient() {
  const params = useParams();
  const serviceId = params.serviceId as string;

  const { data: services } = useQuery({
    queryKey: ["services"],
    queryFn: getServiceInfo,
  });

  const { data: resources, isLoading } = useQuery({
    queryKey: ["resources", serviceId],
    queryFn: () => getResources(serviceId),
  });

  const service = services?.find((s) => s.id === serviceId);

  return (
    <div className="p-6">
      <h1 className="text-2xl font-bold mb-4">
        {service?.name ?? serviceId.toUpperCase()}
      </h1>

      <div className="grid grid-cols-3 gap-4 mb-6">
        <div className="bg-zinc-900 rounded-lg p-4">
          <p className="text-zinc-400 text-sm">Status</p>
          <p className="text-lg font-semibold">{service?.status ?? "—"}</p>
        </div>
        <div className="bg-zinc-900 rounded-lg p-4">
          <p className="text-zinc-400 text-sm">Resources</p>
          <p className="text-lg font-semibold">{service?.resourceCount ?? 0}</p>
        </div>
      </div>

      <h2 className="text-lg font-semibold mb-2">Resources</h2>
      {isLoading ? (
        <p className="text-zinc-400">Loading...</p>
      ) : resources && resources.length > 0 ? (
        <table className="w-full text-sm">
          <thead>
            <tr className="text-zinc-400 text-left border-b border-zinc-800">
              <th className="pb-2">Type</th>
              <th className="pb-2">ID</th>
              <th className="pb-2">Name</th>
            </tr>
          </thead>
          <tbody>
            {resources.map((r) => (
              <tr key={r.id} className="border-b border-zinc-800">
                <td className="py-2">{r.type}</td>
                <td className="py-2 font-mono text-xs">{r.id}</td>
                <td className="py-2">{r.name}</td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : (
        <p className="text-zinc-400">No resources</p>
      )}
    </div>
  );
}
