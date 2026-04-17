import {
  Card,
  CardHeader,
  CardTitle,
  CardContent,
} from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";

interface ServiceCardProps {
  id: string;
  name: string;
  status: string;
  resourceCount: number;
}

export function ServiceCard({
  id,
  name,
  status,
  resourceCount,
}: ServiceCardProps) {
  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between pb-2">
        <CardTitle className="text-sm font-medium">{name}</CardTitle>
        <Badge variant={status === "active" ? "default" : "secondary"}>
          {status}
        </Badge>
      </CardHeader>
      <CardContent>
        <div className="text-2xl font-bold">{resourceCount}</div>
        <p className="text-xs text-muted-foreground">resources</p>
      </CardContent>
    </Card>
  );
}
