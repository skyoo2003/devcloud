"use client";

import { useQuery } from "@tanstack/react-query";
import { getServiceResources } from "@/lib/api";
import { Card, CardContent } from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";

export default function S3Page() {
  const { data: resources, isLoading } = useQuery({
    queryKey: ["s3-resources"],
    queryFn: () => getServiceResources("s3"),
  });

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold">S3 Buckets</h1>
        <Badge variant="outline">{resources?.length ?? 0} buckets</Badge>
      </div>

      {isLoading ? (
        <p className="text-muted-foreground">Loading...</p>
      ) : resources?.length === 0 ? (
        <Card>
          <CardContent className="py-8 text-center text-muted-foreground">
            No buckets found. Use the AWS CLI to create one:
            <code className="block mt-2 text-sm">
              aws --endpoint-url http://localhost:4747 s3 mb s3://my-bucket
            </code>
          </CardContent>
        </Card>
      ) : (
        <Card>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Bucket Name</TableHead>
                <TableHead>Type</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {resources?.map((r) => (
                <TableRow key={r.id}>
                  <TableCell className="font-medium">{r.name}</TableCell>
                  <TableCell>
                    <Badge variant="secondary">{r.type}</Badge>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </Card>
      )}
    </div>
  );
}
