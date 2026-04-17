"use client";

import { useQuery } from "@tanstack/react-query";
import { getServiceResources } from "@/lib/api";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from "@/components/ui/table";

export default function DynamoDBPage() {
  const { data: resources, isLoading } = useQuery({
    queryKey: ["dynamodb-resources"],
    queryFn: () => getServiceResources("dynamodb"),
  });

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold">DynamoDB Tables</h1>
        <Badge variant="outline">{resources?.length ?? 0} resources</Badge>
      </div>
      {isLoading ? (
        <p className="text-muted-foreground">Loading...</p>
      ) : resources?.length === 0 ? (
        <Card>
          <CardContent className="py-8 text-center text-muted-foreground">
            No resources found. Use the AWS CLI:
            <code className="block mt-2 text-sm">aws --endpoint-url http://localhost:4747 dynamodb create-table ...</code>
          </CardContent>
        </Card>
      ) : (
        <Card>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Type</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {resources?.map((r) => (
                <TableRow key={r.id}>
                  <TableCell className="font-medium">{r.name}</TableCell>
                  <TableCell><Badge variant="secondary">{r.type}</Badge></TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </Card>
      )}
    </div>
  );
}
