import ServicePageClient from "./ServicePageClient";

export function generateStaticParams() {
  return [
    { serviceId: "s3" },
    { serviceId: "sqs" },
    { serviceId: "dynamodb" },
    { serviceId: "lambda" },
    { serviceId: "iam" },
  ];
}

export default function ServicePage() {
  return <ServicePageClient />;
}
