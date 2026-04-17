"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const navItems = [
  { href: "/", label: "Home", icon: "Home" },
  { href: "/services/s3", label: "S3", icon: "S3" },
  { href: "/services/sqs", label: "SQS", icon: "SQS" },
  { href: "/services/dynamodb", label: "DynamoDB", icon: "DB" },
  { href: "/services/lambda", label: "Lambda", icon: "Fn" },
  { href: "/services/iam", label: "IAM", icon: "IAM" },
  { href: "/logs", label: "Logs", icon: "Log" },
  { href: "/settings", label: "Settings", icon: "Cfg" },
];

export function Sidebar() {
  const pathname = usePathname();

  return (
    <aside className="w-60 bg-gray-900 text-gray-100 min-h-screen p-4 shrink-0">
      <h1 className="text-xl font-bold mb-6 px-2">DevCloud</h1>
      <nav className="space-y-1">
        {navItems.map((item) => {
          const active = pathname === item.href;
          return (
            <Link
              key={item.href}
              href={item.href}
              className={`flex items-center gap-3 px-3 py-2 rounded-md text-sm font-medium transition-colors ${
                active
                  ? "bg-gray-700 text-white"
                  : "text-gray-400 hover:text-white hover:bg-gray-800"
              }`}
            >
              <span className="w-8 text-xs font-mono text-center opacity-60">
                {item.icon}
              </span>
              <span>{item.label}</span>
            </Link>
          );
        })}
      </nav>
    </aside>
  );
}
