import { readFileSync } from 'node:fs';
const url = 'http://127.0.0.1:8080/2015-03-31/functions/function/invocations';
if (process.argv[2] === '--ready') {
  for (;;) {
    try { await fetch(url, { signal: AbortSignal.timeout(1000) }); break; }
    catch { await new Promise(resolve => setTimeout(resolve, 50)); }
  }
} else {
  const response = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: readFileSync(0) });
  if (!response.ok) throw new Error(`Runtime HTTP ${response.status}`);
  process.stdout.write(await response.text());
}
