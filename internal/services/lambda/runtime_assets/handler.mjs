import { pathToFileURL } from 'node:url';
import { existsSync } from 'node:fs';

export async function handler(event, context) {
  try {
    const reference = process.env.DEVCLOUD_LAMBDA_HANDLER;
    const dot = reference.lastIndexOf('.');
    const module = reference.slice(0, dot);
    const method = reference.slice(dot + 1);
    const file = ['.js', '.mjs', '.cjs'].map(ext => `/var/task/${module}${ext}`).find(existsSync);
    if (!file) throw new Error(`Handler module not found: ${module}`);
    const loaded = await import(pathToFileURL(file).href);
    const target = loaded[method] ?? loaded.default?.[method];
    if (typeof target !== 'function') throw new Error(`Handler is not callable: ${reference}`);
    const payload = JSON.stringify(await target(event, context));
    if (payload === undefined) throw new TypeError('Handler result is not JSON serializable');
    return { success: true, payload };
  } catch (error) {
    console.error(error);
    return { success: false, error: { errorType: error?.name ?? 'Error', errorMessage: String(error?.message ?? error) } };
  }
}
