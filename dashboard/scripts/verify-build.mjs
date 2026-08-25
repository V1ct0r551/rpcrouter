/* global URL */
import { readFile } from 'node:fs/promises';

const index = await readFile(new URL('../dist/index.html', import.meta.url), 'utf8');
if (!/\/(?:dashboard)\/assets\//.test(index)) {
  throw new Error('built index.html must reference /dashboard/assets/');
}
