// Usage: node bench/bench.ts [name-filter] [--base <git-ref>]
//
// Reports the throughput of each workload in MB/s of JSON text, using the
// CPU time of the main thread, which is less noisy than wall time. Each
// measurement runs in a separate process to isolate workloads from each other.
//
// With --base, compares the working tree against the library at a git ref,
// alternating between the two so that drift in the machine affects both.

import { execFileSync } from 'node:child_process';
import { writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { JsonStValue } from '../src/index.ts';

const { parseValues }: typeof import('../src/index.ts') =
  await import(process.env.JSONST_LIB ?? '../src/index.ts');

let seed = 1;
function random(): number {
  seed = (seed * 1103515245 + 12345) % 2147483648;
  return seed / 2147483648;
}
function pick<T>(items: T[]): T {
  return items[Math.floor(random() * items.length)]!;
}
const words = ['lorem', 'ipsum', 'dolor', 'sit', 'amet', 'café', 'naïve', 'quote"d', 'back\\slash', 'line\nbreak', '😀', 'tab\t'];
function text(n: number): string {
  return Array.from({ length: n }, () => pick(words)).join(' ');
}
function record(id: number) {
  return {
    id,
    name: text(2),
    email: `user${id}@example.com`,
    active: random() < 0.5,
    score: Math.round(random() * 1e6) / 100,
    tags: Array.from({ length: Math.floor(random() * 5) }, () => pick(words)),
    address: { street: text(3), city: pick(words), zip: String(10000 + id % 90000) },
    bio: text(Math.floor(random() * 40)),
  };
}

const SIZE = 10e6;
const records: ReturnType<typeof record>[] = [];
for (let id = 0, size = 0; size < SIZE; id++) {
  const r = record(id);
  records.push(r);
  size += JSON.stringify(r).length;
}
const recordsJson = JSON.stringify(records);
const ndjson = records.map(r => JSON.stringify(r)).join('\n');
const longStrings = JSON.stringify(Array.from({ length: 20 }, () => text(SIZE / 20 / 6)));

function chunks(text: string, size: number): string[] {
  const result = [];
  for (let i = 0; i < text.length; i += size) result.push(text.slice(i, i + size));
  return result;
}

async function* stream(chunks: string[]): AsyncGenerator<string> {
  yield* chunks;
}

async function read(value: JsonStValue, expected: unknown): Promise<unknown> {
  if (typeof expected === 'string') {
    let s = '';
    for await (const chunk of value.stringChunks()) s += chunk;
    return s;
  } else if (Array.isArray(expected)) {
    const items = [];
    for await (const item of value.arrayValues()) items.push(await read(item, expected[items.length]));
    return items;
  } else if (expected !== null && typeof expected === 'object') {
    const obj: Record<string, unknown> = {};
    for await (const [k, v] of value.objectEntries()) {
      const key = await read(k, '') as string;
      obj[key] = await read(v, (expected as Record<string, unknown>)[key]);
    }
    return obj;
  } else {
    return value.parse();
  }
}

type Workload = { name: string; text: string; chunkSize?: number; run: (values: AsyncIterable<JsonStValue>) => Promise<unknown> };

const workloads: Workload[] = [
  {
    name: 'JSON.parse (reference)',
    text: recordsJson,
    run: async () => JSON.parse(recordsJson),
  },
  {
    name: 'parse whole document',
    text: recordsJson,
    run: async values => {
      for await (const v of values) return await v.parse();
    },
  },
  {
    name: 'parse each array element',
    text: recordsJson,
    run: async values => {
      const items = [];
      for await (const v of values) for await (const item of v.arrayValues()) items.push(await item.parse());
      return items;
    },
  },
  {
    name: 'parse each array element, 1 KB chunks',
    text: recordsJson,
    chunkSize: 1024,
    run: async values => {
      const items = [];
      for await (const v of values) for await (const item of v.arrayValues()) items.push(await item.parse());
      return items;
    },
  },
  {
    name: 'parse each NDJSON line',
    text: ndjson,
    run: async values => {
      const items = [];
      for await (const v of values) items.push(await v.parse());
      return items;
    },
  },
  {
    name: 'select one field per element',
    text: recordsJson,
    run: async values => {
      const ids = [];
      for await (const v of values) {
        for await (const item of v.arrayValues()) {
          for await (const [k, val] of item.objectEntries()) {
            if (await k.parse() === 'id') ids.push(await val.parse());
          }
        }
      }
      return ids;
    },
  },
  {
    name: 'stream everything',
    text: recordsJson,
    run: async values => {
      for await (const v of values) return await read(v, records);
    },
  },
  {
    name: 'skip everything',
    text: recordsJson,
    run: async values => {
      for await (const _ of values);
    },
  },
  {
    name: 'stream long strings',
    text: longStrings,
    run: async values => {
      let n = 0;
      for await (const v of values) {
        for await (const s of v.arrayValues()) for await (const chunk of s.stringChunks()) n += chunk.length;
      }
      return n;
    },
  },
];

const RUNS = 7;
const ROUNDS = 5;

/** Runs a workload in a child process and returns its throughput in MB/s. */
function measure(name: string, lib?: string): number {
  const env = { ...process.env, JSONST_WORKLOAD: name, JSONST_LIB: lib };
  return Number(execFileSync(process.execPath, [import.meta.filename], { env, encoding: 'utf8' }));
}

function median(xs: number[]): number {
  return [...xs].sort((a, b) => a - b)[xs.length >> 1]!;
}

const only = process.env.JSONST_WORKLOAD;

if (only !== undefined) {
  const { text, chunkSize = 65536, run } = workloads.find(w => w.name === only)!;
  const input = chunks(text, chunkSize);
  let best = Infinity;
  for (let i = 0; i < RUNS; i++) {
    const start = process.threadCpuUsage();
    await run(parseValues(stream(input)));
    const { user, system } = process.threadCpuUsage(start);
    best = Math.min(best, (user + system) / 1e3);
  }
  console.log(text.length / 1e6 / (best / 1e3));
} else {
  const args = process.argv.slice(2);
  const baseIndex = args.indexOf('--base');
  const base = baseIndex === -1 ? undefined : args.splice(baseIndex, 2)[1]!;
  const filter = args[0] ?? '';
  let baseLib: string | undefined;
  if (base !== undefined) {
    baseLib = join(tmpdir(), `jsonst-base-${process.pid}.ts`);
    writeFileSync(baseLib, execFileSync('git', ['show', `${base}:src/index.ts`]));
    console.log(`${''.padEnd(40)} ${base.padStart(8)} ${'current'.padStart(8)} ${'change'.padStart(8)}`);
  }
  const fmt = (x: number) => x.toFixed(0).padStart(8);
  for (const { name } of workloads) {
    if (!name.includes(filter)) continue;
    if (baseLib === undefined) {
      console.log(`${name.padEnd(40)} ${fmt(measure(name))} MB/s`);
      continue;
    }
    const before = [], after = [];
    for (let i = 0; i < ROUNDS; i++) {
      before.push(measure(name, baseLib));
      after.push(measure(name));
    }
    const change = (median(after) / median(before) - 1) * 100;
    console.log(`${name.padEnd(40)} ${fmt(median(before))} ${fmt(median(after))} ${(change >= 0 ? '+' : '') + change.toFixed(1).padStart(6)}%`);
  }
}
