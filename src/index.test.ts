import { test } from 'node:test';
import assert from 'node:assert/strict';
import fc from 'fast-check';
import { parseValues, type JsonStValue } from './index.ts';

async function* stream(...chunks: string[]): AsyncGenerator<string> {
  yield* chunks;
}

function split(text: string, cuts: number[]): string[] {
  const points = [...new Set(cuts.map(c => c % (text.length + 1)))].sort((a, b) => a - b);
  return [...points, text.length].map((end, i) => text.slice(points[i - 1] ?? 0, end));
}

async function collect<T>(iter: AsyncIterable<T>): Promise<T[]> {
  const items = [];
  for await (const item of iter) items.push(item);
  return items;
}

/** Reads a value through the streaming methods, picking the method by trial. */
async function read(value: JsonStValue): Promise<unknown> {
  try {
    return (await collect(value.stringChunks())).join('');
  } catch (e) {
    if (!(e instanceof TypeError)) throw e;
  }
  try {
    const items = [];
    for await (const item of value.arrayValues()) items.push(await read(item));
    return items;
  } catch (e) {
    if (!(e instanceof TypeError)) throw e;
  }
  try {
    const obj: Record<string, unknown> = {};
    for await (const [k, v] of value.objectEntries()) {
      const key = await read(k);
      assert.equal(typeof key, 'string');
      Object.defineProperty(obj, key as string, { value: await read(v), enumerable: true });
    }
    return obj;
  } catch (e) {
    if (!(e instanceof TypeError)) throw e;
  }
  return value.parse();
}

async function readAll(chunks: string[], read: (v: JsonStValue) => Promise<unknown>): Promise<unknown[]> {
  const values = [];
  for await (const value of parseValues(stream(...chunks))) values.push(await read(value));
  return values;
}

const json = fc.record({
  value: fc.jsonValue({ stringUnit: 'binary' }),
  indent: fc.constantFrom(undefined, 1, '\t', '\r\n '),
}).map(({ value, indent }) => JSON.stringify(value, null, indent));

const cuts = fc.array(fc.nat(), { maxLength: 20 });

test('streaming methods agree with JSON.parse', async () => {
  await fc.assert(fc.asyncProperty(json, cuts, async (text, cuts) => {
    assert.deepEqual(await readAll(split(text, cuts), read), [JSON.parse(text)]);
  }));
});

test('parse agrees with JSON.parse', async () => {
  await fc.assert(fc.asyncProperty(json, cuts, async (text, cuts) => {
    assert.deepEqual(await readAll(split(text, cuts), v => v.parse()), [JSON.parse(text)]);
  }));
});

test('arbitrary input is accepted exactly when JSON.parse accepts it', async () => {
  const mutation = fc.tuple(fc.nat(), fc.nat(2), fc.constantFrom(...' "\\u0aZ[]{},:-.e+tfn'));
  const input = fc.oneof(
    fc.tuple(json, fc.array(mutation, { minLength: 1, maxLength: 3 })).map(([text, mutations]) => {
      for (const [pos, deleted, char] of mutations) {
        const i = pos % (text.length + 1);
        text = text.slice(0, i) + char + text.slice(i + deleted);
      }
      return text;
    }),
    fc.string({ unit: fc.constantFrom(...' "\\u0a[]{},:1') }),
  );
  await fc.assert(fc.asyncProperty(input, cuts, fc.boolean(), async (text, cuts, streaming) => {
    let expected: unknown[];
    try {
      expected = [JSON.parse(text)];
    } catch {
      expected = [];
    }
    let actual: unknown[];
    try {
      actual = await readAll(split(text, cuts), streaming ? read : v => v.parse());
    } catch (e) {
      assert(e instanceof SyntaxError, e as Error);
      actual = [];
    }
    if (actual.length > 1 && expected.length === 0) return; // Multiple top-level values.
    assert.deepEqual(actual, expected);
  }), { numRuns: 1000 });
});

test('unconsumed and partially consumed values are skipped', async () => {
  type Strategy = 'skip' | 'parse' | 'stream' | 'partial';
  const strategies = fc.array(fc.constantFrom<Strategy>('skip', 'parse', 'stream', 'partial'), { minLength: 1 });

  // Like read, but each value is skipped, parsed, streamed, or partially
  // streamed. Returns the parts that were consumed.
  async function consume(value: JsonStValue, plan: Iterator<Strategy>, expected: unknown): Promise<unknown> {
    const strategy = plan.next().value;
    if (strategy === 'skip') return undefined;
    if (strategy === 'parse') return value.parse();
    const partial = strategy === 'partial';
    if (typeof expected === 'string') {
      const chunks = [];
      for await (const chunk of value.stringChunks()) {
        chunks.push(chunk);
        if (partial) break;
      }
      return chunks.join('');
    } else if (Array.isArray(expected)) {
      const items = [];
      for await (const item of value.arrayValues()) {
        items.push(await consume(item, plan, expected[items.length]));
        if (partial) break;
      }
      return items;
    } else if (expected !== null && typeof expected === 'object') {
      const entries = [];
      for await (const [k, v] of value.objectEntries()) {
        const [key, val]: [string, unknown] = Object.entries(expected)[entries.length]!;
        entries.push([await consume(k, plan, key), await consume(v, plan, val)]);
        if (partial) break;
      }
      return entries;
    } else {
      return value.parse();
    }
  }

  function expectedParts(expected: unknown, plan: Iterator<Strategy>): unknown {
    const strategy = plan.next().value;
    if (strategy === 'skip') return undefined;
    if (strategy === 'parse') return expected;
    const partial = strategy === 'partial';
    if (typeof expected === 'string') {
      return partial ? undefined : expected; // A prefix; checked separately.
    } else if (Array.isArray(expected)) {
      return (partial ? expected.slice(0, 1) : expected).map(item => expectedParts(item, plan));
    } else if (expected !== null && typeof expected === 'object') {
      return (partial ? Object.entries(expected).slice(0, 1) : Object.entries(expected))
        .map(([k, v]) => [expectedParts(k, plan), expectedParts(v, plan)]);
    } else {
      return expected;
    }
  }

  function erasePartialStrings(actual: unknown, expected: unknown, plan: Iterator<Strategy>): unknown {
    const strategy = plan.next().value;
    if (strategy === 'skip' || strategy === 'parse') return actual;
    const partial = strategy === 'partial';
    if (typeof expected === 'string') {
      if (!partial) return actual;
      assert(expected.startsWith(actual as string));
      return undefined;
    } else if (Array.isArray(expected)) {
      return (actual as unknown[]).map((a, i) => erasePartialStrings(a, expected[i], plan));
    } else if (expected !== null && typeof expected === 'object') {
      const entries = Object.entries(expected);
      return (actual as [unknown, unknown][]).map(([k, v], i) =>
        [erasePartialStrings(k, entries[i]![0], plan), erasePartialStrings(v, entries[i]![1], plan)]);
    } else {
      return actual;
    }
  }

  // Round-trip through JSON to normalize values like -0.
  const values = fc.array(fc.jsonValue({ stringUnit: 'binary' }), { maxLength: 3 })
    .map(values => values.map(v => JSON.parse(JSON.stringify(v))));
  await fc.assert(fc.asyncProperty(values, cuts, strategies, async (values, cuts, plans) => {
    const text = values.map(v => JSON.stringify(v)).join('\n');
    const plan = (): Iterator<Strategy> => {
      let i = 0;
      return { next: () => ({ value: plans[i++ % plans.length]! }) };
    };
    const it = values[Symbol.iterator]();
    const actual = await readAll(split(text, cuts), v => consume(v, plan(), it.next().value));
    assert.deepEqual(
      actual.map((a, i) => erasePartialStrings(a, values[i], plan())),
      values.map(v => expectedParts(v, plan())),
    );
  }));
});

test('every split of tricky input', async () => {
  const text = String.raw`{"\"\\":["\\\"\u00e9\n","\ud83d\ude00\/",-1.5e3,true,{}],"":null}`;
  const expected = [JSON.parse(text)];
  for (let i = 0; i <= text.length; i++) {
    for (let j = i; j <= text.length; j++) {
      const chunks = split(text, [i, j]);
      assert.deepEqual(await readAll(chunks, read), expected, chunks.join('|'));
      assert.deepEqual(await readAll(chunks, v => v.parse()), expected, chunks.join('|'));
    }
  }
});

test('multiple top-level values', async () => {
  assert.deepEqual(await readAll(['1 "a"[]{}', 'null\n', '2'], v => v.parse()), [1, 'a', [], {}, null, 2]);
  assert.deepEqual(await readAll([' \n '], v => v.parse()), []);
});

test('string chunks follow the input chunks', async () => {
  for await (const value of parseValues(stream('"ab', 'c\\', 'n\\u00', 'e9"'))) {
    assert.deepEqual(await collect(value.stringChunks()), ['ab', 'c', '\n', 'é']);
  }
});

test('wrong type is a TypeError and leaves the value usable', async () => {
  for await (const value of parseValues(stream('[1]'))) {
    await assert.rejects(collect(value.objectEntries()), TypeError);
    await assert.rejects(collect(value.stringChunks()), TypeError);
    assert.deepEqual(await value.parse(), [1]);
  }
});

test('a value can be consumed only once', async () => {
  for await (const value of parseValues(stream('"a" "b"'))) {
    await value.parse();
    await assert.rejects(value.parse(), /already consumed/);
    await assert.rejects(collect(value.stringChunks()), /already consumed/);
  }
});

test('a skipped value can not be resumed', async () => {
  const values = [];
  for await (const value of parseValues(stream('"abc', 'def" 1'))) {
    const chunks = value.stringChunks()[Symbol.asyncIterator]();
    if (values.length === 0) assert.deepEqual(await chunks.next(), { done: false, value: 'abc' });
    values.push([value, chunks] as const);
  }
  const [[first, firstChunks], [second, secondChunks]] = values as [typeof values[0], typeof values[0]];
  await assert.rejects(firstChunks.next(), /skipped/);
  await assert.rejects(secondChunks.next(), /already consumed/);
  await assert.rejects(first.parse(), /already consumed/);
  await assert.rejects(second.parse(), /already consumed/);
});

test('parse may be awaited after advancing', async () => {
  const promises = [];
  for await (const value of parseValues(stream('[{"a":', '1},2,', '"x"]'))) {
    for await (const item of value.arrayValues()) promises.push(item.parse());
  }
  assert.deepEqual(await Promise.all(promises), [{ a: 1 }, 2, 'x']);
});

test('parse may be returned from the loop', async () => {
  async function first() {
    for await (const value of parseValues(stream('[1,', '2]'))) return value.parse();
  }
  assert.deepEqual(await first(), [1, 2]);
});

test('object values may be read before keys', async () => {
  for await (const value of parseValues(stream('{"a":1,"b":[2]}'))) {
    const result = [];
    for await (const [k, v] of value.objectEntries()) {
      result.push(await v.parse());
      await assert.rejects(k.parse(), /already consumed/);
    }
    assert.deepEqual(result, [1, [2]]);
  }
});

test('syntax errors', async () => {
  for (const text of ['[1,]', '[1 2]', '{"a" 1}', '{"a":1,}', '{1:2}', '[', '"abc', ']', '{"a"}', '[,1]', '"a\tb"', '{"\u0001":1}']) {
    await assert.rejects(readAll([text], read), SyntaxError, text);
    await assert.rejects(readAll([text], v => v.parse()), SyntaxError, text);
  }
});

test('stops reading the stream when iteration stops', async () => {
  let closed = false;
  async function* source() {
    try {
      yield '1 2 3';
    } finally {
      closed = true;
    }
  }
  for await (const _ of parseValues(source())) break;
  assert(closed);
});
