/**
 * A JSON value in a stream, which can be consumed exactly once using one of
 * its methods. Values must be consumed in stream order: advancing a parent
 * iterator past a value skips whatever part of it was not consumed, after
 * which the value can no longer be used.
 *
 * Skipped parts are only checked for balanced brackets and strings, not fully
 * validated.
 */
export interface JsonStValue {
  /** Parses the whole value, like `JSON.parse`. */
  parse(): Promise<unknown>;
  /** Iterates over the decoded contents of a string as it arrives. */
  stringChunks(): AsyncIterable<string>;
  /** Iterates over the elements of an array as they arrive. */
  arrayValues(): AsyncIterable<JsonStValue>;
  /** Iterates over the members of an object as they arrive. */
  objectEntries(): AsyncIterable<[key: JsonStValue, value: JsonStValue]>;
}

/**
 * Iterates over the whitespace-separated JSON values in a stream of UTF-8
 * bytes or text. Chunks may be split anywhere, even inside a character.
 */
export function parseValues(stream: AsyncIterable<Uint8Array | string>): AsyncIterable<JsonStValue> {
  const input = createInput(stream[Symbol.asyncIterator]());
  let value: Value | undefined;
  const step = (): Step<Value> => {
    const finished = value?.finish();
    if (finished) return finished.then(step);
    const c = peek(input);
    if (c === MORE) return more(input).then(step);
    if (c === EOF) return undefined;
    return value = new Value(input);
  };
  return new Items(step, async () => {
    // The consumer may stop iterating while a parse is in progress, as in
    // `for await (const v of parseValues(s)) return v.parse();`
    await value?.parsed();
    await input.chunks.return?.();
  });
}

/**
 * Returns the next item, or undefined at the end. Returns a promise only when
 * it has to wait for input.
 */
type Step<T> = T | undefined | Promise<T | undefined>;

/**
 * An async iterator over the items returned by a step function. Unlike an
 * async generator, it costs no extra await per item, which matters because
 * items are often small.
 */
class Items<T> implements AsyncIterableIterator<T> {
  #step: () => Step<T>;
  #close: (() => Promise<void>) | undefined;
  #done = false;

  /** `close` is called once iteration ends for any reason. */
  constructor(step: () => Step<T>, close?: () => Promise<void>) {
    this.#step = step;
    this.#close = close;
  }

  next(): Promise<IteratorResult<T>> {
    if (this.#done) return Promise.resolve({ done: true, value: undefined });
    try {
      const item = this.#step();
      return item instanceof Promise
        ? item.then(item => this.#result(item), error => this.#fail(error))
        : this.#result(item);
    } catch (error) {
      return this.#fail(error);
    }
  }

  return(): Promise<IteratorResult<T>> {
    return this.#end();
  }

  [Symbol.asyncIterator](): this {
    return this;
  }

  #result(item: T | undefined): Promise<IteratorResult<T>> {
    return item === undefined ? this.#end() : Promise.resolve({ done: false, value: item });
  }

  async #fail(error: unknown): Promise<never> {
    await this.#end();
    throw error;
  }

  async #end(): Promise<IteratorResult<T>> {
    if (!this.#done) {
      this.#done = true;
      await this.#close?.();
    }
    return { done: true, value: undefined };
  }
}

const EOF = -1;
/** Returned by `peek` when it needs to wait for more input. */
const MORE = -2;
const QUOTE = 34, COMMA = 44, COLON = 58, BACKSLASH = 92;
const LBRACKET = 91, RBRACKET = 93, LBRACE = 123, RBRACE = 125;

function unexpected(c: number): SyntaxError {
  return new SyntaxError(c === EOF
    ? 'Unexpected end of JSON input'
    : `Unexpected token '${String.fromCharCode(c)}' in JSON`);
}

const encoder = new TextEncoder();
// Keeps a leading U+FEFF, which would otherwise be dropped from string contents.
const decoder = new TextDecoder('utf-8', { ignoreBOM: true });

// INPUT //////////////////////////////////////////////////////////////////////

// The stream is buffered as bytes in memory and tokenized in one pass. A token
// is the address of a byte that starts something: a bracket, comma, or colon,
// either quote of a string, or the first byte of a number, boolean, or null.
// The kind of a token is the byte it points to. A number, boolean, or null
// ends at the next token or the end of the stream. The token of a closing
// quote has the SPECIAL bit set if the string has a backslash or a control
// character, so it must be unescaped or rejected rather than just decoded.

const SPECIAL = 0x80000000;
const ADDRESS = 0x7fffffff;

/**
 * Tokenizer state: inside a string, after an odd number of backslashes,
 * inside a scalar, inside a string that has had a special character.
 */
const IN_STRING = 1;

/**
 * Tokenizes the bytes at [pos, end) starting in `state`, writing the tokens
 * as 32-bit words at `out`. Returns the number of tokens times 16 plus the
 * state at `end`.
 */
type Tokenize = (state: number, pos: number, end: number, out: number) => number;

/** Words in the token region at the start of memory, followed by bytes. */
const TOKENS = 16384;
const BYTES = TOKENS * 4;
/** Bytes past the buffered ones that the tokenizer may read. */
const SLACK = 64;
const PAGE = 65536;
const NONE = Infinity;

/**
 * The bytes of the stream and their tokens, hiding chunk boundaries. Fields
 * holding addresses (`len`, `scanned`, `mark`) are updated when bytes move.
 */
type Input = {
  chunks: AsyncIterator<Uint8Array | string>;
  memory: WebAssembly.Memory;
  /** Views of memory, renewed when it grows. */
  bytes: Uint8Array;
  tokens: Uint32Array;
  tokenize: Tokenize;
  /** End of the buffered bytes. */
  len: number;
  /** End of the tokenized bytes, and the tokenizer state there. */
  scanned: number;
  state: number;
  /** Index of the next token, and the number of tokens in the region. */
  cursor: number;
  count: number;
  /** The earliest byte the consumer still needs, or NONE. */
  mark: number;
  ended: boolean;
  /** A high surrogate held back from the end of a text chunk. */
  surrogate: string;
  utf16Length: (pos: number, end: number) => number;
  /**
   * The bytes at [textStart, textEnd) decoded, and a byte address in that
   * range with the index of its character in the text.
   */
  text: string;
  textStart: number;
  textEnd: number;
  textByte: number;
  textIndex: number;
};

function createInput(chunks: AsyncIterator<Uint8Array | string>): Input {
  const memory = new WebAssembly.Memory({ initial: 4 });
  const input: Input = {
    chunks, memory, bytes: new Uint8Array(), tokens: new Uint32Array(),
    ...tokenizer(memory),
    len: BYTES, scanned: BYTES, state: 0, cursor: 0, count: 0, mark: NONE,
    ended: false, surrogate: '', text: '', textStart: 0, textEnd: 0, textByte: 0, textIndex: 0,
  };
  renewViews(input);
  return input;
}

function renewViews(input: Input): void {
  input.bytes = new Uint8Array(input.memory.buffer);
  input.tokens = new Uint32Array(input.memory.buffer, 0, TOKENS);
}

// Functions that may have to wait for input return a promise only when they
// do, to avoid the cost of awaiting when the input is already buffered.

/**
 * Returns the kind of the next token, EOF, or MORE if the buffered input ran
 * out (see more).
 */
function peek(input: Input): number {
  if (input.cursor >= input.count && !scan(input)) {
    if (!input.ended) return MORE;
    if (input.state & IN_STRING) throw unexpected(EOF);
    return EOF;
  }
  return input.bytes[input.tokens[input.cursor]! & ADDRESS]!;
}

/**
 * Tokenizes buffered bytes until the token at the cursor exists. Returns false
 * if the buffered bytes ran out first. The cursor may be past the tokens
 * found so far, to skip tokens that haven't been found yet.
 */
function scan(input: Input): boolean {
  while (input.cursor >= input.count) {
    if (input.scanned === input.len) return false;
    // Every token has been consumed, so the region can be reused.
    input.cursor -= input.count;
    input.count = 0;
    const end = Math.min(input.len, input.scanned + TOKENS);
    const result = input.tokenize(input.state, input.scanned, end, 0);
    input.count = result >>> 4;
    input.state = result & 15;
    input.scanned = end;
  }
  return true;
}

/** Waits for the next chunk. */
async function more(input: Input): Promise<void> {
  const { done, value } = await input.chunks.next();
  if (!done) {
    append(input, value);
  } else {
    input.ended = true;
    if (input.surrogate) append(input, '');
  }
}

function append(input: Input, chunk: Uint8Array | string): void {
  if (typeof chunk === 'string') {
    // A surrogate pair split across chunks can't be encoded separately.
    chunk = input.surrogate + chunk;
    input.surrogate = '';
    const last = chunk.charCodeAt(chunk.length - 1);
    if (last >= 0xd800 && last < 0xdc00 && !input.ended) {
      input.surrogate = chunk.slice(-1);
      chunk = chunk.slice(0, -1);
    }
    reserve(input, chunk.length * 3);
    input.len += encoder.encodeInto(chunk, input.bytes.subarray(input.len)).written;
  } else {
    reserve(input, chunk.length);
    input.bytes.set(chunk, input.len);
    input.len += chunk.length;
  }
}

/**
 * Makes room for `n` more bytes, dropping the bytes that are no longer needed
 * or growing the memory. Must be called with every token consumed.
 */
function reserve(input: Input, n: number): void {
  n += SLACK;
  if (input.len + n <= input.bytes.length) return;
  const keep = Math.min(input.mark, input.scanned);
  const shift = keep - BYTES;
  input.bytes.copyWithin(BYTES, keep, input.len);
  input.len -= shift;
  input.scanned -= shift;
  input.mark -= shift;
  input.textEnd = 0;
  const missing = input.len + n - input.bytes.length;
  if (missing > 0) {
    input.memory.grow(Math.ceil(Math.max(missing, input.bytes.length) / PAGE));
    renewViews(input);
  }
}

/**
 * Consumes the value that starts at the next token, returning the address
 * right after it, with the SPECIAL bit for a string. Returns a promise if it
 * has to wait for input.
 */
function consumeValue(input: Input): number | Promise<number> {
  const c = input.bytes[input.tokens[input.cursor++]!]!;
  if (c === LBRACKET || c === LBRACE) return exitContainer(input, 1);
  if (c === QUOTE) return exitString(input);
  return scalarEnd(input);
}

/**
 * Consumes the closing quote of a string, returning the address after it,
 * with the SPECIAL bit of the closing quote.
 */
function exitString(input: Input): number | Promise<number> {
  // Inside a string the only token is the closing quote, and peek fails if
  // the input ends first.
  if (peek(input) === MORE) return more(input).then(() => exitString(input));
  return input.tokens[input.cursor++]! + 1;
}

/** Returns the end of the scalar consumed last, which is the next token. */
function scalarEnd(input: Input): number | Promise<number> {
  const c = peek(input);
  if (c === MORE) return more(input).then(() => scalarEnd(input));
  return c === EOF ? input.len : input.tokens[input.cursor]!;
}

/**
 * Consumes tokens until `depth` open brackets have been closed, returning the
 * address after the last one.
 */
function exitContainer(input: Input, depth: number): number | Promise<number> {
  for (;;) {
    const { bytes, tokens, count } = input;
    for (let i = input.cursor; i < count; i++) {
      const c = bytes[tokens[i]! & ADDRESS]!;
      if (c === LBRACKET || c === LBRACE) {
        depth++;
      } else if ((c === RBRACKET || c === RBRACE) && --depth === 0) {
        input.cursor = i + 1;
        return tokens[i]! + 1;
      }
    }
    input.cursor = Math.max(input.cursor, count);
    const c = peek(input);
    if (c === EOF) throw unexpected(EOF);
    if (c === MORE) return more(input).then(() => exitContainer(input, depth));
  }
}

/**
 * Consumes the value that starts at the next token, returning it parsed.
 * Returns a promise if it has to wait for input.
 */
function parseValue(input: Input): unknown {
  input.mark = input.tokens[input.cursor]!;
  const end = consumeValue(input);
  return typeof end === 'number' ? parseMarked(input, end) : end.then(end => parseMarked(input, end));
}

function parseMarked(input: Input, end: number): unknown {
  const { bytes, mark } = input;
  input.mark = NONE;
  // Strings aren't sliced from the decoded text, which would keep it alive.
  return bytes[mark] === QUOTE
    ? decodeString(bytes, mark + 1, (end & ADDRESS) - 1, end >= SPECIAL)
    : JSON.parse(valueText(input, mark, end));
}

/**
 * Returns the bytes at [start, end), which start and end at characters,
 * decoded. The buffered bytes are decoded at once, avoiding the fixed cost of
 * decoding each value, and values are sliced from the text. Values are read
 * in order, so byte addresses are mapped to text indices incrementally.
 */
function valueText(input: Input, start: number, end: number): string {
  // Short values are cheaper to decode in JavaScript than to map, and no
  // other value shares the text of a value that ends the buffered bytes.
  if (end - start <= 16 || end === input.len) return decodeText(input.bytes, start, end);
  if (start < input.textStart || end > input.textEnd) {
    // A character split at the end of the buffered bytes comes after `end`,
    // so how it is decoded doesn't matter.
    const text = decoder.decode(input.bytes.subarray(start, input.len));
    // Invalid UTF-8 is decoded in a way the mapping doesn't follow.
    input.text = text.length === input.utf16Length(start, input.len) ? text : '';
    input.textStart = input.textByte = start;
    input.textEnd = input.len;
    input.textIndex = 0;
  }
  if (input.text === '') return decodeText(input.bytes, start, end);
  if (start < input.textByte) {
    input.textByte = input.textStart;
    input.textIndex = 0;
  }
  const from = input.textIndex + input.utf16Length(input.textByte, start);
  const to = from + input.utf16Length(start, end);
  input.textByte = end;
  input.textIndex = to;
  return input.text.slice(from, to);
}

/**
 * Decodes the contents of a complete string at [start, end), parsing it if
 * it has `special` characters, to unescape them or reject control characters.
 */
function decodeString(bytes: Uint8Array, start: number, end: number, special: boolean): string {
  const raw = decodeText(bytes, start, end);
  return special ? JSON.parse('"' + raw + '"') : raw;
}

/**
 * Decodes the UTF-8 bytes at [start, end). Short ASCII text is decoded in
 * JavaScript, avoiding the fixed cost of calling TextDecoder.
 */
function decodeText(bytes: Uint8Array, start: number, end: number): string {
  if (end - start <= 16) {
    let text = '';
    for (let i = start; i < end; i++) {
      const c = bytes[i]!;
      if (c >= 0x80) return decoder.decode(bytes.subarray(start, end));
      text += String.fromCharCode(c);
    }
    return text;
  }
  return decoder.decode(bytes.subarray(start, end));
}

/** The tokenizer in WebAssembly, built from tokenize.wat by `npm run wasm`. */
const TOKENIZER = 'AGFzbQEAAAABGANgAX8Ffn5+fn5gBH9/f38Bf2ACf38BfwIPAQNlbnYGbWVtb3J5AgABAwQDAAECBxoCCHRva2VuaXplAAELdXRmMTZMZW5ndGgAAgq6BwPkAQQBfwF+A3sFfgNAIAAgAWr9AAQAIQMgA0Eg/Q/9UCEEIANBLP0P/SMgA0E6/Q/9I/1QIARB+wD9D/0jIARB/QD9D/0j/VD9UCEFIAGtIQIgBiADQSL9D/0j/WStIAKGhCEGIAcgA0HcAP0P/SP9ZK0gAoaEIQcgCiADQSD9D/0m/WStIAKGhCEKIAggBf1krSAChoQhCCAJIAUgA0Eg/Q/9IyADQQn9D/0j/VAgA0EK/Q/9IyADQQ39D/0j/VD9UP1Q/WStIAKGhCEJIAFBEGoiAUHAAEkNAAsgBiAHIAggCSAKC98EAwJ/FH4BfyADIQQgAEEBca0hDCAAQQF2QQFxrSENIABBAnZBAXGtIQ4gAEEDdq0hFwJAA0AgASACTw0BIAIgAWshBSAFQcAATwRAQcAAIQVCfyEHBUIBIAWthkIBfSEHCyAFQQFrrSEGIAEQACEWIQshCiEJIQggCSAHgyEJIBYgCYQgB4MhFiAJIAlCAYZCf4WDIQ9C1arVqtWq1arVACANhSEQIAkgDyAQQn+Fg3whESAJQn+FIAkgDyAQg3xCqtWq1arVqtWqf4MgESANhELVqtWq1arVqtUAg4SDIRIgESAJVK0gEiAFrYhCAYMgBUHAAEYbIQ0gCCAHgyASQn+FgyEIIAghEyATIBNCAYaFIRMgEyATQgKGhSETIBMgE0IEhoUhEyATIBNCCIaFIRMgEyATQhCGhSETIBMgE0IghoUhEyATQgAgDH2FIRMgEyAGiEIBgyEMIAcgCCALhCAThEJ/hYMhFCAIIAogB4MgE0J/hYOEIBQgFEIBhiAOhEJ/hYOEIRUgFCAGiEIBgyEOQgAhGAJAA0AgFVANASAVeiEZQQAhGiAIIBmIQgGDpwRAIBMgGYhCAYOnBEBCACEXIBlCAXwhGAUgF6dCACAWQgEgGYZCAX1CASAYhkIBfUJ/hYODUnIhGkIAIRcLCyADIAEgGadqIBpBH3RyNgIAIANBBGohAyAVIBVCAX2DIRUMAAsLIAynIBhCwABUcQRAIBdCACAWIBiIUq2EIRcLIAFBwABqIQEMAAsLIAMgBGtBAnQgDCANQgGGhCAOQgKGIBcgDINCA4aEhKdyC3EDAX8BewJ/AkADQCAAIAFPDQEgAP0ABAAhAyABIABrIQVB//8DQQEgBXRBAWsgBUEQTxshBCACIARBfyADQcAB/Q/9TkGAAf0P/SP9ZHNxaSAEIANB8AH9D/0s/WRxaWpqIQIgAEEQaiEADAALCyACCw==';

const tokenizerModule = new WebAssembly.Module(Uint8Array.from(atob(TOKENIZER), c => c.charCodeAt(0)));

/** Instantiates the tokenizer to operate on `memory`. */
function tokenizer(memory: WebAssembly.Memory): Pick<Input, 'tokenize' | 'utf16Length'> {
  const { tokenize, utf16Length } = new WebAssembly.Instance(tokenizerModule, { env: { memory } }).exports;
  return { tokenize: tokenize as Input['tokenize'], utf16Length: utf16Length as Input['utf16Length'] };
}

// VALUES /////////////////////////////////////////////////////////////////////

const PENDING = 0, ACTIVE = 1, DONE = 2;

class Value implements JsonStValue {
  #input: Input;
  /** The key of an object member, which precedes this value in the stream. */
  #key: Value | undefined;
  #state = PENDING;
  /** The kind of the first token of the value, once known. */
  #first = EOF;
  /** Whether a string, array, or object has been entered. */
  #entered = false;
  /** The element last yielded while iterating an array or object. */
  #child: Value | undefined;
  /** The parse in progress, while it waits for input. */
  #capture: Promise<unknown> | undefined;

  constructor(input: Input, key?: Value) {
    this.#input = input;
    this.#key = key;
  }

  async parse(): Promise<unknown> {
    this.#claim();
    const started = this.#start();
    let value = started ? started.then(() => parseValue(this.#input)) : parseValue(this.#input);
    if (value instanceof Promise) {
      this.#capture = value;
      value = await value;
    }
    this.#state = DONE;
    return value;
  }

  stringChunks(): AsyncIterable<string> {
    const input = this.#input;
    let entered = false;
    let closed = false;
    /** Decodes UTF-8 across segments, once the string spans several. */
    let partial: TextDecoder | undefined;
    /** An incomplete escape sequence at the end of the last segment. */
    let held = '';
    const step = (): Step<string> => {
      if (!entered) {
        entered = true;
        const started = this.#enter(QUOTE);
        if (started) return started.then(step);
      }
      if (closed) return undefined;
      this.#checkActive();
      for (;;) {
        // The contents start at the mark. The next token is the closing
        // quote, if it has arrived.
        const c = peek(input);
        if (c === MORE && input.mark === input.len) return more(input).then(step);
        closed = c !== MORE;
        const token = closed ? input.tokens[input.cursor]! : input.len;
        const end = token & ADDRESS;
        if (closed && partial === undefined) {
          // The whole string arrived at once.
          const text = decodeString(input.bytes, input.mark, end, token >= SPECIAL);
          input.cursor++;
          input.mark = NONE;
          this.#state = DONE;
          return text.length > 0 ? text : undefined;
        }
        partial ??= new TextDecoder('utf-8', { ignoreBOM: true });
        let raw = held + partial.decode(input.bytes.subarray(input.mark, end), { stream: !closed });
        held = '';
        if (closed) {
          input.cursor++;
          input.mark = NONE;
          this.#state = DONE;
        } else {
          input.mark = end;
          const complete = completeEscapesEnd(raw);
          held = raw.slice(complete);
          raw = raw.slice(0, complete);
        }
        if (raw.length > 0) return unescape(raw);
        if (closed) return undefined;
      }
    };
    return new Items(step);
  }

  arrayValues(): AsyncIterable<JsonStValue> {
    const input = this.#input;
    let entered = false;
    let child: Value | undefined;
    const step = (): Step<Value> => {
      if (!entered) {
        entered = true;
        const started = this.#enter(LBRACKET);
        if (started) return started.then(step);
      }
      this.#checkActive();
      const finished = child?.finish();
      if (finished) return finished.then(step);
      const c = peek(input);
      if (c === MORE) return more(input).then(step);
      if (c === RBRACKET) {
        input.cursor++;
        this.#state = DONE;
        return undefined;
      }
      if (child !== undefined) {
        if (c !== COMMA) throw unexpected(c);
        input.cursor++;
      }
      return child = this.#child = new Value(input);
    };
    return new Items(step);
  }

  objectEntries(): AsyncIterable<[key: JsonStValue, value: JsonStValue]> {
    const input = this.#input;
    let entered = false;
    let value: Value | undefined;
    let afterComma = false;
    const step = (): Step<[Value, Value]> => {
      if (!entered) {
        entered = true;
        const started = this.#enter(LBRACE);
        if (started) return started.then(step);
      }
      this.#checkActive();
      if (value !== undefined) {
        const finished = value.finish();
        if (finished) return finished.then(step);
        const c = peek(input);
        if (c === MORE) return more(input).then(step);
        if (c === COMMA) {
          input.cursor++;
          value = undefined;
          afterComma = true;
        } else if (c !== RBRACE) {
          throw unexpected(c);
        }
      }
      const c = peek(input);
      if (c === MORE) return more(input).then(step);
      if (c === RBRACE && !afterComma) {
        input.cursor++;
        this.#state = DONE;
        return undefined;
      }
      if (c !== QUOTE) throw unexpected(c);
      afterComma = false;
      const key = new Value(input);
      value = this.#child = new Value(input, key);
      return [key, value];
    };
    return new Items(step);
  }

  /**
   * Consumes whatever part of the value has not been consumed yet. Returns a
   * promise if it has to wait for input.
   */
  finish(): Promise<unknown> | undefined {
    const state = this.#state;
    if (state === DONE) return;
    this.#state = DONE;
    const input = this.#input;
    let result: number | Promise<unknown> | undefined;
    if (this.#capture !== undefined) {
      result = this.parsed();
    } else if (!this.#entered) {
      // Nothing has been consumed yet (an iterator may be about to start).
      const started = this.#start();
      result = started ? started.then(() => consumeValue(input)) : consumeValue(input);
    } else if (this.#first === QUOTE) {
      input.mark = NONE;
      result = exitString(input);
    } else {
      const finished = this.#child?.finish();
      result = finished ? finished.then(() => exitContainer(input, 1)) : exitContainer(input, 1);
    }
    return typeof result === 'number' ? undefined : result;
  }

  /** Waits until a parse in progress, if any, has consumed its input. */
  async parsed(): Promise<void> {
    await this.#capture?.catch(() => {});
  }

  #claim(): void {
    if (this.#state !== PENDING) throw new Error('JSON value already consumed');
    this.#state = ACTIVE;
  }

  #checkActive(): void {
    if (this.#state !== ACTIVE) throw new Error('JSON value skipped before it was consumed');
  }

  /**
   * Moves the input to the first token of the value. Returns a promise if
   * it has to wait for input.
   */
  #start(): Promise<void> | undefined {
    if (this.#first !== EOF) return;
    const input = this.#input;
    const key = this.#key;
    if (key !== undefined) {
      const finished = key.finish();
      if (finished) return finished.then(() => this.#start());
      const c = peek(input);
      if (c === MORE) return more(input).then(() => this.#start());
      if (c !== COLON) throw unexpected(c);
      input.cursor++;
      this.#key = undefined;
    }
    const c = peek(input);
    if (c === MORE) return more(input).then(() => this.#start());
    if (c === EOF || c === COMMA || c === COLON || c === RBRACKET || c === RBRACE) {
      throw unexpected(c);
    }
    this.#first = c;
  }

  /**
   * Enters the value, which must start with `open`. Returns a promise if it
   * has to wait for input.
   */
  #enter(open: number): Promise<void> | undefined {
    this.#claim();
    const started = this.#start();
    if (started) return started.then(() => this.#enterStarted(open));
    this.#enterStarted(open);
  }

  #enterStarted(open: number): void {
    if (this.#first !== open) {
      this.#state = PENDING;
      throw new TypeError(`Expected JSON ${TYPE_NAMES[open]}, found ${typeName(this.#first)}`);
    }
    const input = this.#input;
    // For strings, the mark is where the contents start.
    if (open === QUOTE) input.mark = input.tokens[input.cursor]! + 1;
    input.cursor++;
    this.#entered = true;
  }
}

const TYPE_NAMES: Record<number, string> = { [QUOTE]: 'string', [LBRACKET]: 'array', [LBRACE]: 'object' };

function typeName(first: number): string {
  return TYPE_NAMES[first] ?? 'number, boolean, or null';
}

/**
 * Decodes the escapes in raw contents of a string with complete escapes.
 * Used for pieces of a string whose closing quote hasn't been tokenized yet.
 */
function unescape(raw: string): string {
  // Most strings have nothing to decode or reject.
  return /[\\\u0000-\u001f]/.test(raw) ? JSON.parse('"' + raw + '"') : raw;
}

/**
 * Returns the length of the longest prefix of raw string contents that does
 * not end in an incomplete escape sequence.
 */
function completeEscapesEnd(raw: string): number {
  const backslash = raw.lastIndexOf('\\', raw.length - 1);
  if (backslash === -1 || backslash < raw.length - 6) return raw.length;
  let start = backslash;
  while (start > 0 && raw.charCodeAt(start - 1) === BACKSLASH) start--;
  if ((backslash - start) % 2 === 1) return raw.length; // The last backslash is escaped.
  const tail = raw.length - backslash - 1;
  const complete = tail > 0 && (raw[backslash + 1] !== 'u' || tail >= 5);
  return complete ? raw.length : backslash;
}
