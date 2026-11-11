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
  const p = createParser(stream[Symbol.asyncIterator]());
  return new Items(() => {
    const seq = next(p, ROOT, ROOT);
    if (seq instanceof Promise) return seq.then(seq => seq === undefined ? undefined : new Value(p, seq, 0));
    return seq === undefined ? undefined : new Value(p, seq, 0);
  }, () => close(p));
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

// TOKENIZER //////////////////////////////////////////////////////////////////

// The stream is tokenized in one pass, in slices. A token
// locates a byte that starts something: a bracket, comma, or colon, either
// quote of a string, or the first byte of a number, boolean, or null. The kind
// of a token is the byte it points to. A number, boolean, or null ends at the
// next token or the end of the stream. The token of a closing quote has the
// special bit set if the string has a backslash or a control character, so it
// must be unescaped or rejected rather than just decoded.
//
// Tokens are stored as 16-bit offsets from the start of their slice, with the
// special bit at bit 15. Input reads them as positions with the special bit at
// bit 31 (see token).

const OFFSET = 0x7fff;

/** The most bytes one call to tokenize may cover. */
const SLICE = 16384;
/** The number of bytes the tokenizer's memory holds. */
const BUFFER = 65536;

/**
 * Bit of the tokenizer state for being inside a string. The state is 4 bits
 * (see tokenize.wat), and 0 at the start of a stream.
 */
const IN_STRING = 1;

/**
 * A tokenizer with its own memory. Positions are indices into `bytes`.
 *
 * Tokenizing is a function of a state and bytes, and splitting the bytes
 * doesn't change the result: tokenizing A ++ B after state σ gives the tokens
 * of A, then those of B (offset by |A|) after the state that A ends in. Links
 * are the exception: they are hints, present only when both brackets fall in
 * the same call.
 */
type Tokenizer = {
  /** BUFFER bytes for the caller to fill with the stream. */
  bytes: Uint8Array;
  /** The tokens of the last call, as offsets from its `start`. */
  tokens: Uint16Array;
  /**
   * For the token of an opening bracket in the last call, the index of the
   * token of its closing bracket if that is in the same call, else 0.
   */
  links: Uint16Array;
  /**
   * Tokenizes bytes[start, end), at most SLICE of them, in the stream after
   * `state`. Returns the number of tokens times 16 plus the state at `end`.
   * The result depends only on the arguments and the bytes.
   */
  tokenize(state: number, start: number, end: number): number;
  /** The length in UTF-16 code units of the UTF-8 text at bytes[start, end). */
  utf16Length(start: number, end: number): number;
};

/** The tokenizer in WebAssembly, built from tokenize.wat by `npm run wasm`. */
const TOKENIZER = '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';

const tokenizerModule = new WebAssembly.Module(Uint8Array.from(atob(TOKENIZER), c => c.charCodeAt(0)));

function createTokenizer(): Tokenizer {
  // The layout of memory, which tokenize.wat assumes: the tokens, the links,
  // and the stack of open brackets, each SLICE 16-bit words, then the bytes,
  // and slack the tokenizer may read past them.
  const links = SLICE * 2, bytes = SLICE * 6, slack = 64;
  const memory = new WebAssembly.Memory({ initial: Math.ceil((bytes + BUFFER + slack) / 65536) });
  const exports = new WebAssembly.Instance(tokenizerModule, { env: { memory } }).exports;
  return {
    bytes: new Uint8Array(memory.buffer, bytes, BUFFER),
    tokens: new Uint16Array(memory.buffer, 0, SLICE),
    links: new Uint16Array(memory.buffer, links, SLICE),
    tokenize: exports.tokenize as Tokenizer['tokenize'],
    utf16Length: exports.utf16Length as Tokenizer['utf16Length'],
  };
}

// WINDOW /////////////////////////////////////////////////////////////////////

const NONE = Infinity;

/**
 * A stream of bytes S, buffered in the tokenizer's bytes as space allows: the
 * concatenation of the source's chunks, with text chunks encoded as UTF-8.
 * Positions are offsets in S. The fields before `source` are read-only for
 * other sections; `bytes[p - origin]` is S[p] for p in [origin, end).
 */
type Window = {
  /** The buffered bytes: bytes[i] is at position origin + i. */
  bytes: Uint8Array;
  origin: number;
  /** The position after the last buffered byte. */
  end: number;
  /** Whether `end` is the end of the stream. */
  ended: boolean;
  // The rest is private to this section.
  source: AsyncIterator<Uint8Array | string>;
  /** The part of the last chunk not buffered yet, from `pendingPos`. */
  pending: Uint8Array | string | undefined;
  pendingPos: number;
  /** A high surrogate held back from the end of a text chunk. */
  surrogate: string;
  /**
   * Kept bytes dropped from the buffer, from position `spillStart`. They are
   * joined and decoded with the rest of their range, because JSON.parse would
   * copy text joined from pieces.
   */
  spill: Uint8Array[];
  spillStart: number;
  utf16Length: Tokenizer['utf16Length'];
  /**
   * The bytes at [textStart, textEnd) decoded, and a position in that range
   * with the index of its character in the text.
   */
  text: string;
  textStart: number;
  textEnd: number;
  textByte: number;
  textIndex: number;
};

function createWindow(source: AsyncIterator<Uint8Array | string>, tokenizer: Tokenizer): Window {
  return {
    bytes: tokenizer.bytes, origin: 0, end: 0, ended: false,
    source, pending: undefined, pendingPos: 0, surrogate: '', spill: [], spillStart: NONE,
    utf16Length: tokenizer.utf16Length, text: '', textStart: 0, textEnd: 0, textByte: 0, textIndex: 0,
  };
}

/**
 * Buffers more of S, increasing `end`, or sets `ended`. Bytes before `keep`
 * may be dropped; bytes from `keep` on stay readable by `text`.
 */
async function fill(w: Window, keep: number): Promise<void> {
  if (w.pending === undefined) {
    const { done, value } = await w.source.next();
    let chunk = value;
    if (done) {
      w.ended = true;
      chunk = w.surrogate;
    } else if (typeof chunk === 'string') {
      // A surrogate pair split across chunks can't be encoded separately.
      chunk = w.surrogate + chunk;
      const last = chunk.charCodeAt(chunk.length - 1);
      w.surrogate = last >= 0xd800 && last < 0xdc00 ? chunk.slice(-1) : '';
      if (w.surrogate) chunk = chunk.slice(0, -1);
    }
    w.pending = chunk;
    w.pendingPos = 0;
  }
  drop(w, keep);
  copyPending(w);
}

/**
 * Moves the bytes from `keep` on to the start of the buffer, first spilling
 * them if they fill more than half of it.
 */
function drop(w: Window, keep: number): void {
  const { bytes, end } = w;
  // A spill belongs to the range kept when it was made.
  if (w.spillStart !== keep) {
    w.spill = [];
    w.spillStart = NONE;
  }
  const from = Math.max(w.origin, Math.min(keep, end));
  if (end - from > BUFFER / 2) {
    w.spill.push(bytes.slice(from - w.origin, end - w.origin));
    w.spillStart = keep;
    w.origin = end;
  } else {
    bytes.copyWithin(0, from - w.origin, end - w.origin);
    w.origin = from;
  }
  w.textEnd = 0;
}

/** Copies as much of the pending chunk as fits in the buffer. */
function copyPending(w: Window): void {
  const chunk = w.pending!;
  const pos = w.pendingPos;
  const space = w.bytes.subarray(w.end - w.origin);
  let read;
  if (typeof chunk === 'string') {
    const result = encoder.encodeInto(pos === 0 ? chunk : chunk.slice(pos), space);
    read = result.read;
    w.end += result.written;
  } else {
    read = Math.min(space.length, chunk.length - pos);
    space.set(pos === 0 && read === chunk.length ? chunk : chunk.subarray(pos, pos + read));
    w.end += read;
  }
  w.pendingPos += read;
  if (w.pendingPos === chunk.length) w.pending = undefined;
}

/**
 * Returns S[start, end) decoded as UTF-8, for a range that starts and ends at
 * characters and is readable: buffered, or from the `keep` of the last fill
 * on. The text holds on to nothing else.
 */
function text(w: Window, start: number, end: number): string {
  if (start >= w.origin) return decodeText(w.bytes, start - w.origin, end - w.origin);
  const all = new Uint8Array(end - w.spillStart);
  let at = 0;
  for (const piece of w.spill) {
    all.set(piece, at);
    at += piece.length;
  }
  all.set(w.bytes.subarray(0, end - w.origin), at);
  return decoder.decode(all.subarray(start - w.spillStart));
}

/**
 * Like `text`, but may slice the text from text decoded once for the whole
 * buffer, which saves the fixed cost of decoding each range but keeps that
 * text alive as long as the result. For text used briefly, as by JSON.parse.
 * Ranges are mostly read in order, so positions are mapped to text indices
 * incrementally.
 */
function sharedText(w: Window, start: number, end: number): string {
  // Short ranges are cheaper to decode in JavaScript than to map, and no
  // other range shares the text of one that ends the buffered bytes.
  if (end - start <= 16 || end === w.end || start < w.origin) return text(w, start, end);
  if (start < w.textStart || end > w.textEnd) {
    // A character split at the end of the buffered bytes comes after `end`,
    // so how it is decoded doesn't matter.
    const decoded = decoder.decode(w.bytes.subarray(start - w.origin, w.end - w.origin));
    // Invalid UTF-8 is decoded in a way the mapping doesn't follow.
    w.text = decoded.length === w.utf16Length(start - w.origin, w.end - w.origin) ? decoded : '';
    w.textStart = w.textByte = start;
    w.textEnd = w.end;
    w.textIndex = 0;
  }
  if (w.text === '') return text(w, start, end);
  if (start < w.textByte) {
    w.textByte = w.textStart;
    w.textIndex = 0;
  }
  const from = w.textIndex + w.utf16Length(w.textByte - w.origin, start - w.origin);
  const to = from + w.utf16Length(start - w.origin, end - w.origin);
  w.textByte = end;
  w.textIndex = to;
  return w.text.slice(from, to);
}

/** Returns S[start, end), which must be buffered, until the next fill. */
function bytesOf(w: Window, start: number, end: number): Uint8Array {
  return w.bytes.subarray(start - w.origin, end - w.origin);
}

/** Stops reading the source. */
async function release(w: Window): Promise<void> {
  await w.source.return?.();
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

// INPUT //////////////////////////////////////////////////////////////////////

/**
 * The token sequence T of a window's stream, read in order: T lists the
 * positions in S where a bracket, comma, colon, quote, or scalar starts. The
 * kind of a token is the byte at its position. Tokens are produced lazily, a
 * slice of buffered bytes at a time. Private to this section except through
 * the functions below.
 */
type Input = {
  w: Window;
  /** The window's bytes. */
  bytes: Uint8Array;
  tokens: Uint16Array;
  links: Uint16Array;
  tokenize: Tokenizer['tokenize'];
  /** The position the tokens are offsets from, and its index in `bytes`. */
  base: number;
  at: number;
  /** The position after the tokenized bytes, and the tokenizer state there. */
  scanned: number;
  state: number;
  /** Index of the next token, and the number of tokens. */
  cursor: number;
  count: number;
};

function createInput(source: AsyncIterator<Uint8Array | string>): Input {
  const tokenizer = createTokenizer();
  return {
    w: createWindow(source, tokenizer), bytes: tokenizer.bytes,
    tokens: tokenizer.tokens, links: tokenizer.links, tokenize: tokenizer.tokenize,
    base: 0, at: 0, scanned: 0, state: 0, cursor: 0, count: 0,
  };
}

// Functions that may have to wait for input return a promise only when they
// do, to avoid the cost of awaiting when the input is already buffered.

/**
 * Returns the kind of the next token, EOF, or MORE if the buffered input ran
 * out (see more).
 */
function peek(input: Input): number {
  if (input.cursor >= input.count && !scan(input)) {
    if (!input.w.ended) return MORE;
    if (input.state & IN_STRING) throw unexpected(EOF);
    return EOF;
  }
  return input.bytes[input.at + (input.tokens[input.cursor]! & OFFSET)]!;
}

/** The position of the token at index `i` of the batch. */
function token(input: Input, i: number): number {
  return input.base + (input.tokens[i]! & OFFSET);
}

/** The position of the next token, after peek returned its kind. */
function position(input: Input): number {
  return token(input, input.cursor);
}

/** Consumes the next token, after peek returned its kind, and returns its position. */
function take(input: Input): number {
  return token(input, input.cursor++);
}

/** Whether the token taken last closes a string with special characters. */
function special(input: Input): boolean {
  return input.tokens[input.cursor - 1]! > OFFSET;
}

/**
 * Tokenizes slices of buffered bytes until one has tokens, replacing the
 * consumed ones. Returns false if every buffered byte has been tokenized.
 */
function scan(input: Input): boolean {
  const { w } = input;
  do {
    // A slice may have no tokens, as inside a long string.
    if (input.scanned === w.end) return false;
    const start = input.scanned - w.origin;
    const end = Math.min(w.end - w.origin, start + SLICE);
    const result = input.tokenize(input.state, start, end);
    input.base = input.scanned;
    input.at = start;
    input.cursor = 0;
    input.count = result >>> 4;
    input.state = result & 15;
    input.scanned += end - start;
  } while (input.count === 0);
  return true;
}

/**
 * Buffers more input after peek returned MORE, keeping bytes from `keep` on
 * readable.
 */
function more(input: Input, keep: number): Promise<void> {
  return fill(input.w, keep);
}

/**
 * Consumes the value that starts at the next token, returning the position
 * right after it. Returns a promise if it has to wait for input, keeping bytes
 * from `keep` on readable meanwhile, as do the functions below.
 */
function consumeValue(input: Input, keep: number): number | Promise<number> {
  const i = input.cursor++;
  const c = input.bytes[input.at + (input.tokens[i]! & OFFSET)]!;
  if (c === LBRACKET || c === LBRACE) {
    const link = input.links[i]!;
    if (link === 0) return exitContainer(input, 1, keep);
    input.cursor = link + 1;
    return token(input, link) + 1;
  }
  if (c === QUOTE) return exitString(input, keep);
  return scalarEnd(input, keep);
}

/**
 * Consumes the closing quote of a string, returning the position after it.
 * The closing quote is then the last token consumed.
 */
function exitString(input: Input, keep: number): number | Promise<number> {
  // Inside a string the only token is the closing quote, and peek fails if
  // the input ends first.
  if (peek(input) === MORE) return more(input, keep).then(() => exitString(input, keep));
  return take(input) + 1;
}

/** Returns the end of the scalar consumed last, which is the next token. */
function scalarEnd(input: Input, keep: number): number | Promise<number> {
  const c = peek(input);
  if (c === MORE) return more(input, keep).then(() => scalarEnd(input, keep));
  return c === EOF ? input.w.end : position(input);
}

/**
 * Consumes tokens until `depth` open brackets have been closed, returning the
 * position after the last one. Linked brackets are skipped over.
 */
function exitContainer(input: Input, depth: number, keep: number): number | Promise<number> {
  for (;;) {
    const { bytes, tokens, links, count, at } = input;
    for (let i = input.cursor; i < count; i++) {
      const c = bytes[at + (tokens[i]! & OFFSET)]!;
      if (c === LBRACKET || c === LBRACE) {
        const link = links[i]!;
        if (link === 0) depth++;
        else i = link;
      } else if ((c === RBRACKET || c === RBRACE) && --depth === 0) {
        input.cursor = i + 1;
        return token(input, i) + 1;
      }
    }
    input.cursor = count;
    const c = peek(input);
    if (c === EOF) throw unexpected(EOF);
    if (c === MORE) return more(input, keep).then(() => exitContainer(input, depth, keep));
  }
}

// PARSER /////////////////////////////////////////////////////////////////////

// The parser navigates the tree of values in stream order. Values are named by
// sequence numbers, issued in stream order as `next` reaches them; values
// skipped inside a skipped value get none. The value of an object member is
// numbered after its key, and is reached by an operation on it, which skips
// the key if needed. A value is consumed by one operation, and moving on
// skips whatever part of it was not consumed.
//
// Operations return a plain result, or a promise only when they must wait for
// input. An operation waiting for input holds the parser, and later ones run
// after it.

/** The parent of the top-level values, at depth -1. */
const ROOT = -1;

// States of the current value.
/** Reached, but nothing of it consumed. */
const START = 0;
/** A string entered by `enter`, read up to the pin. */
const STRING = 1;
/** A container entered by `enter`, with no child reached yet. */
const OPENED = 2;
/** Consumed, and the separator after it not yet. */
const DONE = 3;
/** Consumed, and the comma after it too. */
const AFTER_COMMA = 4;
/** An object key consumed, and the colon after it too. */
const AFTER_COLON = 5;

type Parser = {
  input: Input;
  /** The current value: the one reached last, or the container entered last. */
  current: number;
  state: number;
  /** Whether the current value is an object key. */
  key: boolean;
  /** The earliest position whose bytes are still needed, or NONE. */
  pin: number;
  /** The last sequence number issued or reserved for a member's value. */
  last: number;
  /** The entered containers that are still open, and their opening bytes. */
  open: number[];
  kinds: number[];
  /** Decodes UTF-8 across pieces, once the entered string spans several. */
  partial: TextDecoder | undefined;
  /** An incomplete escape sequence at the end of the last piece. */
  held: string;
  /** Settles when the operation waiting for input, if any, is done. */
  waiting: Promise<void> | undefined;
  /** Clears `waiting`. */
  release: () => void;
};

function createParser(source: AsyncIterator<Uint8Array | string>): Parser {
  const p: Parser = {
    input: createInput(source), current: ROOT, state: OPENED, key: false, pin: NONE, last: ROOT,
    open: [], kinds: [], partial: undefined, held: '', waiting: undefined,
    release: () => {
      p.waiting = undefined;
    },
  };
  return p;
}

/**
 * Returns the next child of the entered container `parent` at `depth`, or
 * undefined after its last child.
 */
function next(p: Parser, parent: number, depth: number): Step<number> {
  if (p.waiting !== undefined) return p.waiting.then(() => next(p, parent, depth));
  return hold(p, advance(p, parent, depth));
}

/** Enters value `seq`, which must start with `kind`: QUOTE, LBRACKET, or LBRACE. */
function enter(p: Parser, seq: number, kind: number): void | Promise<void> {
  if (p.waiting !== undefined) return p.waiting.then(() => enter(p, seq, kind));
  return hold(p, enterNow(p, seq, kind));
}

/** Parses value `seq` whole. */
function parse(p: Parser, seq: number): unknown {
  if (p.waiting !== undefined) return p.waiting.then(() => parse(p, seq));
  return hold(p, parseNow(p, seq));
}

/**
 * Returns the next decoded piece of the entered string `seq`, or undefined if
 * there are no more. See `reading` for whether more may follow a piece.
 */
function chunk(p: Parser, seq: number): Step<string> {
  if (p.waiting !== undefined) return p.waiting.then(() => chunk(p, seq));
  return hold(p, chunkNow(p, seq));
}

/** Waits for an operation in progress, then releases the source. */
async function close(p: Parser): Promise<void> {
  await p.waiting;
  await release(p.input.w);
}

/**
 * Makes later operations wait for `result` if it is a promise. Only an
 * operation that found no other waiting sets `waiting`, so it is still the
 * one set when `result` settles.
 */
function hold<T>(p: Parser, result: T): T {
  if (result instanceof Promise) p.waiting = result.then(p.release, p.release);
  return result;
}

// Each step below either completes and updates the state, or returns a
// promise that continues it from where it stopped.

function advance(p: Parser, parent: number, depth: number): Step<number> {
  const { input } = p;
  if (depth !== ROOT && p.open[depth] !== parent) {
    throw new Error('JSON value skipped before it was consumed');
  }
  // Skips the rest of containers entered inside the current child.
  while (p.open.length > depth + 1) {
    p.pin = NONE;
    const end = exitContainer(input, 1, NONE);
    if (typeof end !== 'number') {
      return end.then(() => {
        closeContainer(p);
        return advance(p, parent, depth);
      });
    }
    closeContainer(p);
  }
  if (p.key) {
    const reached = toValue(p);
    if (reached) return reached.then(() => advance(p, parent, depth));
  }
  if (p.state === START || p.state === STRING) {
    const end = finishValue(p);
    if (typeof end !== 'number') return end.then(() => advance(p, parent, depth));
  }
  let c = peek(input);
  if (c === MORE) return more(input, NONE).then(() => advance(p, parent, depth));
  const kind = depth === ROOT ? ROOT : p.kinds[depth]!;
  if (kind === ROOT) {
    if (c === EOF) return undefined;
  } else {
    if (c === (kind === LBRACKET ? RBRACKET : RBRACE) && p.state !== AFTER_COMMA) {
      take(input);
      closeContainer(p);
      return undefined;
    }
    if (p.state === DONE) {
      if (c !== COMMA) throw unexpected(c);
      take(input);
      p.state = AFTER_COMMA;
      c = peek(input);
      if (c === MORE) return more(input, NONE).then(() => advance(p, parent, depth));
    }
  }
  if (kind === LBRACE) {
    if (c !== QUOTE) throw unexpected(c);
    p.key = true;
    p.current = ++p.last;
    p.last++;
  } else {
    checkStart(c);
    p.current = ++p.last;
  }
  p.state = START;
  return p.current;
}

/** Consumes the rest of the current value, which is at START or STRING. */
function finishValue(p: Parser): number | Promise<number> {
  const { input } = p;
  let end;
  if (p.state === START) {
    end = consumeValue(input, p.pin);
  } else {
    p.pin = NONE;
    end = exitString(input, NONE);
  }
  if (typeof end !== 'number') {
    return end.then(end => {
      p.state = DONE;
      return end;
    });
  }
  p.state = DONE;
  return end;
}

/** Makes the innermost open container the current value, consumed. */
function closeContainer(p: Parser): void {
  p.current = p.open.pop()!;
  p.kinds.pop();
  p.state = DONE;
  p.key = false;
}

/** Moves from the current value, a key, to the start of its member's value. */
function toValue(p: Parser): Promise<void> | undefined {
  const { input } = p;
  if (p.state === START || p.state === STRING) {
    const end = finishValue(p);
    if (typeof end !== 'number') return end.then(() => toValue(p));
  }
  if (p.state === DONE) {
    const c = peek(input);
    if (c === MORE) return more(input, NONE).then(() => toValue(p));
    if (c !== COLON) throw unexpected(c);
    take(input);
    p.state = AFTER_COLON;
  }
  const c = peek(input);
  if (c === MORE) return more(input, NONE).then(() => toValue(p));
  checkStart(c);
  p.current++;
  p.key = false;
  p.state = START;
}

function checkStart(c: number): void {
  if (c === EOF || c === COMMA || c === COLON || c === RBRACKET || c === RBRACE) throw unexpected(c);
}

/** Makes value `seq` current, if it is the current key's, and checks that it is at START. */
function claim(p: Parser, seq: number): Promise<void> | undefined {
  if (p.key && seq === p.current + 1) {
    const reached = toValue(p);
    if (reached) return reached;
  }
  if (p.current !== seq || p.state !== START) throw new Error('JSON value already consumed');
}

function enterNow(p: Parser, seq: number, kind: number): void | Promise<void> {
  const claimed = claim(p, seq);
  if (claimed) return claimed.then(() => enterNow(p, seq, kind));
  const { input } = p;
  // A value at START has its first token buffered.
  const c = peek(input);
  if (c !== kind) throw new TypeError(`Expected JSON ${TYPE_NAMES[kind]}, found ${typeName(c)}`);
  const at = take(input);
  if (kind === QUOTE) {
    p.pin = at + 1;
    p.partial = undefined;
    p.held = '';
    p.state = STRING;
  } else {
    p.open.push(seq);
    p.kinds.push(kind);
    p.state = OPENED;
  }
}

function parseNow(p: Parser, seq: number): unknown {
  const claimed = claim(p, seq);
  if (claimed) return claimed.then(() => parseNow(p, seq));
  const { input } = p;
  const kind = peek(input);
  p.pin = position(input);
  const end = finishValue(p);
  return typeof end === 'number' ? parseConsumed(p, kind, end) : end.then(end => parseConsumed(p, kind, end));
}

/** Parses the value consumed last, of `kind`, from the pin to `end`. */
function parseConsumed(p: Parser, kind: number, end: number): unknown {
  const { input } = p;
  const start = p.pin;
  p.pin = NONE;
  // Strings aren't sliced from shared text, which would keep it alive.
  return kind === QUOTE
    ? decodeString(text(input.w, start + 1, end - 1), special(input))
    : JSON.parse(sharedText(input.w, start, end));
}

/**
 * Returns the raw contents of a complete string decoded, parsing it if it has
 * `special` characters, to unescape them or reject control characters.
 */
function decodeString(raw: string, special: boolean): string {
  return special ? JSON.parse('"' + raw + '"') : raw;
}

function chunkNow(p: Parser, seq: number): Step<string> {
  if (p.current !== seq || p.state !== STRING) {
    throw new Error('JSON value skipped before it was consumed');
  }
  const { input } = p;
  const { w } = input;
  for (;;) {
    // The contents not read yet start at the pin. The next token is the
    // closing quote, if it has arrived.
    const c = peek(input);
    if (c === MORE && p.pin === w.end) return more(input, p.pin).then(() => chunkNow(p, seq));
    const closed = c !== MORE;
    const end = closed ? take(input) : w.end;
    let piece = '';
    if (closed && p.partial === undefined) {
      // The rest of the string arrived at once.
      piece = decodeString(text(w, p.pin, end), special(input));
    } else {
      p.partial ??= new TextDecoder('utf-8', { ignoreBOM: true });
      const raw = p.held + p.partial.decode(bytesOf(w, p.pin, end), { stream: !closed });
      const complete = closed ? raw.length : completeEscapesEnd(raw);
      p.held = raw.slice(complete);
      if (complete > 0) piece = unescape(raw.slice(0, complete));
    }
    if (closed) {
      p.pin = NONE;
      p.state = DONE;
      return piece.length > 0 ? piece : undefined;
    }
    p.pin = end;
    if (piece.length > 0) return piece;
  }
}

/** Whether the entered string `seq` may have more pieces. */
function reading(p: Parser, seq: number): boolean {
  return p.current === seq && p.state === STRING;
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

// VALUES /////////////////////////////////////////////////////////////////////

class Value implements JsonStValue {
  #p: Parser;
  #seq: number;
  #depth: number;

  constructor(p: Parser, seq: number, depth: number) {
    this.#p = p;
    this.#seq = seq;
    this.#depth = depth;
  }

  async parse(): Promise<unknown> {
    return parse(this.#p, this.#seq);
  }

  stringChunks(): AsyncIterable<string> {
    const p = this.#p, seq = this.#seq;
    let entered = false;
    /** Whether the last piece has been returned. */
    let ended = false;
    const step = (): Step<string> => {
      if (ended) return undefined;
      if (!entered) {
        entered = true;
        const started = enter(p, seq, QUOTE);
        if (started) return started.then(step);
      }
      const piece = chunk(p, seq);
      if (piece instanceof Promise) {
        return piece.then(piece => {
          ended = !reading(p, seq);
          return piece;
        });
      }
      ended = !reading(p, seq);
      return piece;
    };
    return new Items(step);
  }

  arrayValues(): AsyncIterable<JsonStValue> {
    return this.#children(LBRACKET);
  }

  objectEntries(): AsyncIterable<[key: JsonStValue, value: JsonStValue]> {
    return this.#children(LBRACE);
  }

  #children<T>(kind: number): AsyncIterable<T> {
    const p = this.#p, seq = this.#seq, depth = this.#depth;
    let entered = false;
    const step = (): Step<T> => {
      if (!entered) {
        entered = true;
        const started = enter(p, seq, kind);
        if (started) return started.then(step);
      }
      const child = next(p, seq, depth);
      if (child instanceof Promise) return child.then(child => item(p, kind, child, depth + 1));
      return item(p, kind, child, depth + 1);
    };
    return new Items(step);
  }
}

/** The item of an iterator over the children of `kind` for child `seq`. */
function item<T>(p: Parser, kind: number, seq: number | undefined, depth: number): T | undefined {
  if (seq === undefined) return undefined;
  return (kind === LBRACE ? [new Value(p, seq, depth), new Value(p, seq + 1, depth)] : new Value(p, seq, depth)) as T;
}
