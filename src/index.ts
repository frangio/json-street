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
 * Iterates over the whitespace-separated JSON values in a stream of text.
 */
export function parseValues(stream: AsyncIterable<string>): AsyncIterable<JsonStValue> {
  const input = new Input(stream[Symbol.asyncIterator]());
  let value: Value | undefined;
  const step = (): Step<Value> => {
    const finished = value?.finish();
    if (finished) return finished.then(step);
    const c = input.peek();
    if (c === MORE) return input.peekAsync().then(step);
    if (c === EOF) return undefined;
    return value = new Value(input);
  };
  return new Items(step, async () => {
    // The consumer may stop iterating while a parse is in progress, as in
    // `for await (const v of parseValues(s)) return v.parse();`
    await value?.parsed();
    await input.close();
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
/** Returned by `Input.peek` when it needs to wait for the next chunk. */
const MORE = -2;
const TAB = 9, LF = 10, CR = 13, SPACE = 32;
const QUOTE = 34, COMMA = 44, COLON = 58, BACKSLASH = 92;
const LBRACKET = 91, RBRACKET = 93, LBRACE = 123, RBRACE = 125;

function isWhitespace(c: number): boolean {
  return c === SPACE || c === LF || c === CR || c === TAB;
}

/** Characters that end a number, boolean, or null. */
function isDelimiter(c: number): boolean {
  return isWhitespace(c) || c === COMMA || c === COLON || c === RBRACKET || c === RBRACE
    || c === LBRACKET || c === LBRACE || c === QUOTE;
}

function unexpected(c: number): SyntaxError {
  return new SyntaxError(c === EOF
    ? 'Unexpected end of JSON input'
    : `Unexpected token '${String.fromCharCode(c)}' in JSON`);
}

/**
 * The text of the stream with a cursor, hiding chunk boundaries. Values are
 * delimited by a scanner that only tracks nesting depth and strings.
 */
class Input {
  #chunks: AsyncIterator<string>;
  #buf = '';
  #pos = 0;
  #ended = false;

  // Scanner state.
  #depth = 0;
  #inString = false;
  #escaped = false;

  constructor(chunks: AsyncIterator<string>) {
    this.#chunks = chunks;
  }

  // Methods that may have to wait for input return a promise only when they
  // do, to avoid the cost of awaiting when the input is already buffered.

  /**
   * Skips whitespace and returns the next character, EOF, or MORE if the
   * buffered input ran out (see peekAsync).
   */
  peek(): number {
    const buf = this.#buf;
    let pos = this.#pos;
    while (pos < buf.length) {
      const c = buf.charCodeAt(pos);
      if (!isWhitespace(c)) {
        this.#pos = pos;
        return c;
      }
      pos++;
    }
    this.#pos = pos;
    return this.#ended ? EOF : MORE;
  }

  /** Like peek, but waits for input instead of returning MORE. */
  async peekAsync(): Promise<number> {
    let c;
    while ((c = this.peek()) === MORE) await this.#fill();
    return c;
  }

  /** Consumes the character returned by peek. */
  advance(): void {
    this.#pos++;
  }

  /** Enters the string, array, or object that starts at the peeked character. */
  enter(): void {
    const c = this.#buf.charCodeAt(this.#pos);
    this.#inString = c === QUOTE;
    this.#escaped = false;
    this.#depth = c === LBRACKET || c === LBRACE ? 1 : 0;
    this.#pos++;
  }

  /**
   * Consumes the value that starts at the peeked character, returning its
   * text if `capture` is true.
   */
  value(capture: boolean): string | Promise<string> {
    const c = this.#buf.charCodeAt(this.#pos);
    const start = this.#pos;
    if (c === QUOTE || c === LBRACKET || c === LBRACE) {
      this.enter();
    } else {
      this.#inString = false;
      this.#depth = 0;
    }
    return this.#scan(start, capture);
  }

  /** Skips the rest of the array or object entered last, between elements. */
  exitContainer(): string | Promise<string> {
    this.#inString = false;
    this.#depth = 1;
    return this.#scan(this.#pos, false);
  }

  /** Skips the rest of the string entered last. */
  exitString(): string | Promise<string> {
    return this.#scan(this.#pos, false);
  }

  /**
   * Returns the next piece of raw contents of the string entered last.
   * `inString` is false once the closing quote has been consumed. Returns
   * undefined if the buffered input ran out (see more).
   */
  stringSegment(): string | undefined {
    if (this.#pos >= this.#buf.length) return undefined;
    const start = this.#pos;
    const closed = this.#scanChunk();
    return this.#buf.slice(start, closed ? this.#pos - 1 : this.#pos);
  }

  /** Waits for more input inside a string. */
  async more(): Promise<void> {
    if (!(await this.#fill())) throw unexpected(EOF);
  }

  get inString(): boolean {
    return this.#inString;
  }

  async close(): Promise<void> {
    await this.#chunks.return?.();
  }

  async #fill(): Promise<boolean> {
    const { done, value } = await this.#chunks.next();
    this.#buf = done ? '' : value;
    this.#pos = 0;
    this.#ended = !!done;
    return !done;
  }

  #scan(start: number, capture: boolean): string | Promise<string> {
    if (this.#scanChunk()) return capture ? this.#buf.slice(start, this.#pos) : '';
    return this.#scanAsync(capture ? [this.#buf.slice(start)] : undefined);
  }

  async #scanAsync(parts: string[] | undefined): Promise<string> {
    for (;;) {
      if (!(await this.#fill())) {
        if (this.#inString || this.#depth > 0) throw unexpected(EOF);
        break;
      }
      if (this.#scanChunk()) {
        parts?.push(this.#buf.slice(0, this.#pos));
        break;
      }
      parts?.push(this.#buf);
    }
    return parts?.join('') ?? '';
  }

  /**
   * Advances the scanner through the current chunk. Returns true when the
   * value is complete, leaving the cursor right after it.
   */
  #scanChunk(): boolean {
    const buf = this.#buf;
    let pos = this.#pos;
    let depth = this.#depth;
    let inString = this.#inString;
    let done = false;
    while (pos < buf.length) {
      if (inString) {
        // Find the closing quote: the first one preceded by an even number of
        // backslashes. Only backslashes in this chunk count; an odd run at
        // the end of the previous chunk escapes the first character here.
        let from = pos;
        if (this.#escaped) {
          from++;
          this.#escaped = false;
        }
        let quote = buf.indexOf('"', from);
        while (quote !== -1 && backslashesBefore(buf, quote, from) % 2 === 1) {
          quote = buf.indexOf('"', quote + 1);
        }
        if (quote === -1) {
          this.#escaped = from < buf.length && backslashesBefore(buf, buf.length, from) % 2 === 1;
          pos = buf.length;
          break;
        }
        pos = quote + 1;
        inString = false;
        if (depth === 0) {
          done = true;
          break;
        }
        continue;
      }
      const c = buf.charCodeAt(pos);
      if (depth === 0) {
        if (isDelimiter(c)) {
          done = true;
          break;
        }
      } else if (c === QUOTE) {
        inString = true;
      } else if (c === LBRACKET || c === LBRACE) {
        depth++;
      } else if (c === RBRACKET || c === RBRACE) {
        if (--depth === 0) {
          pos++;
          done = true;
          break;
        }
      }
      pos++;
    }
    this.#pos = pos;
    this.#depth = depth;
    this.#inString = inString;
    return done;
  }
}

const PENDING = 0, ACTIVE = 1, DONE = 2;

class Value implements JsonStValue {
  #input: Input;
  /** The key of an object member, which precedes this value in the stream. */
  #key: Value | undefined;
  #state = PENDING;
  /** The first character of the value, once known. */
  #first = EOF;
  /** Whether a string, array, or object has been entered. */
  #entered = false;
  /** The element last yielded while iterating an array or object. */
  #child: Value | undefined;
  /** The text being captured by parse. */
  #capture: Promise<string> | undefined;

  constructor(input: Input, key?: Value) {
    this.#input = input;
    this.#key = key;
  }

  async parse(): Promise<unknown> {
    this.#claim();
    const started = this.#start();
    let text = started ? started.then(() => this.#input.value(true)) : this.#input.value(true);
    if (typeof text !== 'string') {
      this.#capture = text;
      text = await text;
    }
    this.#state = DONE;
    return this.#first === QUOTE ? decodeString(text.slice(1, -1)) : JSON.parse(text);
  }

  stringChunks(): AsyncIterable<string> {
    const input = this.#input;
    let entered = false;
    let held = '';
    const step = (): Step<string> => {
      if (!entered) {
        entered = true;
        const started = this.#enter(QUOTE);
        if (started) return started.then(step);
      }
      this.#checkActive();
      while (input.inString) {
        const segment = input.stringSegment();
        if (segment === undefined) return input.more().then(step);
        let raw = held + segment;
        held = '';
        if (input.inString) {
          const end = completeEscapesEnd(raw);
          held = raw.slice(end);
          raw = raw.slice(0, end);
        }
        if (raw.length > 0) return decodeString(raw);
      }
      this.#state = DONE;
      return undefined;
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
      const c = input.peek();
      if (c === MORE) return input.peekAsync().then(step);
      if (c === RBRACKET) {
        input.advance();
        this.#state = DONE;
        return undefined;
      }
      if (child !== undefined) {
        if (c !== COMMA) throw unexpected(c);
        input.advance();
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
        const c = input.peek();
        if (c === MORE) return input.peekAsync().then(step);
        if (c === COMMA) {
          input.advance();
          value = undefined;
          afterComma = true;
        } else if (c !== RBRACE) {
          throw unexpected(c);
        }
      }
      const c = input.peek();
      if (c === MORE) return input.peekAsync().then(step);
      if (c === RBRACE && !afterComma) {
        input.advance();
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
    let result: string | Promise<unknown> | undefined;
    if (this.#capture !== undefined) {
      result = this.parsed();
    } else if (!this.#entered) {
      // Nothing has been consumed yet (an iterator may be about to start).
      const started = this.#start();
      result = started ? started.then(() => input.value(false)) : input.value(false);
    } else if (this.#first === QUOTE) {
      result = input.exitString();
    } else {
      const finished = this.#child?.finish();
      result = finished ? finished.then(() => input.exitContainer()) : input.exitContainer();
    }
    return typeof result === 'string' ? undefined : result;
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
   * Moves the input to the first character of the value. Returns a promise if
   * it has to wait for input.
   */
  #start(): Promise<void> | undefined {
    if (this.#first !== EOF) return;
    const input = this.#input;
    const key = this.#key;
    if (key !== undefined) {
      const finished = key.finish();
      if (finished) return finished.then(() => this.#start());
      const c = input.peek();
      if (c === MORE) return input.peekAsync().then(() => this.#start());
      if (c !== COLON) throw unexpected(c);
      input.advance();
      this.#key = undefined;
    }
    const c = input.peek();
    if (c === MORE) return input.peekAsync().then(() => this.#start());
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
    this.#input.enter();
    this.#entered = true;
  }
}

/** Counts the backslashes that precede `end`, back to `start`. */
function backslashesBefore(buf: string, end: number, start: number): number {
  let i = end;
  while (i > start && buf.charCodeAt(i - 1) === BACKSLASH) i--;
  return end - i;
}

const TYPE_NAMES: Record<number, string> = { [QUOTE]: 'string', [LBRACKET]: 'array', [LBRACE]: 'object' };

function typeName(first: number): string {
  return TYPE_NAMES[first] ?? 'number, boolean, or null';
}

/** Decodes the raw contents of a string, which has to be complete. */
function decodeString(raw: string): string {
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
