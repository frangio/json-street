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
export async function* parseValues(stream: AsyncIterable<string>): AsyncIterable<JsonStValue> {
  const input = new Input(stream[Symbol.asyncIterator]());
  try {
    while ((await input.peek()) !== EOF) {
      const value = new Value(input);
      yield value;
      await value.finish();
    }
  } finally {
    await input.close();
  }
}

const EOF = -1;
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

  // Scanner state.
  #depth = 0;
  #inString = false;
  #escaped = false;

  constructor(chunks: AsyncIterator<string>) {
    this.#chunks = chunks;
  }

  /** Skips whitespace and returns the next character, or EOF. */
  async peek(): Promise<number> {
    for (;;) {
      while (this.#pos < this.#buf.length) {
        const c = this.#buf.charCodeAt(this.#pos);
        if (!isWhitespace(c)) return c;
        this.#pos++;
      }
      if (!(await this.#fill())) return EOF;
    }
  }

  /** Consumes the character returned by peek, which must be `c`. */
  async expect(c: number): Promise<void> {
    const next = await this.peek();
    if (next !== c) throw unexpected(next);
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
  async value(capture: boolean): Promise<string> {
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
  async exitContainer(): Promise<void> {
    this.#inString = false;
    this.#depth = 1;
    await this.#scan(this.#pos, false);
  }

  /** Skips the rest of the string entered last. */
  async exitString(): Promise<void> {
    await this.#scan(this.#pos, false);
  }

  /**
   * Returns the next piece of raw contents of the string entered last.
   * `inString` is false once the closing quote has been consumed.
   */
  async stringSegment(): Promise<string> {
    while (this.#pos >= this.#buf.length) {
      if (!(await this.#fill())) throw unexpected(EOF);
    }
    const start = this.#pos;
    const closed = this.#scanChunk();
    return this.#buf.slice(start, closed ? this.#pos - 1 : this.#pos);
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
    return !done;
  }

  async #scan(start: number, capture: boolean): Promise<string> {
    let parts: string[] = [];
    while (!this.#scanChunk()) {
      if (capture) parts.push(this.#buf.slice(start));
      start = 0;
      if (!(await this.#fill())) {
        if (this.#inString || this.#depth > 0) throw unexpected(EOF);
        break;
      }
    }
    if (!capture) return '';
    parts.push(this.#buf.slice(start, this.#pos));
    return parts.join('');
  }

  /**
   * Advances the scanner through the current chunk. Returns true when the
   * value is complete, leaving the cursor right after it.
   */
  #scanChunk(): boolean {
    const buf = this.#buf;
    for (; this.#pos < buf.length; this.#pos++) {
      const c = buf.charCodeAt(this.#pos);
      if (this.#inString) {
        if (this.#escaped) {
          this.#escaped = false;
        } else if (c === BACKSLASH) {
          this.#escaped = true;
        } else if (c === QUOTE) {
          this.#inString = false;
          if (this.#depth === 0) {
            this.#pos++;
            return true;
          }
        }
      } else if (this.#depth === 0) {
        if (isDelimiter(c)) return true;
      } else if (c === QUOTE) {
        this.#inString = true;
      } else if (c === LBRACKET || c === LBRACE) {
        this.#depth++;
      } else if (c === RBRACKET || c === RBRACE) {
        if (--this.#depth === 0) {
          this.#pos++;
          return true;
        }
      }
    }
    return false;
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
    this.#capture = this.#start().then(() => this.#input.value(true));
    const text = await this.#capture;
    this.#state = DONE;
    return JSON.parse(text);
  }

  async *stringChunks(): AsyncIterable<string> {
    await this.#enter(QUOTE);
    const input = this.#input;
    let held = '';
    do {
      let raw = held + await input.stringSegment();
      if (input.inString) {
        const end = completeEscapesEnd(raw);
        held = raw.slice(end);
        raw = raw.slice(0, end);
      }
      if (raw.length > 0) {
        yield JSON.parse('"' + raw + '"') as string;
        this.#checkActive();
      }
    } while (input.inString);
    this.#state = DONE;
  }

  async *arrayValues(): AsyncIterable<JsonStValue> {
    await this.#enter(LBRACKET);
    const input = this.#input;
    if ((await input.peek()) !== RBRACKET) {
      for (;;) {
        const child = this.#child = new Value(input);
        yield child;
        this.#checkActive();
        await child.finish();
        const c = await input.peek();
        if (c === RBRACKET) break;
        if (c !== COMMA) throw unexpected(c);
        await input.expect(COMMA);
      }
    }
    await input.expect(RBRACKET);
    this.#state = DONE;
  }

  async *objectEntries(): AsyncIterable<[key: JsonStValue, value: JsonStValue]> {
    await this.#enter(LBRACE);
    const input = this.#input;
    let c = await input.peek();
    if (c !== RBRACE) {
      for (;;) {
        if (c !== QUOTE) throw unexpected(c);
        const key = new Value(input);
        const value = this.#child = new Value(input, key);
        yield [key, value];
        this.#checkActive();
        await value.finish();
        c = await input.peek();
        if (c === RBRACE) break;
        if (c !== COMMA) throw unexpected(c);
        await input.expect(COMMA);
        c = await input.peek();
      }
    }
    await input.expect(RBRACE);
    this.#state = DONE;
  }

  /** Consumes whatever part of the value has not been consumed yet. */
  async finish(): Promise<void> {
    if (this.#state === PENDING) {
      this.#claim();
      await this.#start();
      await this.#input.value(false);
    } else if (this.#state === ACTIVE) {
      if (this.#capture !== undefined) {
        await this.#capture.catch(() => {});
      } else if (!this.#entered) {
        // An iterator is about to start; nothing has been consumed yet.
        await this.#start();
        await this.#input.value(false);
      } else if (this.#first === QUOTE) {
        await this.#input.exitString();
      } else {
        await this.#child?.finish();
        await this.#input.exitContainer();
      }
    }
    this.#state = DONE;
  }

  #claim(): void {
    if (this.#state !== PENDING) throw new Error('JSON value already consumed');
    this.#state = ACTIVE;
  }

  #checkActive(): void {
    if (this.#state !== ACTIVE) throw new Error('JSON value skipped before it was consumed');
  }

  /** Moves the input to the first character of the value. */
  async #start(): Promise<void> {
    if (this.#first !== EOF) return;
    const key = this.#key;
    if (key !== undefined) {
      await key.finish();
      await this.#input.expect(COLON);
    }
    const c = await this.#input.peek();
    if (c === EOF || c === COMMA || c === COLON || c === RBRACKET || c === RBRACE) {
      throw unexpected(c);
    }
    this.#first = c;
  }

  /** Enters the value, which must start with `open`. */
  async #enter(open: number): Promise<void> {
    this.#claim();
    await this.#start();
    if (this.#first !== open) {
      this.#state = PENDING;
      throw new TypeError(`Expected JSON ${TYPE_NAMES[open]}, found ${typeName(this.#first)}`);
    }
    this.#input.enter();
    this.#entered = true;
  }
}

const TYPE_NAMES: Record<number, string> = { [QUOTE]: 'string', [LBRACKET]: 'array', [LBRACE]: 'object' };

function typeName(first: number): string {
  return TYPE_NAMES[first] ?? 'number, boolean, or null';
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
