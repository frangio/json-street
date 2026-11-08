# json-street

Simple and efficient streaming JSON parser, with no dependencies.

```
npm install json-street
```

Process JSON incrementally, without waiting for all of it to arrive:

```typescript
import { parseValues } from 'json-street';

const response = await fetch('https://api.example.com/data');

// {"items": [{"id": 1, ...}, {"id": 2, ...}, ...], ...}
for await (const root of parseValues(response.body)) {
  for await (const [key, value] of root.objectEntries()) {
    if (await key.parse() === 'items') {
      for await (const item of value.arrayValues()) {
        console.log(await item.parse());
      }
    }
  }
}
```

`parseValues` iterates over the whitespace-separated JSON values in a stream
(a single document, or formats like NDJSON). The stream may yield UTF-8 bytes
or strings, split anywhere. Bytes are fastest: they're not decoded unless
they're part of a value that is read. Each value is consumed once, with one
of these methods:

- `parse()` parses the whole value, like `JSON.parse`.
- `stringChunks()` iterates over the decoded contents of a string as it arrives.
- `arrayValues()` iterates over the elements of an array.
- `objectEntries()` iterates over the `[key, value]` members of an object.

Calling a method for the wrong type throws a `TypeError` and leaves the value
untouched. Malformed JSON throws a `SyntaxError`.

Strings are converted to UTF-8, so unpaired surrogates in them become U+FFFD.

Values must be consumed in stream order. Advancing an iterator skips whatever
part of the previous value was not consumed. Skipped parts are only checked
for balanced brackets and strings.
