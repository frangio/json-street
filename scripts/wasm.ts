// Assembles src/tokenize.wat and embeds the binary in src/index.ts.
// Usage: node scripts/wasm.ts [--check]

import { readFileSync, writeFileSync } from 'node:fs';
import wabt from 'wabt';

const wat = new URL('../src/tokenize.wat', import.meta.url);
const index = new URL('../src/index.ts', import.meta.url);

const module = (await wabt()).parseWat('tokenize.wat', readFileSync(wat, 'utf8'), { simd: true, multi_value: true });
const binary = module.toBinary({}).buffer;
const source = readFileSync(index, 'utf8');
const updated = source.replace(/^const TOKENIZER = '.*';$/m, `const TOKENIZER = '${Buffer.from(binary).toString('base64')}';`);

if (process.argv.includes('--check')) {
  if (updated !== source) {
    console.error('src/index.ts is out of date with src/tokenize.wat; run npm run wasm');
    process.exit(1);
  }
} else {
  writeFileSync(index, updated);
  console.log(`tokenizer: ${binary.length} bytes`);
}
