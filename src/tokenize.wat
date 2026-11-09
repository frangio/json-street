;; The tokenizer of json-street, embedded in index.ts by `npm run wasm`. See
;; `Tokenize` in index.ts for what it computes.
;;
;; It works like the first stage of simdjson: each block of 64 bytes becomes
;; bitmasks with one bit per byte, and bit arithmetic on those finds the
;; escaped characters, the contents of strings, and the tokens, without a
;; branch per byte.
;;
;; It reads up to 63 bytes past `end`, which must be in memory. They don't
;; affect the result.
;;
;; Tokens are 16-bit words at address 0: the offset of the token's byte from
;; `pos`, which is less than 2^15 since `end - pos` must be at most 16384. The
;; token of the closing quote of a string has bit 15 set when the string
;; contains a backslash or a control character, so needs more than decoding.
;;
;; It also matches brackets: when a bracket closes in the same call that it
;; opened in, the index of its token is written as a 16-bit word at $LINKS +
;; 2 × the index of the opening bracket's token. Every other opening bracket gets 0 there.
;; Open brackets are kept on a stack at $STACK, of at most $CAP entries, which
;; starts empty in each call: a bracket that closes in a later call is never
;; linked, so the stack needn't persist, and a closing bracket that arrives
;; with the stack empty is ignored.
;;
;; Positions are relative to the byte region at $BYTES, and the result depends
;; only on the arguments and the bytes.
(module
  (import "env" "memory" (memory 1))

  (global $LINKS i32 (i32.const 32768))
  (global $STACK i32 (i32.const 65536))
  (global $CAP i32 (i32.const 16384))
  (global $BYTES i32 (i32.const 98304))

  ;; Returns bitmasks of the 64 bytes at $p: quotes, backslashes, opening
  ;; brackets, closing brackets, brackets and commas and colons, those or
  ;; whitespace, and control characters.
  (func $classify (param $p i32) (result i64 i64 i64 i64 i64 i64 i64)
    (local $i i32) (local $shift i64) (local $v v128) (local $lower v128) (local $s v128)
    (local $opening v128) (local $closing v128)
    (local $quote i64) (local $backslash i64) (local $struct i64) (local $space i64) (local $control i64)
    (local $open i64) (local $close i64)
    (loop $parts
      (local.set $v (v128.load (i32.add (local.get $p) (local.get $i))))
      ;; Setting bit 5 maps [ to { and ] to }, and no other byte to either.
      (local.set $lower (v128.or (local.get $v) (i8x16.splat (i32.const 0x20))))
      (local.set $opening (i8x16.eq (local.get $lower) (i8x16.splat (i32.const 0x7b))))
      (local.set $closing (i8x16.eq (local.get $lower) (i8x16.splat (i32.const 0x7d))))
      (local.set $s
        (v128.or
          (v128.or
            (i8x16.eq (local.get $v) (i8x16.splat (i32.const 0x2c)))
            (i8x16.eq (local.get $v) (i8x16.splat (i32.const 0x3a))))
          (v128.or (local.get $opening) (local.get $closing))))
      (local.set $shift (i64.extend_i32_u (local.get $i)))
      (local.set $open (i64.or (local.get $open)
        (i64.shl (i64.extend_i32_u (i8x16.bitmask (local.get $opening))) (local.get $shift))))
      (local.set $close (i64.or (local.get $close)
        (i64.shl (i64.extend_i32_u (i8x16.bitmask (local.get $closing))) (local.get $shift))))
      (local.set $quote (i64.or (local.get $quote)
        (i64.shl (i64.extend_i32_u (i8x16.bitmask
          (i8x16.eq (local.get $v) (i8x16.splat (i32.const 0x22))))) (local.get $shift))))
      (local.set $backslash (i64.or (local.get $backslash)
        (i64.shl (i64.extend_i32_u (i8x16.bitmask
          (i8x16.eq (local.get $v) (i8x16.splat (i32.const 0x5c))))) (local.get $shift))))
      (local.set $control (i64.or (local.get $control)
        (i64.shl (i64.extend_i32_u (i8x16.bitmask
          (i8x16.lt_u (local.get $v) (i8x16.splat (i32.const 0x20))))) (local.get $shift))))
      (local.set $struct (i64.or (local.get $struct)
        (i64.shl (i64.extend_i32_u (i8x16.bitmask (local.get $s))) (local.get $shift))))
      (local.set $space (i64.or (local.get $space)
        (i64.shl (i64.extend_i32_u (i8x16.bitmask
          (v128.or
            (local.get $s)
            (v128.or
              (v128.or
                (i8x16.eq (local.get $v) (i8x16.splat (i32.const 0x20)))
                (i8x16.eq (local.get $v) (i8x16.splat (i32.const 0x09))))
              (v128.or
                (i8x16.eq (local.get $v) (i8x16.splat (i32.const 0x0a)))
                (i8x16.eq (local.get $v) (i8x16.splat (i32.const 0x0d)))))))) (local.get $shift))))
      (br_if $parts (i32.lt_u
        (local.tee $i (i32.add (local.get $i) (i32.const 16)))
        (i32.const 64))))
    (local.get $quote) (local.get $backslash) (local.get $open) (local.get $close)
    (local.get $struct) (local.get $space) (local.get $control))

  (func (export "tokenize") (param $state i32) (param $pos i32) (param $end i32) (result i32)
    (local $out i32) (local $base i32) (local $n i32) (local $last i64) (local $valid i64)
    (local $quote i64) (local $backslash i64) (local $struct i64) (local $delim i64)
    (local $inString i64) (local $escaped i64) (local $inScalar i64)
    (local $starts i64) (local $even i64) (local $oddCarries i64) (local $escapes i64)
    (local $inside i64) (local $scalar i64) (local $tokens i64)
    ;; Control characters or backslashes, whether the open string has had
    ;; one, closing quotes of strings that had one, and a token.
    (local $specials i64) (local $special i64) (local $flagged i64) (local $bit i64)
    ;; Opening and closing brackets outside strings, the index of the first
    ;; token of the block, and the tokens of the block.
    (local $open i64) (local $close i64) (local $index i32) (local $blockTokens i64) (local $at i32)
    ;; The number of open brackets in this call.
    (local $depth i32)
    (local.set $pos (i32.add (local.get $pos) (global.get $BYTES)))
    (local.set $end (i32.add (local.get $end) (global.get $BYTES)))
    (local.set $base (local.get $pos))
    (local.set $inString (i64.extend_i32_u (i32.and (local.get $state) (i32.const 1))))
    (local.set $escaped (i64.extend_i32_u (i32.and (i32.shr_u (local.get $state) (i32.const 1)) (i32.const 1))))
    (local.set $inScalar (i64.extend_i32_u (i32.and (i32.shr_u (local.get $state) (i32.const 2)) (i32.const 1))))
    (local.set $special (i64.extend_i32_u (i32.shr_u (local.get $state) (i32.const 3))))
    (block $done
      (loop $blocks
        (br_if $done (i32.ge_u (local.get $pos) (local.get $end)))

        ;; Bits of bytes at or past $end are cleared. The state for the next
        ;; block is read at the last byte before $end.
        (local.set $n (i32.sub (local.get $end) (local.get $pos)))
        (if (i32.ge_u (local.get $n) (i32.const 64))
          (then
            (local.set $n (i32.const 64))
            (local.set $valid (i64.const -1)))
          (else
            (local.set $valid (i64.sub
              (i64.shl (i64.const 1) (i64.extend_i32_u (local.get $n)))
              (i64.const 1)))))
        (local.set $last (i64.extend_i32_u (i32.sub (local.get $n) (i32.const 1))))
        (call $classify (local.get $pos))
        (local.set $specials)
        (local.set $delim)
        (local.set $struct)
        (local.set $close)
        (local.set $open)
        (local.set $backslash)
        (local.set $quote)
        (local.set $backslash (i64.and (local.get $backslash) (local.get $valid)))
        (local.set $specials (i64.and (i64.or (local.get $specials) (local.get $backslash)) (local.get $valid)))

        ;; Escaped characters are those right after a run of backslashes of
        ;; odd length. A run is odd when its start and the position right
        ;; after it have different parity, and adding the run's start bit to
        ;; the run carries into that position. A run continuing an odd run
        ;; from the previous block is treated as starting at an odd position.
        (local.set $starts (i64.and (local.get $backslash)
          (i64.xor (i64.shl (local.get $backslash) (i64.const 1)) (i64.const -1))))
        (local.set $even (i64.xor (i64.const 0x5555555555555555) (local.get $escaped)))
        (local.set $oddCarries (i64.add (local.get $backslash)
          (i64.and (local.get $starts) (i64.xor (local.get $even) (i64.const -1)))))
        (local.set $escapes
          (i64.and
            (i64.xor (local.get $backslash) (i64.const -1))
            (i64.or
              (i64.and
                (i64.add (local.get $backslash) (i64.and (local.get $starts) (local.get $even)))
                (i64.const 0xaaaaaaaaaaaaaaaa))
              (i64.and
                (i64.or (local.get $oddCarries) (local.get $escaped))
                (i64.const 0x5555555555555555)))))
        ;; An odd run reaching the end of a full block carries out of it.
        (local.set $escaped (select
          (i64.extend_i32_u (i64.lt_u (local.get $oddCarries) (local.get $backslash)))
          (i64.and (i64.shr_u (local.get $escapes) (i64.extend_i32_u (local.get $n))) (i64.const 1))
          (i32.eq (local.get $n) (i32.const 64))))

        ;; Unescaped quotes delimit strings. A prefix XOR of them sets the
        ;; bits from each opening quote up to, not including, its closing one.
        (local.set $quote (i64.and (i64.and (local.get $quote) (local.get $valid))
          (i64.xor (local.get $escapes) (i64.const -1))))
        (local.set $inside (local.get $quote))
        (local.set $inside (i64.xor (local.get $inside) (i64.shl (local.get $inside) (i64.const 1))))
        (local.set $inside (i64.xor (local.get $inside) (i64.shl (local.get $inside) (i64.const 2))))
        (local.set $inside (i64.xor (local.get $inside) (i64.shl (local.get $inside) (i64.const 4))))
        (local.set $inside (i64.xor (local.get $inside) (i64.shl (local.get $inside) (i64.const 8))))
        (local.set $inside (i64.xor (local.get $inside) (i64.shl (local.get $inside) (i64.const 16))))
        (local.set $inside (i64.xor (local.get $inside) (i64.shl (local.get $inside) (i64.const 32))))
        (local.set $inside (i64.xor (local.get $inside) (i64.sub (i64.const 0) (local.get $inString))))
        (local.set $inString (i64.and (i64.shr_u (local.get $inside) (local.get $last)) (i64.const 1)))

        ;; Strings contain no quotes, so subtracting the special characters in
        ;; strings from the quotes borrows from each special character up to
        ;; the closing quote of its string, which the XOR then picks out. A
        ;; string open from the previous block with one counts it at bit 0.
        ;; A borrow out of the block means the open string has had one.
        (local.set $specials (i64.or (i64.and (local.get $specials) (local.get $inside)) (local.get $special)))
        (local.set $flagged (i64.and (local.get $quote)
          (i64.xor (local.get $quote) (i64.sub (local.get $quote) (local.get $specials)))))
        (local.set $special (i64.extend_i32_u (i64.gt_u (local.get $specials) (local.get $quote))))

        ;; Scalars are runs of other bytes outside strings.
        (local.set $scalar (i64.and (local.get $valid)
          (i64.xor (i64.or (i64.or (local.get $quote) (local.get $delim)) (local.get $inside)) (i64.const -1))))
        (local.set $tokens
          (i64.or
            (i64.or
              (local.get $quote)
              (i64.and (i64.and (local.get $struct) (local.get $valid))
                (i64.xor (local.get $inside) (i64.const -1))))
            (i64.and (local.get $scalar)
              (i64.xor
                (i64.or (i64.shl (local.get $scalar) (i64.const 1)) (local.get $inScalar))
                (i64.const -1)))))
        (local.set $inScalar (i64.and (i64.shr_u (local.get $scalar) (local.get $last)) (i64.const 1)))

        (local.set $blockTokens (local.get $tokens))
        (local.set $index (i32.shr_u (local.get $out) (i32.const 1)))
        (block $written
          (loop $write
            (br_if $written (i64.eqz (local.get $tokens)))
            (local.set $bit (i64.ctz (local.get $tokens)))
            (i32.store16 (local.get $out)
              (i32.or
                (i32.add (i32.sub (local.get $pos) (local.get $base)) (i32.wrap_i64 (local.get $bit)))
                (i32.shl (i32.wrap_i64 (i64.shr_u (local.get $flagged) (local.get $bit))) (i32.const 15))))
            (local.set $out (i32.add (local.get $out) (i32.const 2)))
            (local.set $tokens (i64.and (local.get $tokens)
              (i64.sub (local.get $tokens) (i64.const 1))))
            (br $write)))
        ;; Brackets are matched in a separate loop, which only runs for them.
        (local.set $open (i64.and (local.get $open) (i64.and (local.get $valid)
          (i64.xor (local.get $inside) (i64.const -1)))))
        (local.set $close (i64.and (local.get $close) (i64.and (local.get $valid)
          (i64.xor (local.get $inside) (i64.const -1)))))
        (local.set $open (i64.or (local.get $open) (local.get $close)))
        (block $matched
          (loop $match
            (br_if $matched (i64.eqz (local.get $open)))
            (local.set $bit (i64.ctz (local.get $open)))
            ;; The index of the bracket's token among the tokens of the call.
            (local.set $at (i32.add (local.get $index) (i32.wrap_i64 (i64.popcnt (i64.and (local.get $blockTokens)
              (i64.sub (i64.shl (i64.const 1) (local.get $bit)) (i64.const 1)))))))
            (if (i32.wrap_i64 (i64.and (i64.shr_u (local.get $close) (local.get $bit)) (i64.const 1)))
              (then
                ;; Unbalanced closing brackets are left for the parser to reject.
                (if (local.get $depth)
                  (then
                    (local.set $depth (i32.sub (local.get $depth) (i32.const 1)))
                    (if (i32.lt_u (local.get $depth) (global.get $CAP))
                      (then (i32.store16
                        (i32.add (global.get $LINKS) (i32.shl
                          (i32.load16_u (i32.add (global.get $STACK) (i32.shl (local.get $depth) (i32.const 1))))
                          (i32.const 1)))
                        (local.get $at)))))))
              (else
                (i32.store16 (i32.add (global.get $LINKS) (i32.shl (local.get $at) (i32.const 1))) (i32.const 0))
                (if (i32.lt_u (local.get $depth) (global.get $CAP))
                  (then (i32.store16
                    (i32.add (global.get $STACK) (i32.shl (local.get $depth) (i32.const 1)))
                    (local.get $at))))
                (local.set $depth (i32.add (local.get $depth) (i32.const 1)))))
            (local.set $open (i64.and (local.get $open) (i64.sub (local.get $open) (i64.const 1))))
            (br $match)))

        (local.set $pos (i32.add (local.get $pos) (i32.const 64)))
        (br $blocks)))
    (i32.or
      (i32.shl (local.get $out) (i32.const 3))
      (i32.wrap_i64 (i64.or
        (i64.or (local.get $inString) (i64.shl (local.get $escaped) (i64.const 1)))
        (i64.or
          (i64.shl (local.get $inScalar) (i64.const 2))
          (i64.shl (i64.and (local.get $special) (local.get $inString)) (i64.const 3)))))))
;; Returns the length in UTF-16 code units of the UTF-8 text at [pos, end):
  ;; one unit per byte that starts a character, plus one more for each 4-byte
  ;; character. Reads up to 15 bytes past `end`.
  (func (export "utf16Length") (param $pos i32) (param $end i32) (result i32)
    (local $n i32) (local $v v128) (local $valid i32) (local $left i32)
    (local.set $pos (i32.add (local.get $pos) (global.get $BYTES)))
    (local.set $end (i32.add (local.get $end) (global.get $BYTES)))
    (block $done
      (loop $parts
        (br_if $done (i32.ge_u (local.get $pos) (local.get $end)))
        (local.set $v (v128.load (local.get $pos)))
        (local.set $left (i32.sub (local.get $end) (local.get $pos)))
        (local.set $valid (select (i32.const 0xffff)
          (i32.sub (i32.shl (i32.const 1) (local.get $left)) (i32.const 1))
          (i32.ge_u (local.get $left) (i32.const 16))))
        (local.set $n (i32.add (local.get $n)
          (i32.add
            (i32.popcnt (i32.and (local.get $valid) (i32.xor (i32.const -1) (i8x16.bitmask
              (i8x16.eq
                (v128.and (local.get $v) (i8x16.splat (i32.const 0xc0)))
                (i8x16.splat (i32.const 0x80)))))))
            (i32.popcnt (i32.and (local.get $valid) (i8x16.bitmask
              (i8x16.ge_u (local.get $v) (i8x16.splat (i32.const 0xf0)))))))))
        (local.set $pos (i32.add (local.get $pos) (i32.const 16)))
        (br $parts)))
    (local.get $n))
)
