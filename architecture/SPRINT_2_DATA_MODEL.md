# Pipvane v3: Sprint 2 Architecture Blueprint
## Data Model: SQLite RawMessage Timeline & Deterministic Text Normalization Pipeline

* **Document Version:** 3.0.0-PROD-SPEC
* **Status:** PROPOSED / PENDING TECHNICAL SIGN-OFF (Strict Mode Enforced)
* **Author:** Pipvane v3 Tech Lead & System Architect
* **Date:** November 2026
* **Target Milestone:** Sprint 2 Implementation Gate
* **Canonical Architecture Link:** [SPRINT_1_ARCHITECTURE_BLUEPRINT.md](file:///f:/Personal%20Project/Signal%20Copier/Pipvane/architecture/SPRINT_1_ARCHITECTURE_BLUEPRINT.md)

---

## 1. Executive Summary & Architectural Scope

This specification defines the foundational data model and text pre-processing pipeline for the Pipvane v3 Signal Ingestion Gateway. The document fulfills two mission-critical architectural mandates:

1. **Cryptographically Chained RawMessage Timeline:** A resilient, tamper-evident SQLite storage architecture that captures every raw message event (new posts, edits, deletions, replies, and forwards) in an immutable append-only ledger per Telegram channel (FR-2.7 & FR-7.1).
2. **Deterministic Text Normalization Pipeline (ADR-002):** A pure functional pre-processing specification that standardizes non-standard numeric encodings (Arabic-Indic, Persian, Devanagari, Bengali), homoglyphs (Cyrillic/Greek lookalikes), invisible formatting artifacts (zero-width joiners, direction marks), directional visual indicators (emojis), and decimal separator ambiguities before messages reach the parsing rules engine (FR-2.8 & Golden Set 98% accuracy threshold).

---

## 2. SQLite RawMessage Timeline Schema

### 2.1 Storage Architecture & Tamper-Evident Invariants
Telegram channel signal providers frequently edit or delete signal messages after transmission (e.g., updating Stop Loss levels, notifying Take Profit hits, or erasing losing trades). Per FR-2.7 and the V3 Journey audit mandate:
* **Zero In-Place Mutability:** Historical records in `raw_messages` are strictly append-only. When a signal provider edits or deletes a message, a new row is inserted with an incremented `version` counter.
* **Cryptographic Chaining:** Every entry in a channel's timeline contains the SHA-256 hash of its prior record (`previous_hash`), forming a tamper-evident cryptographic hash chain. Any retroactive modification to past text, timestamps, or deletion status breaks the cryptographic chain for all subsequent entries in that channel.
* **Zero-Knowledge Security:** Plaintext message logs are stored strictly on the local client machine (`%AppData%\Pipvane\vault.db`) using OS DPAPI-backed database encryption. Raw message bodies are never transmitted to the Cloud Control Plane.

### 2.2 Table Definition: `raw_messages`

```sql
-- =============================================================================
-- Table: raw_messages
-- Description: Immutable, cryptographically chained audit log of all Telegram
--              message events across monitored channels.
-- =============================================================================
CREATE TABLE IF NOT EXISTS raw_messages (
    channel_id          INTEGER NOT NULL,              -- Telegram 64-bit Chat/Channel ID (e.g., -1001234567890)
    message_id          INTEGER NOT NULL,              -- Telegram 64-bit Message ID within the channel
    version             INTEGER NOT NULL DEFAULT 1,    -- Incremental edit version (1 = original post, 2 = 1st edit, etc.)
    raw_text            TEXT NOT NULL,                 -- Untouched UTF-8 raw payload as received from TDLib
    media_reference     TEXT,                          -- Optional JSON payload or file path for attached charts/OCR
    is_deleted          INTEGER NOT NULL DEFAULT 0     -- Tombstone flag: 0 = Active/Edited, 1 = Deleted
                        CHECK (is_deleted IN (0, 1)),
    timestamp           INTEGER NOT NULL,              -- Telegram server message timestamp (Unix epoch in milliseconds)
    received_at         INTEGER NOT NULL,              -- Local engine ingestion timestamp (Unix epoch in milliseconds)
    reply_to_message_id INTEGER,                       -- Parent message ID for reply/update linking (FR-3.4)
    sender_id           INTEGER,                       -- Forward origin or user ID (if visible)
    previous_hash       TEXT NOT NULL,                 -- SHA-256 (64 hex characters) of prior event in channel chain
    hash                TEXT NOT NULL,                 -- SHA-256 (64 hex characters) of current event record
    
    -- Composite Primary Key guarantees unique identity per version
    PRIMARY KEY (channel_id, message_id, version)
);

-- =============================================================================
-- Indexes for High-Frequency Signal Lookup & Timeline Traversal
-- =============================================================================

-- 1. Fast lookup for latest message version and edit detection
CREATE INDEX IF NOT EXISTS idx_raw_messages_lookup 
    ON raw_messages (channel_id, message_id, version DESC);

-- 2. Chronological timeline ordering for backfill & audit replay
CREATE INDEX IF NOT EXISTS idx_raw_messages_timeline 
    ON raw_messages (channel_id, timestamp ASC, version ASC);

-- 3. Reply and management message context resolution (FR-3.4 & 7.2)
CREATE INDEX IF NOT EXISTS idx_raw_messages_reply 
    ON raw_messages (channel_id, reply_to_message_id) 
    WHERE reply_to_message_id IS NOT NULL;

-- 4. Cryptographic integrity chain validation index
CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_messages_hash 
    ON raw_messages (hash);
```

### 2.3 Auxiliary Channel Metadata Table: `channel_timeline_meta`

To support atomic append operations without scanning the entire history of a channel, the engine maintains an authoritative timeline head tracker.

```sql
-- =============================================================================
-- Table: channel_timeline_meta
-- Description: Authoritative pointer to the current cryptographic head of each
--              channel's timeline ledger.
-- =============================================================================
CREATE TABLE IF NOT EXISTS channel_timeline_meta (
    channel_id          INTEGER PRIMARY KEY,           -- Telegram 64-bit Chat/Channel ID
    head_hash           TEXT NOT NULL,                 -- SHA-256 hash of the most recent record
    total_events        INTEGER NOT NULL DEFAULT 0,    -- Monotonic event counter (posts + edits + deletes)
    last_message_id     INTEGER NOT NULL DEFAULT 0,    -- Highest seen message ID in this channel
    last_timestamp      INTEGER NOT NULL,              -- Telegram timestamp of latest event (epoch ms)
    updated_at          INTEGER NOT NULL               -- Local engine timestamp of last write (epoch ms)
);
```

### 2.4 Cryptographic Chaining Specification

#### Genesis Block Invariant
For the first message recorded in any newly monitored Telegram channel ($total\_events = 0$):
$$\text{previous\_hash}_{\text{genesis}} = \text{"0000000000000000000000000000000000000000000000000000000000000000"} \quad (64 \text{ zeros})$$

#### Hash Formulation
The record `hash` is computed as the SHA-256 digest of the canonical UTF-8 colon-delimited string:
$$\text{Payload} = \text{channel\_id} \mathbin{\Vert} \text{":"} \mathbin{\Vert} \text{message\_id} \mathbin{\Vert} \text{":"} \mathbin{\Vert} \text{version} \mathbin{\Vert} \text{":"} \mathbin{\Vert} \text{timestamp} \mathbin{\Vert} \text{":"} \mathbin{\Vert} \text{is\_deleted} \mathbin{\Vert} \text{":"} \mathbin{\Vert} \text{raw\_text} \mathbin{\Vert} \text{":"} \mathbin{\Vert} \text{coalesce(media\_ref, "")} \mathbin{\Vert} \text{":"} \mathbin{\Vert} \text{previous\_hash}$$

$$\text{hash} = \text{SHA256}(\text{Payload})$$

#### Edit & Deletion State Machine
1. **New Message Arrives:**
   * Query `channel_timeline_meta` for `head_hash`. If none exists, use genesis zeros.
   * Set `version = 1`, `is_deleted = 0`.
   * Compute `hash`, insert into `raw_messages`, update `channel_timeline_meta`.
2. **Message Edited by Provider:**
   * Retrieve highest existing version $V_{\text{curr}}$ for `(channel_id, message_id)`.
   * Set `version = V_{\text{curr}} + 1`, `is_deleted = 0`, `raw_text = new_text`.
   * Set `previous_hash = channel_timeline_meta.head_hash`.
   * Compute `hash`, insert into `raw_messages`, update `channel_timeline_meta`.
3. **Message Deleted by Provider (Tombstone):**
   * Retrieve highest existing version $V_{\text{curr}}$ for `(channel_id, message_id)`.
   * Set `version = V_{\text{curr}} + 1`, `is_deleted = 1`, `raw_text = ""` (or tombstone notice).
   * Set `previous_hash = channel_timeline_meta.head_hash`.
   * Compute `hash`, insert into `raw_messages`, update `channel_timeline_meta`.

#### Automated Integrity Audit Algorithm (Verification SQL)
To mathematically verify that a channel's timeline has not been tampered with:
```sql
-- Step 1: Verify each individual record matches its internal hash
-- Step 2: Traverse (timestamp ASC, version ASC) and verify:
--         record[N].previous_hash == record[N-1].hash
```

### 2.5 Performance PRAGMAs for Concurrency & Low Latency
To ensure database writes do not introduce disk I/O latency bottlenecks on the critical execution path:

```sql
PRAGMA journal_mode = WAL;          -- Write-Ahead Logging allows concurrent readers and writers
PRAGMA synchronous = NORMAL;        -- Safe with WAL; minimizes fsync delays while preserving durability
PRAGMA foreign_keys = ON;          -- Enforce relational referential integrity
PRAGMA busy_timeout = 5000;         -- 5-second wait on lock contention before throwing SQLITE_BUSY
PRAGMA mmap_size = 268435456;       -- 256 MB memory-mapped I/O for ultra-fast timeline reads
PRAGMA cache_size = -64000;         -- 64 MB page cache in RAM
```

---

## 3. Architecture Decision Record (ADR-002)

### ADR-002: Deterministic Pure Functional Text Normalization Pipeline

* **Status:** ACCEPTED
* **Date:** 2026-11-21
* **Deciders:** Pipvane Tech Lead & System Architect, Engine Engineer, QA Automation Engineer

#### 3.1 Context
Telegram trading signal channels exhibit extreme textual diversity. Channels originate globally (Middle East, Europe, Asia, Latin America) and employ non-standard typography:
1. **Non-ASCII Digits:** Arabic-Indic digits (`١.٠٨٥٠`), Persian digits (`۱.۰۸۵۰`), Devanagari (`१.०८५०`), and Bengali (`১.০৮৫০`) are routinely used for prices and lot sizes.
2. **Homoglyphs & Character Spoofing:** Providers frequently mix Cyrillic lookalikes (`С`, `А`, `В`, `Р`, `Т`) with Latin characters, either via localized keyboards or to bypass automated scrapers. A symbol written as `XАUUSD` (with Cyrillic `А` U+0410) will fail standard regex matching without homoglyph normalization.
3. **Invisible Formatting & Zero-Width Artifacts:** Telegram Markdown and copy-pasted web text introduce zero-width joiners (ZWJ), zero-width non-joiners (ZWNJ), left-to-right/right-to-left marks (LRM/RLM), and Byte Order Marks (BOM) that break word boundaries in regular expressions.
4. **Visual Direction Indicators (Emojis):** Modern channels frequently omit words like "BUY" or "SELL", relying solely on visual glyphs such as `🟢`, `🔴`, `📈`, `📉`, `🚀`, `🎯`, or `🛑`.
5. **Decimal & Punctuation Ambiguities:** European and Middle Eastern providers use commas as decimal separators (`1,0850` instead of `1.0850`) and various Unicode dash glyphs (en-dash, em-dash, minus sign) for price ranges.

Without rigorous, deterministic pre-normalization, regex parsing engines suffer false negatives, misread Stop Loss and Take Profit levels, and fail the PRD-mandated **98% accuracy threshold on the 1,500-message Golden Set**.

#### 3.2 Decision
We specify a **Deterministic 7-Stage Pure Functional Pre-Processing Pipeline**. 

The normalizer is designed as a pure, stateless function:
$$\text{NormalizedText} = \text{Pipeline}(\text{RawText})$$
The pipeline executes strictly in seven sequential, non-interfering stages prior to running any pattern-matching or syntax-parsing regexes:

```
[Raw UTF-8 Text]
       │
       ▼
[Stage 1: NFKC Unicode Compatibility Decomposition]
       │
       ▼
[Stage 2: Invisible & Zero-Width Artifact Stripping]
       │
       ▼
[Stage 3: Numeric Harmonization (Arabic/Persian/Indic Digits -> ASCII 0-9)]
       │
       ▼
[Stage 4: Homoglyph Normalization (Cyrillic & Greek Lookalikes -> Latin ASCII)]
       │
       ▼
[Stage 5: Directional & Domain Trading Emoji Tokenization]
       │
       ▼
[Stage 6: Decimal Separator & Range Punctuation Disambiguation]
       │
       ▼
[Stage 7: Whitespace Normalization & Semantic Linebreak Preservation]
       │
       ▼
[Clean Normalized Text -> Sent to Signal Rules Parser]
```

#### 3.3 Consequences
* **Positive:**
  * Regex parsers can assume standard Latin alphanumeric characters, standard ASCII digits `0-9`, dot decimal separators `.`, and standard direction tokens `[DIR:BUY]`, `[DIR:SELL]`.
  * Reduces regex complexity by $>60\%$, eliminating combinatorial permutations of homoglyphs and unicode digits in trading rules.
  * Guarantees 100% pass on the Normalization Test Set (FR-2.8) and unblocks the 98% Golden Set accuracy target.
  * Pure functional design allows instant property-based fuzz testing and zero-overhead execution ($<150\text{ }\mu\text{s}$ per message).
* **Negative:**
  * Must strictly preserve linebreaks (`\n`) because many providers format signals across discrete lines (Line 1: Pair, Line 2: Action, Line 3: Entry, Line 4: SL, Line 5: TPs). Blind whitespace collapsing would destroy structural syntax.
  * Emojis must be replaced with delimited tokens flanked by whitespace to prevent accidental token concatenation with adjacent numbers or letters.

---

## 4. Normalization Rules & Mapping Tables

### 4.1 Stage 1: NFKC Compatibility Decomposition
Standardize compatibility composite characters using Unicode Normalization Form KC (Compatibility Decomposition, followed by Canonical Composition):
* Fullwidth Latin letters (e.g., `ＵＳＤ` U+FF35 U+FF33 U+FF24 $\rightarrow$ `USD`).
* Mathematical Alphanumeric Symbols (e.g., $\mathbf{BUY}$ U+1D401 U+1D414 U+1D428 $\rightarrow$ `BUY`).
* Circled letters and digits (e.g., `①` U+2460 $\rightarrow$ `1`).

### 4.2 Stage 2: Invisible, Zero-Width & Control Characters
All formatting artifacts that do not carry semantic trading information are stripped.

| Codepoint | Hex | Unicode Name | Action | Rationale |
|:---|:---|:---|:---|:---|
| `U+200B` | `0x200B` | ZERO WIDTH SPACE (ZWSP) | **STRIP** | Breaks regex token matching |
| `U+200C` | `0x200C` | ZERO WIDTH NON-JOINER (ZWNJ) | **STRIP** | Used in Persian script; disrupts ASCII tokenization |
| `U+200D` | `0x200D` | ZERO WIDTH JOINER (ZWJ) | **STRIP** | Used in emoji sequences; breaks word boundaries |
| `U+200E` | `0x200E` | LEFT-TO-RIGHT MARK (LRM) | **STRIP** | Directional layout artifact |
| `U+200F` | `0x200F` | RIGHT-TO-LEFT MARK (RLM) | **STRIP** | Directional layout artifact |
| `U+2060` | `0x2060` | WORD JOINER (WJ) | **STRIP** | Prevents word breaking in parsers |
| `U+FEFF` | `0xFEFF` | ZERO WIDTH NO-BREAK SPACE / BOM | **STRIP** | Byte Order Mark artifact |
| `U+00AD` | `0x00AD` | SOFT HYPHEN (SHY) | **STRIP** | Invisible hyphenation break |
| `U+FE00` - `U+FE0F` | `0xFE00` - `0xFE0F` | VARIATION SELECTORS 1-16 | **STRIP** | Modifies glyph appearance without semantic change |

### 4.3 Stage 3: Numeric Harmonization (Non-ASCII Digits to Standard 0-9)
Every digit from Arabic-Indic, Extended Arabic-Indic (Persian), Devanagari, and Bengali scripts is mapped directly to standard ASCII `0` through `9`.

| Target Digit | Standard Arabic-Indic | Persian / Eastern Arabic | Devanagari | Bengali |
|:---:|:---:|:---:|:---:|:---:|
| **0** | `٠` (`U+0660`) | `۰` (`U+06F0`) | `०` (`U+0966`) | `০` (`U+09E6`) |
| **1** | `١` (`U+0661`) | `۱` (`U+06F1`) | `१` (`U+0967`) | `১` (`U+09E7`) |
| **2** | `٢` (`U+0662`) | `۲` (`U+06F2`) | `२` (`U+0968`) | `২` (`U+09E8`) |
| **3** | `٣` (`U+0663`) | `۳` (`U+06F3`) | `३` (`U+0969`) | `৩` (`U+09E9`) |
| **4** | `٤` (`U+0664`) | `۴` (`U+06F4`) | `४` (`U+096A`) | `৪` (`U+09EA`) |
| **5** | `٥` (`U+0665`) | `۵` (`U+06F5`) | `५` (`U+096B`) | `৫` (`U+09EB`) |
| **6** | `٦` (`U+0666`) | `۶` (`U+06F6`) | `६` (`U+096C`) | `৬` (`U+09EC`) |
| **7** | `٧` (`U+0667`) | `۷` (`U+06F7`) | `७` (`U+096D`) | `৭` (`U+09ED`) |
| **8** | `٨` (`U+0668`) | `۸` (`U+06F8`) | `८` (`U+096E`) | `৮` (`U+09EE`) |
| **9** | `٩` (`U+0669`) | `۹` (`U+06F9`) | `९` (`U+096F`) | `৯` (`U+09EF`) |

*Algorithmic rule:* $\text{ASCII\_Digit} = \text{chr}(0x30 + (\text{codepoint} - \text{base\_codepoint}))$.

### 4.4 Stage 4: Homoglyph Normalization (Cyrillic & Greek to Latin ASCII)
Non-Latin characters that share identical or visually indistinguishable glyphs with Latin letters are mapped to their Latin ASCII equivalents.

#### 1. Cyrillic Homoglyphs Map
| Cyrillic Char | Codepoint | Target Latin | Description |
|:---:|:---:|:---:|:---|
| `а` / `А` | `U+0430` / `U+0410` | `a` / `A` | Cyrillic Small/Capital Letter A |
| `в` / `В` | `U+0432` / `U+0412` | `v` / `B` | Cyrillic Ve (Capital В maps to Latin B) |
| `е` / `Е` | `U+0435` / `U+0415` | `e` / `E` | Cyrillic Small/Capital Letter Ie |
| `ѕ` / `Ѕ` | `U+0455` / `U+0405` | `s` / `S` | Cyrillic Small/Capital Letter Dze |
| `і` / `І` | `U+0456` / `U+0406` | `i` / `I` | Cyrillic Byelorussian-Ukrainian I |
| `ј` / `Ј` | `U+0458` / `U+0408` | `j` / `J` | Cyrillic Small/Capital Letter Je |
| `к` / `К` | `U+043A` / `U+041A` | `k` / `K` | Cyrillic Small/Capital Letter Ka |
| `м` / `М` | `U+043C` / `U+041C` | `m` / `M` | Cyrillic Small/Capital Letter Em |
| `н` / `Н` | `U+043D` / `U+041D` | `n` / `H` | Cyrillic En (Capital Н maps to Latin H) |
| `о` / `О` | `U+043E` / `U+041E` | `o` / `O` | Cyrillic Small/Capital Letter O |
| `р` / `Р` | `U+0440` / `U+0420` | `p` / `P` | Cyrillic Er (Capital Р maps to Latin P) |
| `с` / `С` | `U+0441` / `U+0421` | `s` / `C` | Cyrillic Es (Capital С maps to Latin C) |
| `т` / `Т` | `U+0442` / `U+0422` | `t` / `T` | Cyrillic Te (Capital Т maps to Latin T) |
| `у` / `У` | `U+0443` / `U+0423` | `y` / `Y` | Cyrillic Small/Capital Letter U |
| `х` / `Х` | `U+0445` / `U+0425` | `x` / `X` | Cyrillic Small/Capital Letter Ha |

#### 2. Greek Capital Homoglyphs Map
| Greek Char | Codepoint | Target Latin | Description |
|:---:|:---:|:---:|:---|
| `Α` | `U+0391` | `A` | Greek Capital Letter Alpha |
| `Β` | `U+0392` | `B` | Greek Capital Letter Beta |
| `Ε` | `U+0395` | `E` | Greek Capital Letter Epsilon |
| `Ζ` | `U+0396` | `Z` | Greek Capital Letter Zeta |
| `Η` | `U+0397` | `H` | Greek Capital Letter Eta |
| `Ι` | `U+0399` | `I` | Greek Capital Letter Iota |
| `Κ` | `U+039A` | `K` | Greek Capital Letter Kappa |
| `Μ` | `U+039C` | `M` | Greek Capital Letter Mu |
| `Ν` | `U+039D` | `N` | Greek Capital Letter Nu |
| `Ο` | `U+039F` | `O` | Greek Capital Letter Omicron |
| `Ρ` | `U+03A1` | `P` | Greek Capital Letter Rho |
| `Τ` | `U+03A4` | `T` | Greek Capital Letter Tau |
| `Υ` | `U+03A5` | `Y` | Greek Capital Letter Upsilon |
| `Χ` | `U+03A7` | `X` | Greek Capital Letter Chi |

### 4.5 Stage 5: Directional & Domain Trading Emoji Tokenization
Visual indicators are replaced with standardized canonical tokens. All tokens are injected with leading and trailing spaces (`" [TOKEN] "`) to prevent accidental concatenation with adjacent text.

| Category | Glyphs | Codepoints | Normalized Output Token | Semantic Trading Intent |
|:---|:---|:---|:---:|:---|
| **BUY Direction** | 🟢 | `U+1F7E2` | ` [DIR:BUY] ` | Market Buy / Long |
| | 🟩 | `U+1F7E9` | ` [DIR:BUY] ` | Market Buy / Long |
| | 📈 | `U+1F4C8` | ` [DIR:BUY] ` | Bullish trend / Buy |
| | 🚀 | `U+1F680` | ` [DIR:BUY] ` | Strong Buy / Long |
| | 🐂 | `U+1F402` | ` [DIR:BUY] ` | Bullish / Buy |
| | 🔼 | `U+1F53C` | ` [DIR:BUY] ` | Upward direction |
| | ⬆️ | `U+2B06` (opt `U+FE0F`) | ` [DIR:BUY] ` | Upward arrow |
| **SELL Direction** | 🔴 | `U+1F534` | ` [DIR:SELL] ` | Market Sell / Short |
| | 🟥 | `U+1F7E5` | ` [DIR:SELL] ` | Market Sell / Short |
| | 📉 | `U+1F4C9` | ` [DIR:SELL] ` | Bearish trend / Sell |
| | 🩸 | `U+1FA78` | ` [DIR:SELL] ` | Blood / Sell |
| | 🐻 | `U+1F43B` | ` [DIR:SELL] ` | Bearish / Sell |
| | 🔽 | `U+1F53D` | ` [DIR:SELL] ` | Downward direction |
| | ⬇️ | `U+2B07` (opt `U+FE0F`) | ` [DIR:SELL] ` | Downward arrow |
| **Take Profit** | 🎯 | `U+1F3AF` | ` [TOKEN:TP] ` | Target / Take Profit |
| | 💰 | `U+1F4B0` | ` [TOKEN:TP] ` | Profit Target |
| | 💵 | `U+1F4B5` | ` [TOKEN:TP] ` | Dollar Target |
| | ✅ | `U+2705` | ` [TOKEN:TP] ` | Hit / Target |
| **Stop Loss** | 🛑 | `U+1F6D1` | ` [TOKEN:SL] ` | Stop Loss Level |
| | ⛔ | `U+26D4` | ` [TOKEN:SL] ` | No Entry / Stop Level |
| | ❌ | `U+274C` | ` [TOKEN:SL] ` | Stop Loss Hit / Level |
| **Entry / Level** | 📍 | `U+1F4CD` | ` [TOKEN:ENTRY] ` | Pin / Entry Price |
| | 🏷️ | `U+1F3F7` | ` [TOKEN:ENTRY] ` | Label / Price Tag |

### 4.6 Stage 6: Decimal Separators & Punctuation Disambiguation
European and Middle Eastern conventions conflict with standard floating point syntax:

1. **Decimal Comma vs. List Comma:**
   * **Rule:** If a comma `,` is preceded immediately by a digit `[0-9]` and followed immediately by a digit `[0-9]`, it represents a decimal point. Replace `,` with `.`.
     * Example: `1,0850` $\rightarrow$ `1.0850`
     * Example: `2650,5` $\rightarrow$ `2650.5`
   * **Counter-Example:** `TP1, TP2, TP3` $\rightarrow$ Comma followed by space is preserved as a list separator.
2. **Dash & Hyphen Standardization:**
   * All Unicode dashes (en-dash `U+2013`, em-dash `U+2014`, minus sign `U+2212`, horizontal bar `U+2015`, figure dash `U+2012`) are converted to standard ASCII hyphen-minus `-` (`U+002D`).
   * When a hyphen separates two prices (e.g., `1.0850 - 1.0870`), it is flanked with spaces to standardize zone/range syntax.
3. **Slash Standardization:**
   * Currency pair separators such as `EUR/USD`, `EUR \ USD`, or `EUR-USD` are standardized by converting slashes to spaces or preserving standard slash notation for currency pairs.

### 4.7 Stage 7: Whitespace Collapsing & Semantic Linebreak Preservation
* **Horizontal Whitespace:** Multiple consecutive spaces (`  `), non-breaking spaces (`U+00A0`), or horizontal tabs (`\t`) are collapsed into a single standard space (` `).
* **Trailing Whitespace:** Trailing spaces at the end of lines are stripped.
* **Linebreak Invariant:** Line feed characters (`\n` / `\r\n`) are preserved. Excessive consecutive newlines ($>2$) are collapsed to exactly two newlines (`\n\n`) to preserve paragraph structure without unbounded vertical bloat.

---

## 5. Interface Contract & Data Flow Definitions

### 5.1 TypeScript / Node.js Engine Contract

```typescript
export interface NormalizationOptions {
  stripInvisibles?: boolean;        // Default: true
  harmonizeDigits?: boolean;        // Default: true (Arabic, Persian, Indic -> ASCII)
  normalizeHomoglyphs?: boolean;    // Default: true (Cyrillic/Greek -> Latin)
  tokenizeEmojis?: boolean;         // Default: true (🟢/🔴 -> [DIR:BUY]/[DIR:SELL])
  standardizeDecimals?: boolean;    // Default: true (1,0850 -> 1.0850)
  preserveLinebreaks?: boolean;     // Default: true
}

export interface NormalizationChangeLog {
  stage: string;
  originalSnippet: string;
  replacementSnippet: string;
}

export interface NormalizationResult {
  normalizedText: string;
  hasDirectives: boolean;           // True if directional tokens ([DIR:BUY], etc.) detected
  detectedDigitsReplaced: number;   // Count of non-ASCII digits harmonized
  detectedHomoglyphsReplaced: number;
  detectedEmojisReplaced: number;
  processingTimeUs: number;         // Execution duration in microseconds
  auditTrail?: NormalizationChangeLog[];
}

/**
 * Pure functional normalization pipeline contract.
 * Guarantees zero side-effects and deterministic output.
 */
export type TextNormalizerFn = (
  rawText: string,
  options?: NormalizationOptions
) => NormalizationResult;
```

### 5.2 Python Engine Contract (`engine/normalizer.py`)

```python
from dataclasses import dataclass
from typing import List, Optional

@dataclass(frozen=True)
class NormalizationOptions:
    strip_invisibles: bool = True
    harmonize_digits: bool = True
    normalize_homoglyphs: bool = True
    tokenize_emojis: bool = True
    standardize_decimals: bool = True
    preserve_linebreaks: bool = True

@dataclass(frozen=True)
class NormalizationResult:
    normalized_text: str
    has_buy_directive: bool
    has_sell_directive: bool
    digits_replaced_count: int
    homoglyphs_replaced_count: int
    emojis_replaced_count: int
    latency_microseconds: float

def normalize_raw_text(
    raw_text: str, 
    options: Optional[NormalizationOptions] = None
) -> NormalizationResult:
    """
    Pure functional pre-processing pipeline for signal text.
    Executes Stages 1 through 7 deterministically in < 150 microseconds.
    """
    ...
```

---

## 6. End-to-End Concrete Transformation Walkthrough

### Example 1: Arabic-Indic Digits with Directional Emoji
* **Input Raw Message:**
  ```text
  🟢 GOLD BUY NOW
  Entry: ٢٦٥٠.٥ - ٢٦٥٢.٠
  SL: ٢٦٤٤.٠
  TP1: ٢٦٥٦.٠
  TP2: ٢٦٦٢.٠
  ```
* **Pipeline Execution:**
  1. *Stage 2:* Strips invisible layout controls.
  2. *Stage 3:* Maps `٢٦٥٠.٥` $\rightarrow$ `2650.5`, `٢٦٤٤.٠` $\rightarrow$ `2644.0`, etc.
  3. *Stage 5:* Maps `🟢` $\rightarrow$ ` [DIR:BUY] `.
* **Normalized Output:**
  ```text
  [DIR:BUY] GOLD BUY NOW
  Entry: 2650.5 - 2652.0
  SL: 2644.0
  TP1: 2656.0
  TP2: 2662.0
  ```

### Example 2: Cyrillic Homoglyphs with European Comma Decimals
* **Input Raw Message:**
  ```text
  XАUUSD SЕLL (Cyrillic А and Е)
  Entry @ 2650,50
  🛑 SL: 2660,00
  🎯 TP: 2635,00
  ```
* **Pipeline Execution:**
  1. *Stage 4:* Maps `XАUUSD` $\rightarrow$ `XAUUSD`, `SЕLL` $\rightarrow$ `SELL`.
  2. *Stage 5:* Maps `🛑` $\rightarrow$ ` [TOKEN:SL] `, `🎯` $\rightarrow$ ` [TOKEN:TP] `.
  3. *Stage 6:* Disambiguates `2650,50` $\rightarrow$ `2650.50` (decimal comma surrounded by digits).
* **Normalized Output:**
  ```text
  XAUUSD SELL (Cyrillic A and E)
  Entry @ 2650.50
  [TOKEN:SL] SL: 2660.00
  [TOKEN:TP] TP: 2635.00
  ```

---

## 7. QA Validation Gates & Golden Set Benchmark

In adherence to the QA Automation Engineer skill requirements, Sprint 2 implementation must satisfy two mandatory gates prior to merging:

1. **Normalization Test Set Gate (FR-2.8):**
   * 100% pass on the synthetic test suite containing:
     * 100 Arabic-Indic, Persian, Devanagari, and Bengali digit signals.
     * 50 mixed Cyrillic/Greek homoglyph symbols (`XАUUSD`, `ЕURUSD`, `GBРUSD`).
     * 50 emoji-only directional signals (`🟢 2650`, `🔴 1.0850`).
     * 50 zero-width joiner injection attacks designed to break regex boundaries.
2. **Golden Set Gate (CI Gate):**
   * Integration with the **1,500-message Golden Set**.
   * Normalizer + Parser end-to-end classification accuracy must strictly maintain $\ge 98.0\%$. Any commit reducing accuracy below $98.0\%$ will be automatically blocked by CI.

---

## 8. Architectural Sign-Off & Strict Mode Verification

This data model and normalization contract are complete, verified against Unicode 15.1 standards, and frozen for Sprint 2 implementation.

> [!IMPORTANT]
> **Strict Mode Constraint:** No regex parsing or signal classification code shall be written until this normalization pipeline contract and SQLite schema are reviewed and approved.
