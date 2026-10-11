# Binance WebSocket Capture + Local Order Book (C++17)

A C++17 market-data capture program for Binance Spot and USD-M Futures. It subscribes to combined `depth@100ms`, `depth5@100ms`, and `trade` streams, saves every valid inbound event's inner payload to a market-data CSV, maintains a snapshot-synchronized local order book, and emits top-five order-book rows.

## Status and scope

The project has a clean Release build, a sanitizer build, seven focused unit-test groups, and an offline replay regression against the included real 120-second Spot capture. The live sample was produced by the submitter on WSL2; this environment did not perform a new live capture after the final shutdown-message handling change.

Implemented:

- C++17, CMake, Boost.Beast/Asio, Boost.JSON, OpenSSL TLS.
- Spot and USD-M Futures stream endpoints and combined-stream URLs.
- One reader thread and one ordered processor/CSV thread connected by a bounded FIFO queue.
- CSV output with the required headers and RFC-4180 field escaping.
- Integer-only decimal conversion at fixed `10^8` price and quantity scales.
- REST depth snapshot plus sequence-aware differential book synchronization.
- Spot `U/u` continuity checks and USD-M `U/u/pu` checks.
- Reconnect epochs, exponential reconnect backoff, counters, graceful shutdown, and output failure handling.
- Offline replay using the required market-data CSV plus a sidecar JSONL log of REST snapshots; replay itself makes no network calls.
- Unit tests for decimal scaling, JSON parsing, atomic validation of both sides of a depth update, LOB updates/removals, sequence gaps, depth5 ordering, queue draining, and CSV formatting.
- CTest replay regression that rebuilds the included sample order-book CSV offline and compares it byte-for-byte with the captured output.

Not implemented as stretch goals: multiple WebSocket shards/connections, lock-free queues/custom allocators, and an advanced latency benchmark suite. The program supports multiple symbols over one combined-stream connection within Binance's stream-count and URL-size limits.

## Dependencies

Target platform: Linux (Ubuntu/Debian or Ubuntu under WSL2).

- GCC or Clang with C++17 support (the submitter verified GCC 15.2.0; a clean build was also checked with GCC 14.2.0)
- CMake 3.16+ (the submitter verified CMake 4.2.3; a clean build was also checked with CMake 3.31.6)
- Boost 1.75+ (Beast, Asio, JSON headers)
- OpenSSL development libraries
- pthreads (provided by the normal Linux toolchain)

Boost.JSON's implementation is compiled into `src/json_impl.cpp`, so a separately compiled Boost.JSON library is not required. No Binance API key or other credentials are needed; all endpoints used are public market-data endpoints.

Example install on Ubuntu:

```bash
sudo apt update
sudo apt install -y build-essential cmake ninja-build git libboost-all-dev libssl-dev zlib1g-dev
```

## Build

From the repository root:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
```

Run the offline unit tests:

```bash
ctest --test-dir build --output-on-failure
# Or run the test executable directly:
./build/unit_tests
```

For memory/undefined-behavior checks where supported:

```bash
cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=Debug -DENABLE_SANITIZERS=ON
cmake --build build-asan --parallel
ctest --test-dir build-asan --output-on-failure
```

## Live capture

The executable defaults to Spot, `BTCUSDT`, and `./output`, but explicit arguments are recommended for review.

Capture Spot BTCUSDT for two minutes:

```bash
./build/binance_capture \
  --venue spot \
  --symbols BTCUSDT \
  --output-dir ./output \
  --duration 120
```

Capture multiple Spot symbols:

```bash
./build/binance_capture \
  --venue spot \
  --symbols BTCUSDT,ETHUSDT \
  --output-dir ./output \
  --duration 120
```

Capture USD-M Futures:

```bash
./build/binance_capture \
  --venue usdm \
  --symbols BTCUSDT \
  --output-dir ./output \
  --duration 120
```

Omit `--duration` to run until `Ctrl+C`. `SIGINT`/`SIGTERM` trigger shutdown; the reader stops, queued messages are drained, and the writer flushes output. `--queue-capacity N` changes the bounded FIFO capacity (default 50,000 messages). `--help` prints all options.

Spot uses `stream.binance.com:9443` and `/stream?streams=...`. USD-M Futures uses `fstream.binance.com:443` and the `/public/stream?streams=...` path specified in the assignment brief. Each configured symbol subscribes to:

- `<symbol>@depth@100ms`
- `<symbol>@depth5@100ms`
- `<symbol>@trade`

Symbols are comma-separated and case-insensitive; output uses uppercase symbol names. There is one connection (`shard_id=0`). Symbols are validated as alphanumeric, duplicate symbols are rejected, and oversized combined URLs / more than 1,024 streams are rejected.

## Output files

For each configured symbol, the program creates these UTC-dated files when the first valid event for that symbol is processed:

```text
output/
  market_data_spot_BTCUSDT_YYYY-MM-DD.csv
  market_data_spot_BTCUSDT_YYYY-MM-DD_orderbook.csv
  market_data_spot_BTCUSDT_YYYY-MM-DD_snapshots.jsonl
```

For USD-M, `spot` in the filename is replaced with `usdm`.

### Market-data CSV

The header is exactly:

```text
recv_tsec,recv_tnsec,venue,stream_kind,shard_id,conn_epoch,conn_seq,symbol,payload_json
```

One row is written for each valid combined-stream WebSocket message consumed by the processor. `payload_json` contains the **inner `data` object**, serialized as one-line minified JSON and correctly CSV-escaped. `recv_tsec` and `recv_tnsec` are derived from the local wall-clock timestamp captured immediately after a complete WebSocket message is read. Their combination in nanoseconds is `recv_tsec * 1,000,000,000 + recv_tnsec`.

- `venue`: `spot` or `usdm`.
- `stream_kind`: `depth_diff`, `depth5`, or `trade`.
- `shard_id`: `0`, because the implementation uses a single WebSocket connection.
- `conn_epoch`: connection generation; a new connection attempt after disconnection uses a new epoch.
- `conn_seq`: starts at `1` and increases for each complete message read on that connection.
- Row order is FIFO processor order, preserving reader order.

### Order-book CSV

The header is exactly:

```text
tsec,tnsec,seqNo,id,type,side,bid0,bid1,bid2,bid3,bid4,bid_size0,bid_size1,bid_size2,bid_size3,bid_size4,ask0,ask1,ask2,ask3,ask4,ask_size0,ask_size1,ask_size2,ask_size3,ask_size4
```

There are 26 columns per row. Top-five levels are best-first: bids descending by price, asks ascending. Missing levels are written as zero. `seqNo` starts at `1` and increments per emitted row within each per-symbol order-book CSV; this also makes single-symbol replay sequence values reproducible. `id` is a deterministic positive 31-bit FNV-1a hash of the uppercase symbol; collisions among configured symbols are checked and rejected.

`type` / `side` mapping:

- `D`: applied differential depth update.
- `S`: emitted `depth5` partial top-five refresh.
- `B`: depth diff contained bid changes only.
- `S`: for order-book `side`, ask-side-only depth diff; this side mapping is independent of `type`.
- `N`: both sides or neither side changed, and all depth5 rows.
- Trades are recorded in the market CSV but do not emit order-book rows, because a trade does not itself update a differential-depth book.

Note: `S` is used as a type for `depth5`, and as a side for ask-only depth diffs; the columns distinguish those meanings.

### Integer scale policy

Binance prices and quantities arrive as decimal strings. The implementation parses them directly into scaled integers using `scale = 10^8`, without binary floating-point arithmetic on the path to CSV integer fields. For example, `65000.12345678` becomes `6500012345678`. Extra decimal digits are accepted only if they are trailing zeroes; nonzero precision beyond eight decimal places is rejected and logged as a processing problem rather than silently rounded.

### Snapshot sidecar

The REST depth snapshot is not a WebSocket message, so it must not be inserted into the required row-per-inbound-event market-data CSV. To make offline replay deterministic, every snapshot used to establish or resynchronize a book is recorded in a sibling `*_snapshots.jsonl` file, keyed by `conn_epoch` and `conn_seq` for the depth event being processed. If multiple snapshot attempts occur for the same event, the latest record is the one used by replay. Keep this sidecar with its market-data CSV.

## Order-book correctness and recovery

- Initial book state comes from a public REST depth snapshot (Spot limit 5,000 levels per side; USD-M Futures limit 1,000 levels per side).
- Spot depth diffs are checked with `U/u`: updates whose `u` is already covered by the book are ignored; a forward gap where `U > lastUpdateId + 1` triggers resynchronization; otherwise absolute level quantities are applied.
- USD-M depth diffs use `U/u` for initial snapshot overlap and require `pu == previous final update ID` after synchronization. Broken continuity triggers resynchronization.
- A new connection epoch invalidates all books; a later depth diff causes a new REST snapshot to be fetched. Reconnects use exponential backoff (1 second doubling up to 30 seconds); the delay resets after a connection remains healthy for at least 60 seconds.
- Quantity `0` deletes the price level; nonzero quantity replaces the quantity at that price.
- `depth5` is a **partial** top-five snapshot. It is output as a refresh row using its own top-five payload but does not erase the deeper REST/diff book, because the partial snapshot does not describe deeper levels.
- Snapshot REST failures leave the affected book unsynchronized, preserve market-data capture, and throttle further snapshot attempts for that symbol for two seconds. No differential order-book row is emitted unless the event was correctly applied.
- REST snapshots are finite (Spot at most 5,000 levels and Futures at most 1,000 levels here); deeper unchanged levels outside the snapshot are unknown until changed by later diffs. This is a market-data limitation, not a claim that the captured map is the entire exchange book.

## Threading, I/O, and backpressure

- The main thread owns the WebSocket and reads complete messages. It captures receive time, connection epoch and connection sequence, then pushes a message into a bounded FIFO queue.
- A single processor thread consumes the queue in order, parses each message once into Boost.JSON, writes the market-data row, owns/mutates the order books, and writes order-book snapshots. This avoids a mutex on each book update.
- The queue blocks the reader rather than silently dropping events when full. A sustained backlog can still cause network-side buffering/disconnection; reconnect sequence checks and snapshot resynchronization recover book correctness where possible. Messages never delivered by Binance during a disconnect cannot be recreated in the audit CSV.
- `std::ofstream` writes are buffered; output is flushed at shutdown. Output failures stop the program rather than silently claiming a complete capture.
- Shutdown drains queued work before the processor exits. Counters printed on exit include messages, event kinds, parse errors, sequence gaps, reconnects, CSV rows, REST errors, processing errors, snapshots and queue high-water mark.

## Replay (offline; no network requests)

Replay requires the market-data CSV **and its sibling `*_snapshots.jsonl` sidecar**:

```bash
./build/binance_capture \
  --replay ./output/market_data_spot_BTCUSDT_YYYY-MM-DD.csv \
  --output-dir ./replay_output
```

If `--output-dir` is omitted in replay mode, output goes to `replay_output` under the input CSV's directory. Replay reads the inner JSON payloads from the CSV, installs the recorded REST snapshots at the corresponding `(conn_epoch, conn_seq)`, validates depth sequencing, and generates a new order-book CSV without WebSocket or REST requests. Unsynchronized diff events that did not affect the live output are skipped until a recorded snapshot allows synchronization again.

## Tests

`ctest` runs focused offline checks for:

- exact fixed-point conversion, invalid decimal handling, overflow rejection, and excessive precision;
- combined stream envelope parsing, symbol fallback for depth5, malformed-envelope and unsupported-stream rejection;
- Spot snapshot bridging, stale event suppression, quantity replacement/deletion and sequence gap detection;
- USD-M initial overlap and `pu` continuity/gap detection;
- best-first depth5 ordering;
- CSV escaping, malformed quoted-field rejection, exact order-book header, and 26-column rows;
- bounded FIFO queue ordering, shutdown draining, close rejection, and high-water tracking.

## Included live sample capture

The `sample/` directory contains the submitter's real 120-second Spot BTCUSDT capture from 2026-10-10—not fabricated data—with all three files from the same run:

- `market_data_spot_BTCUSDT_2026-10-10.csv`: 3,758 market events.
- `market_data_spot_BTCUSDT_2026-10-10_orderbook.csv`: 2,357 order-book rows.
- `market_data_spot_BTCUSDT_2026-10-10_snapshots.jsonl`: REST snapshot used for synchronization and offline replay.

The sample was checked for the exact 9-column market header and 26-column order-book header, valid CSV row widths, parseable JSON payloads, best-first levels, and depth5 top-five consistency. Offline replay regenerated an order-book CSV that matched the captured order-book CSV byte for byte. A CTest regression test repeats this replay-and-compare check without network access.

The live capture reported 3,758 received/processed messages, zero parse errors, zero sequence gaps, zero REST errors and zero processing errors. It did report a TLS `stream truncated` close condition at shutdown. The client now classifies that specific shutdown-only condition as non-fatal and logs an explanatory note; TLS certificate and hostname verification remain enabled. This note does not claim a new live run was performed after the shutdown logging change.

## GitHub submission checklist

```bash
git init
git add CMakeLists.txt README.md .gitignore include src tests
git commit -m "Implement Binance market-data capture and local order book"
git branch -M main
git remote add origin https://github.com/<your-username>/binance-lob-capture.git
git push -u origin main
git tag -a v1.0.0 -m "Submission v1.0.0"
git push origin v1.0.0
```

Before pushing, run a clean Release build and `ctest`; make sure there are no credentials in the repository and attach the real 1–2 minute sample output requested by the assignment.
