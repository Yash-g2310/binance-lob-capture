# Assignment acceptance checklist

This checklist maps the supplied Software Developer assignment PDF to this repository. It distinguishes implemented behavior from live checks that still need to be run on the submitter's machine.

## Required functional deliverables

- [x] C++17 implementation with reproducible CMake build and documented dependencies.
- [x] Binance Spot combined WebSocket streams: `depth@100ms`, `depth5@100ms`, and `trade`.
- [x] USD-M Futures endpoint/path and the same stream families implemented.
- [x] Configurable comma-separated symbols; symbol validation and duplicate rejection.
- [x] Market-data CSV has the required 9-column header and writes the inner `data` payload as minified JSON with CSV escaping.
- [x] Local receive timestamp split into seconds and nanosecond remainder.
- [x] `shard_id`, `conn_epoch`, and per-epoch `conn_seq` are recorded.
- [x] Order-book CSV uses the exact 26-column header and integer values for price/size fields.
- [x] Price levels are ordered best-first; quantity zero removes a level.
- [x] `depth5` rows represent a partial top-five refresh; trades are audited but do not mutate the differential-depth book.
- [x] Fixed-point scale `10^8` is documented; binary floating point is not used to create the integer CSV values.
- [x] Spot `U/u` and USD-M `U/u/pu` sequence handling; connection epoch invalidates books and triggers resynchronization.
- [x] Bounded FIFO queue, single ordered consumer, buffered file I/O, counters, and output error handling.
- [x] Offline replay consumes the market CSV plus snapshot sidecar and makes no network calls.
- [x] Real 120-second Spot sample included as a matching market CSV, order-book CSV, and snapshot sidecar.

## Verification performed

- [x] Clean Release build completed without compiler warnings in the verification environment.
- [x] Debug build with AddressSanitizer and UndefinedBehaviorSanitizer completed.
- [x] Unit test executable passed, including malformed JSON/decimal cases, queue behavior, sequence gaps, quantity deletion, and malformed second-side update atomicity.
- [x] CTest replayed the included real capture without network access and compared the regenerated order-book CSV byte-for-byte to the captured one.
- [x] Included sample has 3,758 market rows and 2,357 order-book rows; schema and top-five checks passed.

## Explicit limitations / final local checks

- Multiple WebSocket shards, custom allocators/lock-free queues, and a benchmark suite are stretch goals and are not implemented.
- USD-M Futures code is implemented but the included live sample is Spot only; run a brief USD-M smoke test if time permits.
- The included live run ended with a TLS `stream truncated` condition during WebSocket shutdown. Final code reports this specific shutdown-only condition as non-fatal, but the updated shutdown message has not itself been verified by a second live capture. TLS peer and hostname verification remain enabled.
- Before submission, run `cmake -S . -B build -DCMAKE_BUILD_TYPE=Release`, `cmake --build build --parallel`, and `ctest --test-dir build --output-on-failure` on the target machine.
