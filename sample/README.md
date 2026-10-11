# Included live sample

These three files were captured by the submitter from Binance Spot BTCUSDT during a 120-second live run on 2026-10-10. They are the same-run market CSV, order-book CSV, and REST-snapshot sidecar required for deterministic replay.

Observed live counters: 3,758 messages received and processed; 1,181 depth updates; 1,181 depth5 messages; 1,396 trades; zero parse errors, sequence gaps, REST errors, or processing errors; queue high-water mark 84. Five depth diffs were already covered by the REST snapshot and were correctly not applied as duplicate order-book updates, while all inbound messages remained in the market-data CSV.

Offline replay from the market CSV plus the snapshot sidecar reproduces the included order-book CSV byte-for-byte. This regression is registered in CTest. The capture's shutdown reported a TLS `stream truncated` condition; the final code treats this specific shutdown-only condition as non-fatal while keeping TLS verification enabled. The code change has not been live-tested after this capture.
