# Shopping Sessions — Buckeye boys

A single Rust binary (`src/main.rs`) that implements `rules.md` for the Databricks Shopping Sessions track.
The Dockerfile builds it; the image's `CMD` reads `/data` and writes `/output/user_daily/` and `/output/campaign_daily/` as Parquet.

## How it works

1. **Read.** Every CSV is mmapped and cut into line-aligned chunks. A SIMD tokenizer finds `\n`, `,` and `"` 64 bytes at a time, applying the rules' CSV dialect exactly.
2. **Links**, built on a separate thread while events parse: union-find over the linked IDs, person = byte-wise smallest ID, stored as flat open-addressing tables.
3. **Events.** Each event is parsed, validated, and counting-sorted by event-id hash into exact-size buffers.
4. **Clicks** are parsed, validated and mapped to persons.
5. **Dedup** per event-id partition (smallest `ingest_ts` wins, before validation). Survivors are mapped to persons with prefetched link lookups and partitioned by person.
6. **Sessions** are built per person partition: radix-group by person, then sessionize, credit the latest click within 7 days (ties go to the smallest `click_id`), count assists over merged 7-day click intervals, and aggregate both tables.
7. **Write.** Uncompressed Parquet, one file per thread.

IDs are compared through a 128-bit bijection of their bytes, so IDs of up to 15 bytes (all the generator writes) can never collide; longer ones fall back to xxh3-128. Byte order is used wherever the rules need it: person names and `click_id` ties.

## Build

The Dockerfile uses profile-guided optimization. It builds the organizers' dataset generator (vendored, unchanged, in `pgo/generator/`), generates a 2M-event training set with a seed of its own, runs an instrumented build on it, and compiles the final binary with that profile. If any of those steps fails, it falls back to a plain optimized build. All of this happens at build time; the run is the binary alone.

## Verify

```sh
cargo build --release
python scripts/run_fixtures.py fixtures -- "$PWD/target/release/sessions"     # 36/36
DATA_DIR=dev OUTPUT_DIR=out target/release/sessions && python scripts/compare.py out dev-answer
python scripts/reference.py DATA_DIR REF_OUT   # slow independent Python implementation, for other seeds
```
