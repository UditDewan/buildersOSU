# Shopping Sessions: a single Rust binary. Build is untimed; the run is the binary alone.
FROM rust:1.87-bookworm AS build
WORKDIR /src
# Profile-guided optimization: train on a dataset from the organizers' generator (build time only).
COPY pgo ./pgo
RUN cd pgo/generator && cargo build --release -q \
 && ./target/release/generate --seed 99 --events 2000000 --clicks 500000 --output /train
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# x86-64-v3 (AVX2) runs on any Cascade Lake scorer; memchr picks wider SIMD at run time.
ENV FLAGS="-C target-cpu=x86-64-v3"
# Any PGO step failing falls back to a plain optimized build.
RUN ( rustup component add llvm-tools-preview \
   && RUSTFLAGS="$FLAGS -Cprofile-generate=/tmp/pgo" cargo build --release --locked --target-dir /tmp/tgen \
   && DATA_DIR=/train OUTPUT_DIR=/tmp/out NCPU=4 /tmp/tgen/release/sessions \
   && "$(find / -name llvm-profdata -type f 2>/dev/null | head -1)" merge -o /tmp/pgo.profdata /tmp/pgo \
   && RUSTFLAGS="$FLAGS -Cprofile-use=/tmp/pgo.profdata" cargo build --release --locked \
   ) || RUSTFLAGS="$FLAGS" cargo build --release --locked

FROM debian:bookworm-slim
COPY --from=build /src/target/release/sessions /usr/local/bin/sessions
USER 1000:1000
CMD ["/usr/local/bin/sessions"]
