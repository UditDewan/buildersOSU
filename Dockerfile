# Shopping Sessions: a single Rust binary. Build is untimed; the run is the binary alone.
FROM rust:1.87-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# x86-64-v3 (AVX2) runs on any Cascade Lake scorer; memchr picks wider SIMD at run time.
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
RUN cargo build --release --locked

FROM debian:bookworm-slim
COPY --from=build /src/target/release/sessions /usr/local/bin/sessions
USER 1000:1000
CMD ["/usr/local/bin/sessions"]
