# Shopping Sessions: a single static binary, built from source.
# No FROM line pins a platform: the scorer builds this natively on linux/amd64,
# and `make build PLATFORM=linux/arm64` builds it natively on an Apple-silicon Mac.

FROM rust:1.100-alpine3.22 AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# Not built into the image; the manifest names it, so Cargo wants to see it.
COPY ref ./ref
# The crate has no dependencies, so the build needs nothing from crates.io.
# The scorers are Cascade Lake or newer: x86-64-v3 (AVX2, BMI2) is always there.
#
# Profile-guided optimization (BENCH.md, experiment 52): an instrumented build
# runs once on a stand-in dataset from src/bin/synth.rs, and the shipped binary
# is compiled with what it counted. Only how the code is laid out and which
# paths the compiler favours change; what it computes cannot. `llvm-profdata`
# comes from rustup's llvm-tools, which needs the network at build time.
RUN set -eu; \
    FLAGS=""; \
    case "$(uname -m)" in x86_64) FLAGS="-C target-cpu=x86-64-v3" ;; esac; \
    rustup component add llvm-tools 2>/dev/null || rustup component add llvm-tools-preview; \
    PROFDATA="$(find "$(rustc --print sysroot)" -name llvm-profdata -type f | head -n 1)"; \
    test -x "$PROFDATA"; \
    cargo build --release --locked --offline --bin synth; \
    target/release/synth --events 2000000 --clicks 500000 --seed 99 --output /tmp/train; \
    RUSTFLAGS="$FLAGS -Cprofile-generate=/tmp/pgo" cargo build --release --locked --offline --bin pipeline --target-dir /tmp/instrumented; \
    NCPU=4 /tmp/instrumented/release/pipeline /tmp/train /tmp/train-out; \
    "$PROFDATA" merge -o /tmp/pgo.profdata /tmp/pgo; \
    RUSTFLAGS="$FLAGS -Cprofile-use=/tmp/pgo.profdata" cargo build --release --locked --offline --bin pipeline; \
    rm -rf /tmp/train /tmp/train-out /tmp/instrumented /tmp/pgo; \
    mkdir -p /rootfs/app /rootfs/data /rootfs/output /rootfs/scratch; \
    cp target/release/pipeline /rootfs/app/pipeline

# At run time the root filesystem is read-only, the user is 1000:1000 and there
# is no network. The binary is static and writes only to $OUTPUT_DIR.
#
# The runtime image is as small as an image with a shell can be (2 MB): if a
# sandbox is unpacked from the image for every run, its size is paid on every
# run. BusyBox rather than scratch so that a harness that runs `sh` or `chmod`
# inside the image still can; the pipeline itself needs neither, and creates
# its output readable, writable and removable by anyone.
FROM busybox:1.37-musl
# One layer: the binary, and the mount points should the sandbox not create them.
COPY --from=build /rootfs/ /
WORKDIR /app
CMD ["/app/pipeline"]
