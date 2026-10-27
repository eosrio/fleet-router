# Fleet Router — multi-stage build.
# aws-lc-rs (upstream wss://) needs the image's C compiler plus CMake.
# rs_abieos stays on its pure-Rust backend; libclang is not required.
# rust:1.99-trixie
FROM rust:1.99-trixie@sha256:e259494b14672950a909b4c54527e19d29cae6ebf980672114c8bb85e5594e4a AS builder
WORKDIR /build
RUN apt-get update && \
    apt-get install -y --no-install-recommends cmake && \
    rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --release --locked

# debian:trixie-slim
FROM debian:trixie-slim@sha256:62bd423aeee912639ca8ee681c2b762cb3e25137e0a92b20be455898a9f034ad
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin fleet
COPY --from=builder /build/target/release/fleet-router /usr/local/bin/fleet-router
USER fleet
EXPOSE 17000
# Liveness probe: confirm the proxy port is accepting TCP connections.
# `fleet-router check` dials 127.0.0.1:17000, so the image does not need a shell.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["fleet-router", "check"]
ENTRYPOINT ["fleet-router"]
