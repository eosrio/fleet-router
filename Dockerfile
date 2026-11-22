# Fleet Router — multi-stage build.
# aws-lc-rs (upstream wss://) needs the image's C compiler plus CMake.
# rs_abieos stays on its pure-Rust backend; libclang is not required.
# rust:1.100-trixie
FROM rust:1.100-trixie@sha256:e19840879045c951c905d0eaa9887f3439c62000e429963371b376bac8d38c31 AS builder
WORKDIR /build
RUN apt-get update && \
    apt-get install -y --no-install-recommends cmake && \
    rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --release --locked

# debian:trixie-slim
FROM debian:trixie-slim@sha256:a72b4433de1cf7d6bae0c4cee74d0547f58601a643b3375f070c51600d63b6b2
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
