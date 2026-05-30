# Fleet Router — multi-stage build. Pure-Rust: no C/C++ toolchain required.
FROM rust:1.95-bookworm AS builder
WORKDIR /build
COPY . .
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin fleet
COPY --from=builder /build/target/release/fleet-router /usr/local/bin/fleet-router
USER fleet
EXPOSE 17000
# Liveness probe: confirm the proxy port is accepting TCP connections.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD bash -c 'exec 3<>/dev/tcp/127.0.0.1/17000' || exit 1
ENTRYPOINT ["fleet-router"]
