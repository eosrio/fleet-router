# Fleet Router — build from project root
FROM rust:1.82-bookworm AS builder
RUN apt-get update && \
    apt-get install -y --no-install-recommends clang libclang-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/fleet-router /usr/local/bin/fleet-router
EXPOSE 9000
ENTRYPOINT ["fleet-router"]
