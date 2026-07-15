# syntax=docker/dockerfile:1.7

FROM rust:1.90-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY .cargo ./.cargo
COPY benches ./benches
COPY config ./config
COPY src ./src

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --locked --release \
    && cp /app/target/release/arb_bot /usr/local/bin/arb_bot

FROM debian:bookworm-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/bin/arb_bot /usr/local/bin/arb_bot
COPY config ./config

USER 65532:65532
ENTRYPOINT ["arb_bot"]
CMD ["run"]
