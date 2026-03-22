# Build stage
FROM rust:1.94-bookworm AS builder
WORKDIR /app

# Copy manifest files first for better layer caching
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY examples ./examples

# Build release binary
RUN cargo build --release -p potatodb

# Runtime stage
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/potatodb /usr/local/bin/potatodb

ENTRYPOINT ["potatodb"]
# CMD ["--repl"]