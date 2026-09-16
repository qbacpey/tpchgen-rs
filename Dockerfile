# syntax=docker/dockerfile:1
# Build stage for Rust binary
FROM rust:1.97 AS builder

# TARGETARCH is automatically set by BuildKit (e.g., amd64, arm64)
ARG TARGETARCH

WORKDIR /src
COPY . .

# Build release binary with BuildKit cache mounts
# Registry and git caches are shared (source code is arch-independent)
# Target cache is per-architecture to avoid mixing compiled artifacts
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,id=target-${TARGETARCH},target=/src/target \
    cargo build --profile release && \
    cp target/release/tpchgen-cli /usr/local/bin/tpchgen-cli

# Python dependencies stage - install pyarrow at build time
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS python-deps

WORKDIR /app
COPY scripts/inspect_tpch_parquet.py .

# Create a venv and install dependencies from inline script metadata
RUN uv venv /opt/venv && \
    uv pip install --python /opt/venv/bin/python pyarrow

# Runtime stage - use Python slim image (venv symlinks to /usr/local/bin/python)
FROM python:3.12-slim-bookworm

LABEL org.opencontainers.image.title="tpchgen" \
      org.opencontainers.image.description="TPC-H data generator with Parquet output" \
      org.opencontainers.image.url="https://github.com/TomAugspurger/tpchgen-rs" \
      org.opencontainers.image.source="https://github.com/TomAugspurger/tpchgen-rs" \
      org.opencontainers.image.licenses="Apache-2.0"

RUN printf '%s\n' 'APT::Update::Error-Mode "any";' > /etc/apt/apt.conf.d/warnings-as-errors && \
    printf '%s\n' 'APT::Acquire::Retries "10";' > /etc/apt/apt.conf.d/retries && \
    printf '%s\n' 'APT::Acquire::https::Timeout "240";' > /etc/apt/apt.conf.d/https-timeout && \
    printf '%s\n' 'APT::Acquire::http::Timeout "240";' > /etc/apt/apt.conf.d/http-timeout

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    parallel \
    && rm -rf /var/lib/apt/lists/*

# Copy Python venv with pyarrow pre-installed
COPY --from=python-deps /opt/venv /opt/venv

# Set up environment so uv run uses the pre-installed venv
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy uv for running Python scripts
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy the binary
COPY --from=builder /usr/local/bin/tpchgen-cli /usr/local/bin/tpchgen-cli

# Copy the data generation script
COPY scripts/generate_tpch.py /usr/local/bin/generate_tpch.py
COPY scripts/inspect_tpch_parquet.py /usr/local/bin/inspect_tpch_parquet.py
RUN chmod +x /usr/local/bin/generate_tpch.py
RUN chmod +x /usr/local/bin/inspect_tpch_parquet.py

ENTRYPOINT ["/usr/local/bin/generate_tpch.py"]
