# Dockerfile for testing thermorawfilereader on Linux x86-64
FROM --platform=linux/amd64 mcr.microsoft.com/dotnet/runtime:8.0-jammy

# Install Rust and build dependencies
RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Set working directory
WORKDIR /app

# Copy the project
COPY . .

# Build and test - just run thermorawfilereader tests with parallel features
CMD ["bash", "-c", "cargo test -p thermorawfilereader --features rayon,tokio --release 2>&1"]
