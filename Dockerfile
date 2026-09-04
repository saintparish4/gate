# syntax=docker/dockerfile:1.7

# =============================================================================
# Build Stage
# =============================================================================
FROM eclipse-temurin:17-jdk-jammy AS builder

# Install SBT
ARG SBT_VERSION=1.10.3
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    unzip && \
    curl -fsSL https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz | tar -xz -C /usr/local && \
    ln -s /usr/local/sbt/bin/sbt /usr/local/bin/sbt && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy build configuration files (for better layer caching)
COPY project/ ./project/
COPY build.sbt ./

# Download dependencies (cached layer if build.sbt doesn't change)
# This layer will be cached unless dependencies change
RUN sbt update || true

# Copy source code
COPY src/ ./src/

# Build the application using sbt-native-packager
# Creates universal distribution in target/universal/stage
RUN sbt universal:stage

# =============================================================================
# Runtime Stage
# =============================================================================
FROM eclipse-temurin:17-jre-jammy AS runtime

# Add metadata labels (2025 standard: OCI labels)
LABEL org.opencontainers.image.title="Keyra" \
      org.opencontainers.image.description="Distributed rate limiting, token quotas, and idempotency for AI gateways" \
      org.opencontainers.image.version="0.1.0-SNAPSHOT" \
      org.opencontainers.image.authors="Sharif Parish" \
      org.opencontainers.image.source="https://github.com/saintparish4/keyra" \
      maintainer="Sharif Parish"

# Install curl for health checks
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create non-root user for security (2025 best practice)
RUN groupadd -r appuser && \
    useradd -r -g appuser -u 1000 appuser && \
    mkdir -p /app/lib /app/bin /app/conf /tmp && \
    chown -R appuser:appuser /app /tmp

WORKDIR /app

# Copy the built application from builder stage
# sbt-native-packager creates: bin/, lib/, and optionally conf/ directories
COPY --from=builder --chown=appuser:appuser /build/target/universal/stage/bin/ ./bin/
COPY --from=builder --chown=appuser:appuser /build/target/universal/stage/lib/ ./lib/

# Conditionally copy conf directory if it exists (BuildKit mount feature)
# This runs as root before switching to appuser
RUN --mount=from=builder,source=/build/target/universal/stage,target=/tmp/stage,ro \
    if [ -d /tmp/stage/conf ] && [ "$(ls -A /tmp/stage/conf)" ]; then \
        cp -r /tmp/stage/conf/* ./conf/ && \
        chown -R appuser:appuser ./conf/; \
    fi

# Make startup scripts executable
RUN chmod +x ./bin/* || true

# Switch to non-root user
USER appuser

# Expose the application port
EXPOSE 8080

# Set JVM options for containerized environments (2025 best practice)
ENV JAVA_OPTS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0 -XX:+ExitOnOutOfMemoryError -Djava.security.egd=file:/dev/./urandom"

# Health check (2025 standard: use HEALTHCHECK instruction)
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Use the startup script from sbt-native-packager
# The script name matches the project name from build.sbt
# It handles classpath and JVM options properly
ENTRYPOINT ["./bin/keyra"]
