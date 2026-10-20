# syntax=docker/dockerfile:1.7

# The usage aggregator (ADR-007). Built from the same build definition as the
# service, but only its own sources go in.
FROM eclipse-temurin:17-jdk-jammy AS builder

ARG SBT_VERSION=1.12.0
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates unzip && \
    curl -fsSL https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz | tar -xz -C /usr/local && \
    ln -s /usr/local/sbt/bin/sbt /usr/local/bin/sbt && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY project/ ./project/
COPY build.sbt ./
RUN sbt aggregator/update || true

COPY aggregator/src/main/ ./aggregator/src/main/
RUN sbt aggregator/universal:stage

FROM eclipse-temurin:17-jre-jammy AS runtime

LABEL org.opencontainers.image.title="Gate usage aggregator" \
      org.opencontainers.image.description="Folds Gate's Kinesis event stream into per-client usage in DynamoDB" \
      org.opencontainers.image.version="0.1.0" \
      org.opencontainers.image.source="https://github.com/saintparish4/gate"

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    groupadd -r appuser && useradd -r -g appuser -u 1000 appuser

WORKDIR /app
COPY --from=builder --chown=appuser:appuser /build/aggregator/target/universal/stage/ ./
USER appuser
EXPOSE 9095
ENV JAVA_OPTS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0 -XX:+ExitOnOutOfMemoryError"
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:9095/health || exit 1
ENTRYPOINT ["./bin/gate-usage-aggregator"]
