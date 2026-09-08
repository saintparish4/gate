#!/bin/bash
set -euo pipefail

echo "============================================"
echo "Initializing LocalStack resources..."
echo "============================================"

# Wait for LocalStack to be ready with retry logic
wait_for_localstack() {
    local max_attempts=30
    local attempt=1

    echo "Waiting for LocalStack services to be ready..."
    while [ $attempt -le $max_attempts ]; do
        if curl -sf http://localhost:4566/_localstack/health >/dev/null 2>&1; then
            echo "LocalStack is ready!"
            return 0
        fi
        echo "Attempt $attempt/$max_attempts: LocalStack not ready yet, waiting..."
        sleep 2
        attempt=$((attempt + 1))
    done

    echo "ERROR: LocalStack did not become ready after $max_attempts attempts"
    return 1
}

# Wait for specific service to be available
wait_for_service() {
    local service=$1
    local max_attempts=20
    local attempt=1

    while [ $attempt -le $max_attempts ]; do
        if awslocal "$service" list-tables >/dev/null 2>&1 || awslocal "$service" list-streams >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
        attempt=$((attempt + 1))
    done

    echo "WARNING: Service $service may not be fully ready"
    return 0
}

# Wait for LocalStack to be ready
wait_for_localstack

# Function to check if DynamoDB table exists
table_exists() {
    awslocal dynamodb describe-table --table-name "$1" >/dev/null 2>&1 || return 1
}

# Function to check if Kinesis stream exists
stream_exists() {
    awslocal kinesis describe-stream --stream-name "$1" >/dev/null 2>&1 || return 1
}

# Wait for DynamoDB service
wait_for_service "dynamodb"

# Create DynamoDB table: rate-limits
if table_exists "rate-limits"; then
    echo "DynamoDB table 'rate-limits' already exists, skipping creation"
else
    echo "Creating DynamoDB table: rate-limits"
    if awslocal dynamodb create-table \
      --table-name rate-limits \
      --attribute-definitions AttributeName=pk,AttributeType=S \
      --key-schema AttributeName=pk,KeyType=HASH \
      --billing-mode PAY_PER_REQUEST \
      --tags Key=Environment,Value=local; then
        echo "Waiting for table 'rate-limits' to become active..."
        awslocal dynamodb wait table-exists --table-name rate-limits || true
    else
        echo "WARNING: Failed to create table 'rate-limits', it may already exist"
    fi
fi

# Enable TTL on rate-limits table
echo "Enabling TTL on rate-limits table"
awslocal dynamodb update-time-to-live \
  --table-name rate-limits \
  --time-to-live-specification Enabled=true,AttributeName=ttl 2>/dev/null || echo "Note: TTL update skipped (may already be enabled or not supported)"

# Create DynamoDB table: idempotency
if table_exists "idempotency"; then
    echo "DynamoDB table 'idempotency' already exists, skipping creation"
else
    echo "Creating DynamoDB table: idempotency"
    if awslocal dynamodb create-table \
      --table-name idempotency \
      --attribute-definitions AttributeName=pk,AttributeType=S \
      --key-schema AttributeName=pk,KeyType=HASH \
      --billing-mode PAY_PER_REQUEST \
      --tags Key=Environment,Value=local; then
        echo "Waiting for table 'idempotency' to become active..."
        awslocal dynamodb wait table-exists --table-name idempotency || true
    else
        echo "WARNING: Failed to create table 'idempotency', it may already exist"
    fi
fi

# Enable TTL on idempotency table
echo "Enabling TTL on idempotency table"
awslocal dynamodb update-time-to-live \
  --table-name idempotency \
  --time-to-live-specification Enabled=true,AttributeName=ttl 2>/dev/null || echo "Note: TTL update skipped (may already be enabled or not supported)"

# Create DynamoDB table: gate-token-quotas
if table_exists "gate-token-quotas"; then
    echo "DynamoDB table 'gate-token-quotas' already exists, skipping creation"
else
    echo "Creating DynamoDB table: gate-token-quotas"
    if awslocal dynamodb create-table \
      --table-name gate-token-quotas \
      --attribute-definitions AttributeName=pk,AttributeType=S \
      --key-schema AttributeName=pk,KeyType=HASH \
      --billing-mode PAY_PER_REQUEST \
      --tags Key=Environment,Value=local; then
        echo "Waiting for table 'gate-token-quotas' to become active..."
        awslocal dynamodb wait table-exists --table-name gate-token-quotas || true
    else
        echo "WARNING: Failed to create table 'gate-token-quotas', it may already exist"
    fi
fi

# Enable TTL on gate-token-quotas table
echo "Enabling TTL on gate-token-quotas table"
awslocal dynamodb update-time-to-live \
  --table-name gate-token-quotas \
  --time-to-live-specification Enabled=true,AttributeName=ttl 2>/dev/null || echo "Note: TTL update skipped (may already be enabled or not supported)"

# Wait for Kinesis service
wait_for_service "kinesis"

# Create Kinesis stream
if stream_exists "rate-limit-events"; then
    echo "Kinesis stream 'rate-limit-events' already exists, skipping creation"
else
    echo "Creating Kinesis stream: rate-limit-events"
    if awslocal kinesis create-stream \
      --stream-name rate-limit-events \
      --shard-count 1; then
        echo "Waiting for Kinesis stream to become active..."
        awslocal kinesis wait stream-exists --stream-name rate-limit-events || true
    else
        echo "WARNING: Failed to create stream 'rate-limit-events', it may already exist"
    fi
fi

echo "============================================"
echo "LocalStack initialization complete!"
echo "============================================"

# List created resources
echo ""
echo "DynamoDB Tables:"
awslocal dynamodb list-tables 2>/dev/null || echo "Could not list DynamoDB tables"

echo ""
echo "Kinesis Streams:"
awslocal kinesis list-streams 2>/dev/null || echo "Could not list Kinesis streams"
