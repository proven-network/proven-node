# LocalStack Integration Tests

This directory contains integration tests for the S3-backed cold storage tier using LocalStack.

## Prerequisites

1. Docker installed and running
2. `docker-compose` installed
3. Port 4566 available (LocalStack S3 endpoint)

## Running the Tests

### Option 1: Using the test script (recommended)

```bash
./scripts/test-localstack.sh
```

This script will:

- Start LocalStack using docker-compose
- Wait for it to be ready
- Run all LocalStack integration tests
- Clean up containers when done

### Option 2: Manual setup

1. Start LocalStack:

   ```bash
   docker-compose -f docker-compose.localstack.yml up -d
   ```

2. Wait for LocalStack to be ready:

   ```bash
   curl http://localhost:4566/_localstack/health
   ```

3. Set environment variables:

   ```bash
   export AWS_ACCESS_KEY_ID=test
   export AWS_SECRET_ACCESS_KEY=test
   export AWS_ENDPOINT_URL=http://localhost:4566
   export AWS_EC2_METADATA_DISABLED=true
   export AWS_DEFAULT_REGION=us-east-1
   ```

4. Run the tests:

   ```bash
   cargo test --test localstack_integration --features testing -- --ignored --nocapture
   ```

5. Clean up:
   ```bash
   docker-compose -f docker-compose.localstack.yml down -v
   ```

## Test Coverage

The LocalStack integration tests cover:

1. **Cold Tier Storage** (`test_cold_tier_storage`)
   - Storing blobs directly to S3
   - Retrieving blobs from S3
   - Deleting blobs from S3

2. **Multipart Upload** (`test_multipart_upload`)
   - Large blob uploads (>5MB)
   - Verifies multipart upload functionality

3. **Cold Tier Caching** (`test_cold_tier_caching`)
   - Retrieval from S3 with hot tier caching
   - Performance comparison of cached vs uncached access

4. **S3 Failure Recovery** (`test_s3_failure_recovery`)
   - Handling S3 connection failures
   - Error recovery and fallback behavior

5. **Concurrent Operations** (`test_concurrent_migrations`)
   - Multiple simultaneous S3 operations
   - Verifies data integrity under load

6. **Encryption with S3** (`test_encryption_with_s3`)
   - Verifies blobs are encrypted before storing in S3
   - Tests decryption with correct/incorrect keys
   - Confirms data confidentiality in cloud storage

## Adding New Tests

To add new LocalStack integration tests:

1. Add your test function to `localstack_integration.rs`
2. Use the `#[ignore]` attribute so it only runs when explicitly requested
3. Follow the pattern of existing tests for LocalStack setup
4. Remember to set `AWS_ENDPOINT_URL_S3` environment variable

## Troubleshooting

### LocalStack won't start

- Check if port 4566 is already in use: `lsof -i :4566`
- Check Docker logs: `docker-compose -f docker-compose.localstack.yml logs`

### Tests fail with connection errors

- Ensure LocalStack is fully started (check health endpoint)
- Verify environment variables are set correctly
- Check if you're behind a corporate proxy

### S3 bucket errors

- LocalStack automatically creates buckets on first use
- If you get bucket errors, the test might be trying to create a bucket that already exists

## Performance Notes

- LocalStack S3 is slower than real S3 for some operations
- Tests may take longer than production code
- Consider adjusting timeouts for CI/CD environments
