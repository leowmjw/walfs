# Examples

This directory contains example programs demonstrating various WALFS features.

## Prerequisites

All examples require Go 1.25.6 or later. Install it from [go.dev](https://go.dev/dl/).

For Tigris integration examples, you'll need:
- A Tigris account (sign up at [console.tigris.dev](https://console.tigris.dev/))
- Access credentials (Access Key ID and Secret Access Key)
- A bucket created in your Tigris account

## Scenario #1: Basic Remote WAL with Tigris

**Directory:** `basic/`

Demonstrates core WAL operations with Tigris object storage integration.

### What it shows:

- **Writing records to local WAL**: Append log entries with automatic buffering
- **Segment rotation**: Automatic creation of new segments
- **Upload to Tigris**: Sealed segments uploaded to remote object storage
- **Reading from local and remote**: Data accessible from both sources
- **Graceful fallback**: Runs in local-only mode if Tigris is not configured

### Setup Instructions

#### 1. Configure Tigris Credentials

Copy the sample environment file and add your credentials:

```bash
# From the repository root
cp .env.sample .env
```

Edit `.env` and fill in your Tigris credentials:

```bash
TIGRIS_ENDPOINT=https://fly.storage.tigris.dev
TIGRIS_ACCESS_KEY_ID=tid_your_access_key_id
TIGRIS_SECRET_ACCESS_KEY=tsec_your_secret_key
TIGRIS_BUCKET_NAME=your-bucket-name
TIGRIS_PREFIX=walfs/basic/
TIGRIS_REGION=auto
```

**Getting Tigris Credentials:**
1. Sign up at [console.tigris.dev](https://console.tigris.dev/)
2. Create a new bucket (e.g., `walfs-demo`)
3. Generate access credentials from the dashboard
4. Copy the Access Key ID and Secret Access Key

#### 2. Run the Example

**Option A: Using mise (Recommended)**

```bash
# From repository root
mise run example:basic
```

**Option B: Using Makefile**

```bash
# From repository root
make example-basic
```

**Option C: Direct execution**

```bash
cd examples/basic
go run main.go
```

#### 3. Run in Local-Only Mode

To test without Tigris (no credentials required):

```bash
# Using mise
mise run example:basic:local

# Using Makefile
make example-basic-local

# Direct execution (without .env)
cd examples/basic
go run main.go
```

### Expected Output

**With Tigris configured:**

```
=== Basic WALFS with Tigris Object Store ===

Configuration:
  Endpoint: https://fly.storage.tigris.dev
  Bucket: your-bucket-name
  Prefix: walfs/basic/
  Region: auto

✓ WALog initialized with Tigris storage

--- Writing Records ---
  ✓ Record 1: segment=1 offset=64 - User login: alice@example.com
  ✓ Record 2: segment=1 offset=120 - Order created: order-12345
  ✓ Record 3: segment=1 offset=175 - Payment processed: $99.99
  ✓ Record 4: segment=1 offset=230 - Shipment dispatched: tracking-67890
  ✓ Record 5: segment=1 offset=295 - User logout: alice@example.com

--- Rotating Segment ---
  ✓ Segment rotated and uploaded to Tigris

--- Reading Records ---
  [1] [17:30:45] User login: alice@example.com
  [2] [17:30:45] Order created: order-12345
  [3] [17:30:45] Payment processed: $99.99
  [4] [17:30:45] Shipment dispatched: tracking-67890
  [5] [17:30:45] User logout: alice@example.com

--- Remote Storage Status ---
  Remote segments: [1]
  ✓ Segment 1 available in Tigris

--- Sequential Reader ---
  [1] Segment 1: [17:30:45] User login: alice@example.com
  [2] Segment 1: [17:30:45] Order created: order-12345
  [3] Segment 1: [17:30:45] Payment processed: $99.99
  [4] Segment 1: [17:30:45] Shipment dispatched: tracking-67890
  [5] Segment 1: [17:30:45] User logout: alice@example.com
  Total records read: 5

=== Success ===
✓ Records written to local WAL
✓ Sealed segment uploaded to Tigris
✓ Data readable from both local and remote storage
```

**Without Tigris (local-only mode):**

```
=== Basic WALFS with Tigris Object Store ===

⚠️  Tigris credentials not configured. Running in local-only mode.
    To enable remote storage, set the following environment variables:
    - TIGRIS_ACCESS_KEY_ID
    - TIGRIS_SECRET_ACCESS_KEY
    - TIGRIS_BUCKET_NAME

--- Running in Local-Only Mode ---

✓ WALog initialized (local only)

--- Writing Records ---
  ✓ Record 1: segment=1 offset=64
  ✓ Record 2: segment=1 offset=120
  ✓ Record 3: segment=1 offset=176

--- Reading Records ---
  [1] [17:31:20] Local record #1
  [2] [17:31:20] Local record #2
  [3] [17:31:20] Local record #3

=== Success ===
✓ Records written and read from local WAL

💡 To enable Tigris integration, configure the environment variables
```

### Key Configuration

```go
// Create Tigris store
store, err := walfs.NewTigrisStore(ctx, walfs.TigrisConfig{
    Endpoint:        "https://fly.storage.tigris.dev",
    AccessKeyID:     "tid_your_key",
    SecretAccessKey: "tsec_your_secret",
    BucketName:      "your-bucket",
    Prefix:          "walfs/basic/",
    Region:          "auto",
})

// Create WALog with Tigris integration
wal, err := walfs.NewWALog(dir, ".wal",
    walfs.WithMaxSegmentSize(512*1024),  // 512KB segments
    walfs.WithRemoteStore(store),         // Enable Tigris
    walfs.WithUploadOnSeal(true),         // Auto-upload on rotation
)
```

### Troubleshooting

**Error: "failed to create Tigris store"**
- Verify your credentials are correct
- Ensure the bucket exists in your Tigris account
- Check that the endpoint URL is correct for your region

**Error: "failed to upload segment"**
- Verify network connectivity to Tigris
- Check bucket permissions
- Ensure sufficient bucket quota

**Records not appearing in Tigris:**
- Segments are only uploaded when sealed (rotated)
- Call `wal.RotateSegment()` to force rotation
- Or write enough data to fill the segment (512KB in this example)

## Scenario #3: Idle Segment Rotation

**Directory:** `idle-rotate/`

Demonstrates the idle segment rotation feature for low-volume data producers.

### What it shows:

- **Automatic rotation on idle timeout**: Segments rotate automatically after a period of inactivity
- **Timer reset on writes**: Active writes prevent rotation by resetting the timer
- **Remote storage integration**: Rotated segments are automatically uploaded to remote storage
- **Multi-node deployment**: Shows how other nodes can access data via download-on-read
- **Configurable timeout**: Balance between data freshness and segment overhead

### Use Cases:

- Low-volume logging systems
- Periodic metrics collection
- Event-driven architectures with bursty traffic
- Multi-region deployments requiring data synchronization

### Key Configuration:

```go
wal, err := walfs.NewWALog(dir, ".wal",
    walfs.WithIdleSegmentRotation(1 * time.Minute),  // Rotate after 1 minute of inactivity
    walfs.WithRemoteStore(store),                     // Configure remote storage
    walfs.WithUploadOnSeal(true),                     // Upload on rotation
)
```

### How to Run:

```bash
cd examples/idle-rotate
go run main.go
```

### Expected Output:

The example demonstrates 4 scenarios:

1. **Regular writes prevent rotation** - Shows that the timer is reset on each write
2. **Idle period triggers rotation** - After inactivity, segment automatically rotates
3. **Reading rotated segments** - All data remains accessible after rotation
4. **Multi-node deployment** - Second node downloads and reads data from remote store

The example uses a 3-second idle timeout for demonstration purposes. In production, you would typically use 1-5 minutes depending on your data freshness requirements.

### Production Considerations:

- Set `idleRotationDuration` based on your data freshness SLA (e.g., 1-5 minutes)
- For very low-volume producers, consider using shorter timeouts (30-60 seconds)
- For periodic data (e.g., hourly metrics), align timeout with your data cadence
- Monitor segment rotation count to ensure the timeout is appropriate
- Use with `WithUploadOnSeal(true)` to ensure data is available globally
