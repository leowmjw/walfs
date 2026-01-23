# AGENTS.md - Design Decisions and Architecture Notes

This document captures the design decisions, architecture choices, and lessons learned during the development of WALFS, particularly the Tigris integration for global consistency.

## Project Overview

WALFS is a high-performance Write-Ahead Log implementation in Go using memory-mapped I/O. The Tigris integration adds global distribution capabilities while maintaining the local performance characteristics.

## Upstream Sync Summary (January 2025)

The upstream sync from unisondb (commit `4308a7c`) brought significant enhancements focused on **Raft consensus support** and **improved durability**. Our remote store features are **fully orthogonal** to these changes.

### New Features from Upstream

| Feature | Purpose | Our Integration |
|---------|---------|-----------------|
| `Write(data, logIndex)` signature | Raft log indexing | Tests use `Write(data, 0)` when index not needed |
| `ShardedIndex` | Concurrent Raft index → position lookup | Independent - we don't use Raft mode |
| `WithReaderCommitCheck()` | Raft FSM.Apply() boundary control | Independent - for Raft coordination |
| `Truncate(logIndex)` | WAL truncation for Raft | Independent - works alongside remote store |
| `DirectorySyncer` | Configurable directory fsync | Compatible - enhances durability |
| `WithCustomMarker()` | Mode validation (Raft vs standalone) | Independent - prevents mode mixing |
| Segment index files (`.idx`) | Fast record lookup | Compatible - works with remote segments |

### Why No Conflicts

Our remote store features integrate cleanly because:

1. **Upload happens after seal** - `rotateSegment()` calls `SealSegment()` then `uploadSegment()`
2. **Download happens before recovery** - `recoverSegments()` downloads first, then scans local files
3. **Idle rotation uses existing rotation** - `resetIdleTimer()` triggers standard `rotateSegment()`

## Key Design Decisions

### 1. Sealed Segment Upload Pattern

**Decision**: Upload only sealed (immutable) segments to Tigris, not the active segment.

**Rationale**:
- S3/Tigris objects are immutable - you can't do partial writes
- Sealed segments are read-only, matching S3 object semantics perfectly
- Active segment continues using fast local mmap writes
- Upload happens atomically on segment rotation

**Evidence**: The S3 API only supports PutObject (whole object) and multipart uploads. There's no equivalent to mmap's byte-level random access. See `tigris_store.go:Upload()`.

### 2. Strong Consistency via Synchronous Upload

**Decision**: `rotateSegment()` blocks until upload completes before returning.

**Rationale**:
- Guarantees that when a write returns, the data is globally durable
- Simpler failure model - if upload fails, rotation fails, caller can retry
- Tigris provides strong read-after-write consistency within regions

**Trade-off**: Higher latency on rotation (network round-trip). Acceptable because:
- Rotation is infrequent (every 16MB by default)
- Data integrity > latency for WAL use cases

**Code path**: `walog.go:rotateSegment()` - upload happens after `MarkSealedInMemory()`

### 3. Interface Abstraction for Storage Backend

**Decision**: Created `SegmentStore` interface rather than tightly coupling to Tigris.

```go
type SegmentStore interface {
    Upload(ctx context.Context, segmentID SegmentID, data io.Reader, size int64) error
    Download(ctx context.Context, segmentID SegmentID) (io.ReadCloser, int64, error)
    Exists(ctx context.Context, segmentID SegmentID) (bool, error)
    List(ctx context.Context) ([]SegmentID, error)
    Delete(ctx context.Context, segmentID SegmentID) error
}
```

**Rationale**:
- Enables testing with `MemoryStore` without network
- Allows future backends (other S3 providers, GCS, Azure Blob)
- `NoOpStore` provides backward compatibility for local-only mode

### 4. Backward Compatibility

**Decision**: Remote store features are opt-in via options pattern.

**Rationale**:
- Existing users don't need to change anything
- Without `WithRemoteStore()`, WALFS works exactly as before
- Gradual adoption path for distributed deployments

### 5. Error Handling Strategy

| Scenario | Behavior | Rationale |
|----------|----------|-----------|
| Upload failure | Rotation fails, return error | Strong consistency guarantee |
| Download failure | Log warning, continue | Graceful degradation for readers |
| List failure | Log warning, use local only | Recovery can proceed with local data |
| Context timeout | Propagate cancellation | Respect caller's timeout preferences |

**Code reference**: `walog.go:downloadSegment()` logs warnings but doesn't fail recovery.

### 6. Object Key Naming Convention

**Decision**: Use `{prefix}{segmentID:09d}.wal` format.

**Example**: `wal/production/000000001.wal`

**Rationale**:
- Matches local file naming for consistency
- Zero-padded IDs ensure lexicographic sort = chronological sort
- Prefix allows multiple WALs in same bucket
- `.wal` suffix allows filtering in bucket listings

### 7. Not Using TigrisFS (FUSE Mount)

**Decision**: Direct S3 API integration instead of mounting Tigris via TigrisFS.

**Rationale**:
- FUSE adds overhead and latency for every operation
- TigrisFS caching semantics don't align with WAL consistency needs
- Direct control over upload/download timing
- Simpler deployment (no FUSE dependencies)

**Evidence from research**: TigrisFS is optimized for AI workloads with aggressive caching and prefetching, not for WAL semantics where write ordering is critical.

### 8. Idle Segment Rotation

**Decision**: Implement optional automatic segment rotation when no writes occur for a configurable duration.

**Rationale**:
- Low-volume data producers may never fill a segment
- Without rotation, data stays in unsealed segment indefinitely
- Other regions cannot read unsealed segments
- Configurable timeout balances latency vs segment overhead

**Implementation**:
```go
walfs.WithIdleSegmentRotation(1 * time.Minute)  // Rotate after 1 min idle
```

**Code path**:
- `Write()` calls `resetIdleTimer()` after each successful write
- Timer fires `rotateSegment()` if no writes within duration
- Only rotates if segment has data (`WriteOffset() > segmentHeaderSize`)
- Timer stopped on `Close()`

**Use cases**:
- Low-volume logging systems
- Periodic metrics collection
- Event-driven architectures with bursty traffic
- Multi-region deployments requiring data synchronization

## Raft vs Object Store: Architectural Comparison

### Why Raft Exists in WALFS

The upstream Raft support enables WALFS to be used as the WAL for distributed consensus systems (like etcd). Raft provides:
- **Strong consistency** - All nodes see the same order of operations
- **Leader election** - Automatic failover when leader fails
- **Log replication** - Entries replicated to majority before commit

### Comparison Table

| Aspect | Raft Consensus | Object Store (Tigris/S3) |
|--------|---------------|--------------------------|
| **Primary Use Case** | Distributed coordination, state machine replication | Durable storage, global data distribution |
| **Consistency Model** | Strong (linearizable) | Configurable (eventual or strong per-request) |
| **Write Latency** | Higher (requires N/2+1 acks) | Lower (single write) |
| **Fault Tolerance** | Tolerates (N-1)/2 failures | Handled by provider (11 nines durability) |
| **Scaling - Nodes** | Limited (single leader bottleneck) | Unlimited (stateless readers) |
| **Scaling - Data** | Limited by leader capacity | Unlimited (petabytes+) |
| **Operational Complexity** | High (manage quorum, elections) | Low (managed service) |
| **Node Management** | Manual (add/remove carefully) | None (serverless) |
| **Recovery** | Replay WAL on each node | Download from object store |
| **Global Distribution** | Complex (multi-DC Raft is hard) | Built-in (automatic replication) |
| **Cost Model** | Dedicated servers (3-5 minimum) | Pay-per-use |
| **Multi-Writer** | Yes (via leader) | Limited (conditional writes) |

### When to Use Which

| Scenario | Recommended Approach |
|----------|---------------------|
| Distributed coordination (locks, leader election) | Raft |
| Durable log storage and distribution | Object Store |
| Global read scaling | Object Store |
| Low-latency local writes | Raft (if local cluster) |
| Disaster recovery | Object Store |
| Cost-sensitive deployments | Object Store |

### Our Implementation Advantage

The object store approach provides:
1. **Simpler operations** - No Raft cluster to manage
2. **Better durability** - 11 nines vs manual replication
3. **Global distribution** - Built into Tigris
4. **Lower cost** - Pay only for storage/requests used
5. **Faster recovery** - `WithDownloadOnRead` instantly recovers

**Key insight from Milvus architecture analysis**:
> "In a cloud-native era, local storage is replaced by shared storage solutions like EBS and S3. As a result, consensus-based replication is no longer a must for distributed systems."

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                         WALog                                │
├─────────────────────────────────────────────────────────────┤
│  Write Path (Local)          │  Upload Path (Remote)        │
│  ─────────────────           │  ──────────────────          │
│  1. Write to mmap            │  1. Seal segment             │
│  2. Update metadata          │  2. Open segment file        │
│  3. Optional msync           │  3. Upload to SegmentStore   │
│  4. Reset idle timer         │  4. Create new segment       │
├─────────────────────────────────────────────────────────────┤
│  Read Path (Local)           │  Download Path (Remote)      │
│  ─────────────────           │  ────────────────────        │
│  1. Check local segments     │  1. List remote segments     │
│  2. Read from mmap           │  2. Download missing         │
│  3. Return data slice        │  3. Write to local file      │
│                              │  4. Open as local segment    │
├─────────────────────────────────────────────────────────────┤
│  Idle Rotation Path          │  Raft Integration (Upstream) │
│  ───────────────────         │  ─────────────────────────── │
│  1. Timer fires after idle   │  1. Write(data, logIndex)    │
│  2. Check segment has data   │  2. ShardedIndex mapping     │
│  3. Call rotateSegment()     │  3. Commit() boundary        │
│  4. Upload if enabled        │  4. Truncate() support       │
└─────────────────────────────────────────────────────────────┘
```

## File Organization

```
walfs/
├── segment.go              # Core segment implementation (mmap)
├── walog.go                # WALog orchestration + remote + idle rotation
├── store.go                # SegmentStore interface + NoOpStore + MemoryStore
├── tigris_store.go         # Tigris/S3 implementation
├── sharded_index.go        # Raft log index (upstream)
├── decoder.go              # Record decoder interface
├── util.go                 # Reader tracker utility
├── doc.go                  # Package documentation
├── segment_test.go         # Segment unit tests
├── walog_test.go           # WALog unit tests
├── walog_remote_test.go    # Remote store integration tests
├── walog_internal_test.go  # Internal tests (upstream)
├── wal_corruption_test.go  # Corruption handling tests (upstream)
├── store_test.go           # Store interface tests
├── sharded_index_test.go   # Index tests (upstream)
├── segment_benchmark_test.go # Performance benchmarks
└── examples/
    ├── basic/main.go       # Basic Tigris integration example
    └── idle-rotate/main.go # Idle rotation example
```

## Testing Strategy

### Unit Tests with MemoryStore

All remote store tests use `MemoryStore` to avoid network dependencies:

```go
store := walfs.NewMemoryStore()
wal, _ := walfs.NewWALog(dir, ".wal",
    walfs.WithRemoteStore(store),
    walfs.WithUploadOnSeal(true),
)
```

### Error Injection

`MemoryStore` supports error injection for testing failure paths:

```go
store.UploadError = errors.New("simulated failure")
// Now uploads will fail
```

### Edge Cases Covered

1. **Upload failure mid-rotation** - Rotation fails, segment stays sealed locally
2. **Download incomplete segment** - File cleanup on partial download
3. **Concurrent readers downloading** - Thread-safe store operations
4. **Mixed local/remote segments** - Recovery merges both sources
5. **Empty remote store** - Creates initial segment locally
6. **Large segments** - Tested with 1MB segments
7. **Idle rotation trigger** - Timer fires and rotates correctly
8. **Idle rotation with no data** - Skips rotation if segment empty

### Write Signature Note

The upstream changed `Write(data)` to `Write(data, logIndex)`. For non-Raft use cases:
```go
// Use 0 for logIndex when Raft indexing not needed
pos, err := wal.Write([]byte("data"), 0)
```

## Configuration Reference

```go
// Writer configuration (single region)
walfs.NewWALog(dir, ext,
    walfs.WithMaxSegmentSize(16*1024*1024),  // 16MB segments
    walfs.WithRemoteStore(tigrisStore),       // Enable Tigris
    walfs.WithUploadOnSeal(true),             // Upload on rotation
    walfs.WithUploadTimeout(5*time.Minute),   // Upload timeout
    walfs.WithBytesPerSync(64*1024),          // Sync every 64KB
    walfs.WithIdleSegmentRotation(time.Minute), // Rotate after 1 min idle
)

// Reader configuration (any region)
walfs.NewWALog(dir, ext,
    walfs.WithRemoteStore(tigrisStore),
    walfs.WithDownloadOnRead(true),  // Download missing segments
    walfs.WithUploadOnSeal(false),   // Don't upload (read-only)
)

// Raft mode (upstream feature)
walfs.NewWALog(dir, ext,
    walfs.WithReaderCommitCheck(),   // Enable commit boundary
    walfs.WithCustomMarker(0x52414654), // "RAFT" marker
    walfs.WithCustomMarkerValidator(func(m uint32) error {
        if m != 0x52414654 { return errors.New("not a Raft WAL") }
        return nil
    }),
)
```

## Tigris-Specific Notes

### Endpoint Configuration

```go
walfs.TigrisConfig{
    Endpoint: "https://fly.storage.tigris.dev",  // Fly.io internal
    // OR
    Endpoint: "https://t3.storage.dev",          // External access
    Region:   "auto",                            // Always "auto" for Tigris
}
```

### Consistency Model

Tigris provides:
- Strong read-after-write consistency within regions
- Optional global strong consistency (bucket-level or request-level)
- Automatic data placement based on access patterns

For WAL use case with single writer, regional consistency is sufficient because:
- Writer always uploads to same region
- Readers download entire segments (not partial reads)
- Segment immutability after seal prevents conflicts

### Global Strong Consistency (When Needed)

```go
// Bucket-level: all operations strongly consistent
// Set via Tigris console or API

// Request-level: use X-Tigris-Consistent header
// Our implementation uses default regional consistency
```

**Trade-off**: Global strong consistency routes all operations through a single leader, increasing latency for distant users.

## Future Considerations

### Potential Improvements

1. **Multipart upload for large segments** - Currently buffers entire segment in memory
2. **Parallel download on recovery** - Download multiple segments concurrently
3. **Compression before upload** - Reduce storage costs and transfer time
4. **Encryption at rest** - Client-side encryption before upload
5. **Metrics/observability** - Upload latency, success rate, bytes transferred
6. **S3 conditional writes** - Use `If-None-Match` for safe uploads (AWS added Nov 2024)

### Not Implemented (By Design)

1. **Active segment replication** - Would require complex sync protocol
2. **Multi-writer support** - Would need distributed consensus (Raft/Paxos)
3. **Segment versioning** - Tigris doesn't support S3 versioning
4. **Incremental uploads** - S3 doesn't support append operations

## Dependencies

```
github.com/aws/aws-sdk-go-v2           # AWS SDK v2 core
github.com/aws/aws-sdk-go-v2/config    # SDK configuration
github.com/aws/aws-sdk-go-v2/credentials # Static credentials
github.com/aws/aws-sdk-go-v2/service/s3 # S3 client
github.com/edsrzf/mmap-go              # Memory-mapped files
```

## References

### Tigris & Object Storage
- [Tigris Documentation](https://www.tigrisdata.com/docs/)
- [Tigris S3 API Compatibility](https://www.tigrisdata.com/docs/api/s3/)
- [Tigris Consistency Model](https://www.tigrisdata.com/docs/concepts/consistency/)
- [Tigris Architecture](https://www.tigrisdata.com/docs/concepts/architecture/)
- [S3 Conditional Writes (Nov 2024)](https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-s3-functionality-conditional-writes/)

### Raft & Distributed Consensus
- [Raft Consensus Algorithm](https://raft.github.io/)
- [etcd Raft Deep Dive](https://medium.com/@rawan_17928/building-a-distributed-key-value-store-a-deep-dive-into-raft-consensus-and-etcd-c139753ba09a)
- [etcd-io/raft GitHub](https://github.com/etcd-io/raft)
- [Milvus: Raft or Not](https://milvus.io/blog/raft-or-not.md)

### WAL Design
- [etcd WAL Design](https://github.com/etcd-io/etcd/tree/main/server/storage/wal)
- [BoltDB Alignment Discussion](https://github.com/boltdb/bolt/issues/548)
- [AWS Go SDK v2](https://aws.github.io/aws-sdk-go-v2/docs/)
