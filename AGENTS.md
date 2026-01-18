# AGENTS.md - Design Decisions and Architecture Notes

This document captures the design decisions, architecture choices, and lessons learned during the development of WALFS, particularly the Tigris integration for global consistency.

## Project Overview

WALFS is a high-performance Write-Ahead Log implementation in Go using memory-mapped I/O. The Tigris integration adds global distribution capabilities while maintaining the local performance characteristics.

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

**Code path**: `walog.go:rotateSegment()` lines 540-552

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
│                              │  4. Create new segment       │
├─────────────────────────────────────────────────────────────┤
│  Read Path (Local)           │  Download Path (Remote)      │
│  ─────────────────           │  ────────────────────        │
│  1. Check local segments     │  1. List remote segments     │
│  2. Read from mmap           │  2. Download missing         │
│  3. Return data slice        │  3. Write to local file      │
│                              │  4. Open as local segment    │
└─────────────────────────────────────────────────────────────┘
```

## File Organization

```
walfs/
├── segment.go          # Core segment implementation (mmap)
├── walog.go            # WALog orchestration + remote integration
├── store.go            # SegmentStore interface + NoOpStore + MemoryStore
├── tigris_store.go     # Tigris/S3 implementation
├── util.go             # Reader tracker utility
├── doc.go              # Package documentation
├── segment_test.go     # Segment unit tests
├── walog_test.go       # WALog unit tests
├── walog_remote_test.go # Remote store integration tests
├── store_test.go       # Store interface tests
└── segment_benchmark_test.go # Performance benchmarks
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

## Configuration Reference

```go
// Writer configuration (single region)
walfs.NewWALog(dir, ext,
    walfs.WithMaxSegmentSize(16*1024*1024),  // 16MB segments
    walfs.WithRemoteStore(tigrisStore),       // Enable Tigris
    walfs.WithUploadOnSeal(true),             // Upload on rotation
    walfs.WithUploadTimeout(5*time.Minute),   // Upload timeout
    walfs.WithBytesPerSync(64*1024),          // Sync every 64KB
)

// Reader configuration (any region)
walfs.NewWALog(dir, ext,
    walfs.WithRemoteStore(tigrisStore),
    walfs.WithDownloadOnRead(true),  // Download missing segments
    walfs.WithUploadOnSeal(false),   // Don't upload (read-only)
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
- Optional global strong consistency (bucket-level setting)
- Automatic data placement based on access patterns

For WAL use case with single writer, regional consistency is sufficient because:
- Writer always uploads to same region
- Readers download entire segments (not partial reads)
- Segment immutability after seal prevents conflicts

## Future Considerations

### Potential Improvements

1. **Multipart upload for large segments** - Currently buffers entire segment in memory
2. **Parallel download on recovery** - Download multiple segments concurrently
3. **Compression before upload** - Reduce storage costs and transfer time
4. **Encryption at rest** - Client-side encryption before upload
5. **Metrics/observability** - Upload latency, success rate, bytes transferred

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

- [Tigris Documentation](https://www.tigrisdata.com/docs/)
- [Tigris S3 API Compatibility](https://www.tigrisdata.com/docs/api/s3/)
- [AWS Go SDK v2](https://aws.github.io/aws-sdk-go-v2/docs/)
- [etcd WAL Design](https://github.com/etcd-io/etcd/tree/main/server/storage/wal)
- [BoltDB Alignment Discussion](https://github.com/boltdb/bolt/issues/548)
