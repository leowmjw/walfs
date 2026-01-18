package walfs

import (
	"bytes"
	"context"
	"io"
	"sort"
	"sync"
)

// SegmentStore defines the interface for storing and retrieving sealed WAL segments.
// Implementations must be safe for concurrent use.
type SegmentStore interface {
	// Upload uploads a sealed segment to the store.
	// The upload MUST be synchronous - when this returns nil, the segment
	// is durably stored and visible to all readers globally.
	// The data is read from the provided io.Reader.
	Upload(ctx context.Context, segmentID SegmentID, data io.Reader, size int64) error

	// Download retrieves a segment from the store.
	// Returns ErrSegmentNotFound if the segment does not exist.
	// The caller is responsible for closing the returned ReadCloser.
	Download(ctx context.Context, segmentID SegmentID) (io.ReadCloser, int64, error)

	// Exists checks if a segment exists in the store.
	Exists(ctx context.Context, segmentID SegmentID) (bool, error)

	// List returns all segment IDs available in the store.
	// Results are returned in ascending order.
	List(ctx context.Context) ([]SegmentID, error)

	// Delete removes a segment from the store.
	// Returns nil if segment does not exist (idempotent).
	Delete(ctx context.Context, segmentID SegmentID) error
}

// NoOpStore is a null implementation that does nothing.
// Used for local-only mode (backward compatibility).
type NoOpStore struct{}

// Upload is a no-op that always succeeds.
func (n *NoOpStore) Upload(ctx context.Context, segmentID SegmentID, data io.Reader, size int64) error {
	return nil
}

// Download always returns ErrSegmentNotFound.
func (n *NoOpStore) Download(ctx context.Context, segmentID SegmentID) (io.ReadCloser, int64, error) {
	return nil, 0, ErrSegmentNotFound
}

// Exists always returns false.
func (n *NoOpStore) Exists(ctx context.Context, segmentID SegmentID) (bool, error) {
	return false, nil
}

// List always returns an empty slice.
func (n *NoOpStore) List(ctx context.Context) ([]SegmentID, error) {
	return nil, nil
}

// Delete is a no-op that always succeeds.
func (n *NoOpStore) Delete(ctx context.Context, segmentID SegmentID) error {
	return nil
}

// MemoryStore is an in-memory implementation for testing.
// It is safe for concurrent use.
type MemoryStore struct {
	mu       sync.RWMutex
	segments map[SegmentID][]byte

	// For testing error injection
	UploadError   error
	DownloadError error
	ListError     error
	DeleteError   error
}

// NewMemoryStore creates a new in-memory store for testing.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		segments: make(map[SegmentID][]byte),
	}
}

// Upload stores the segment data in memory.
func (m *MemoryStore) Upload(ctx context.Context, segmentID SegmentID, data io.Reader, size int64) error {
	if m.UploadError != nil {
		return m.UploadError
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	buf := new(bytes.Buffer)
	if _, err := io.Copy(buf, data); err != nil {
		return err
	}

	m.mu.Lock()
	m.segments[segmentID] = buf.Bytes()
	m.mu.Unlock()

	return nil
}

// Download retrieves segment data from memory.
func (m *MemoryStore) Download(ctx context.Context, segmentID SegmentID) (io.ReadCloser, int64, error) {
	if m.DownloadError != nil {
		return nil, 0, m.DownloadError
	}

	select {
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	default:
	}

	m.mu.RLock()
	data, ok := m.segments[segmentID]
	m.mu.RUnlock()

	if !ok {
		return nil, 0, ErrSegmentNotFound
	}

	// Return a copy to avoid data races
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	return io.NopCloser(bytes.NewReader(dataCopy)), int64(len(dataCopy)), nil
}

// Exists checks if a segment exists in memory.
func (m *MemoryStore) Exists(ctx context.Context, segmentID SegmentID) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	m.mu.RLock()
	_, ok := m.segments[segmentID]
	m.mu.RUnlock()

	return ok, nil
}

// List returns all segment IDs in ascending order.
func (m *MemoryStore) List(ctx context.Context) ([]SegmentID, error) {
	if m.ListError != nil {
		return nil, m.ListError
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	m.mu.RLock()
	ids := make([]SegmentID, 0, len(m.segments))
	for id := range m.segments {
		ids = append(ids, id)
	}
	m.mu.RUnlock()

	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})

	return ids, nil
}

// Delete removes a segment from memory.
func (m *MemoryStore) Delete(ctx context.Context, segmentID SegmentID) error {
	if m.DeleteError != nil {
		return m.DeleteError
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	m.mu.Lock()
	delete(m.segments, segmentID)
	m.mu.Unlock()

	return nil
}

// SegmentCount returns the number of segments in the store (for testing).
func (m *MemoryStore) SegmentCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.segments)
}

// GetData returns the raw data for a segment (for testing).
func (m *MemoryStore) GetData(segmentID SegmentID) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.segments[segmentID]
	if !ok {
		return nil, false
	}
	// Return a copy
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	return dataCopy, true
}
