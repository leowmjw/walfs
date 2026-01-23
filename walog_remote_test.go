package walfs_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/walfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWALog_WithRemoteStore_UploadOnSeal tests that segments are uploaded when sealed.
func TestWALog_WithRemoteStore_UploadOnSeal(t *testing.T) {
	dir := t.TempDir()
	store := walfs.NewMemoryStore()

	wal, err := walfs.NewWALog(dir, ".wal",
		walfs.WithMaxSegmentSize(256), // Small size to trigger rotation
		walfs.WithRemoteStore(store),
		walfs.WithUploadOnSeal(true),
		walfs.WithUploadTimeout(10*time.Second),
	)
	require.NoError(t, err)
	defer wal.Close()

	// Write enough data to trigger rotation
	data := bytes.Repeat([]byte("A"), 100)
	for i := 0; i < 5; i++ {
		_, err := wal.Write(data, 0)
		require.NoError(t, err)
	}

	// Force a rotation
	err = wal.RotateSegment()
	require.NoError(t, err)

	// Verify segments were uploaded
	ids, err := store.List(context.Background())
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(ids), 1, "at least one segment should be uploaded")
}

// TestWALog_WithRemoteStore_DownloadOnRead tests that missing segments are downloaded.
func TestWALog_WithRemoteStore_DownloadOnRead(t *testing.T) {
	writerDir := t.TempDir()
	readerDir := t.TempDir()
	store := walfs.NewMemoryStore()

	// Writer creates segments and uploads them
	writer, err := walfs.NewWALog(writerDir, ".wal",
		walfs.WithMaxSegmentSize(256),
		walfs.WithRemoteStore(store),
		walfs.WithUploadOnSeal(true),
	)
	require.NoError(t, err)

	// Write data to create multiple segments
	testData := [][]byte{
		[]byte("segment1-data"),
		bytes.Repeat([]byte("B"), 100),
		[]byte("segment2-data"),
		bytes.Repeat([]byte("C"), 100),
	}

	for _, data := range testData {
		_, err := writer.Write(data, 0)
		require.NoError(t, err)
	}

	// Force rotation to upload all sealed segments
	err = writer.RotateSegment()
	require.NoError(t, err)
	writer.Close()

	// Verify store has segments
	ids, err := store.List(context.Background())
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(ids), 1, "store should have segments")

	// Reader starts with empty directory and downloads from store
	reader, err := walfs.NewWALog(readerDir, ".wal",
		walfs.WithMaxSegmentSize(256),
		walfs.WithRemoteStore(store),
		walfs.WithDownloadOnRead(true),
	)
	require.NoError(t, err)
	defer reader.Close()

	// Verify segments were downloaded
	localFiles, err := os.ReadDir(readerDir)
	require.NoError(t, err)

	walFiles := 0
	for _, f := range localFiles {
		if filepath.Ext(f.Name()) == ".wal" {
			walFiles++
		}
	}
	assert.GreaterOrEqual(t, walFiles, 1, "segments should be downloaded locally")
}

// TestWALog_WithRemoteStore_StrongConsistency tests that upload completes before seal returns.
func TestWALog_WithRemoteStore_StrongConsistency(t *testing.T) {
	dir := t.TempDir()
	store := walfs.NewMemoryStore()

	wal, err := walfs.NewWALog(dir, ".wal",
		walfs.WithMaxSegmentSize(256),
		walfs.WithRemoteStore(store),
		walfs.WithUploadOnSeal(true),
	)
	require.NoError(t, err)
	defer wal.Close()

	// Write data
	_, err = wal.Write(bytes.Repeat([]byte("X"), 100), 0)
	require.NoError(t, err)

	// Rotate (which seals and uploads)
	err = wal.RotateSegment()
	require.NoError(t, err)

	// Immediately after rotation, segment should be in store
	exists, err := store.Exists(context.Background(), 1)
	require.NoError(t, err)
	assert.True(t, exists, "segment should be immediately available after rotation")
}

// TestWALog_WithRemoteStore_UploadFailure tests that rotation fails if upload fails.
func TestWALog_WithRemoteStore_UploadFailure(t *testing.T) {
	dir := t.TempDir()
	store := walfs.NewMemoryStore()
	store.UploadError = errors.New("simulated upload failure")

	wal, err := walfs.NewWALog(dir, ".wal",
		walfs.WithMaxSegmentSize(256),
		walfs.WithRemoteStore(store),
		walfs.WithUploadOnSeal(true),
	)
	require.NoError(t, err)
	defer wal.Close()

	// Write data
	_, err = wal.Write(bytes.Repeat([]byte("Y"), 100), 0)
	require.NoError(t, err)

	// Rotation should fail due to upload failure
	err = wal.RotateSegment()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "upload")
}

// TestWALog_WithRemoteStore_DownloadFailure tests graceful handling of download failures.
func TestWALog_WithRemoteStore_DownloadFailure(t *testing.T) {
	dir := t.TempDir()
	store := walfs.NewMemoryStore()

	// Manually add a segment to the store
	segmentData := make([]byte, 256)
	copy(segmentData, []byte("mock segment data"))
	err := store.Upload(context.Background(), 5, bytes.NewReader(segmentData), int64(len(segmentData)))
	require.NoError(t, err)

	// Set download error
	store.DownloadError = errors.New("simulated download failure")

	// Reader should still initialize (download failure is logged but not fatal)
	reader, err := walfs.NewWALog(dir, ".wal",
		walfs.WithRemoteStore(store),
		walfs.WithDownloadOnRead(true),
	)
	require.NoError(t, err)
	defer reader.Close()

	// Should have created initial segment locally
	assert.NotNil(t, reader.Current())
}

// TestWALog_WithRemoteStore_NoRemoteStore tests backward compatibility without remote store.
func TestWALog_WithRemoteStore_NoRemoteStore(t *testing.T) {
	dir := t.TempDir()

	// Create WAL without remote store (local-only mode)
	wal, err := walfs.NewWALog(dir, ".wal",
		walfs.WithMaxSegmentSize(256),
	)
	require.NoError(t, err)
	defer wal.Close()

	// Write and rotate should work normally
	_, err = wal.Write([]byte("local only data"), 0)
	require.NoError(t, err)

	err = wal.RotateSegment()
	require.NoError(t, err)

	assert.Equal(t, walfs.SegmentID(2), wal.Current().ID())
}

// TestWALog_WithRemoteStore_UploadTimeout tests upload timeout handling.
func TestWALog_WithRemoteStore_UploadTimeout(t *testing.T) {
	dir := t.TempDir()

	// Create a slow store that takes longer than timeout
	store := &slowStore{
		MemoryStore: walfs.NewMemoryStore(),
		delay:       100 * time.Millisecond,
	}

	wal, err := walfs.NewWALog(dir, ".wal",
		walfs.WithMaxSegmentSize(256),
		walfs.WithRemoteStore(store),
		walfs.WithUploadOnSeal(true),
		walfs.WithUploadTimeout(10*time.Millisecond), // Very short timeout
	)
	require.NoError(t, err)
	defer wal.Close()

	// Write data
	_, err = wal.Write(bytes.Repeat([]byte("Z"), 100), 0)
	require.NoError(t, err)

	// Rotation should fail due to timeout
	err = wal.RotateSegment()
	assert.Error(t, err)
}

// slowStore is a MemoryStore wrapper that adds delay to uploads.
type slowStore struct {
	*walfs.MemoryStore
	delay time.Duration
}

func (s *slowStore) Upload(ctx context.Context, segmentID walfs.SegmentID, data io.Reader, size int64) error {
	select {
	case <-time.After(s.delay):
		return s.MemoryStore.Upload(ctx, segmentID, data, size)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TestWALog_WithRemoteStore_ConcurrentReaders tests concurrent reader downloads.
func TestWALog_WithRemoteStore_ConcurrentReaders(t *testing.T) {
	store := walfs.NewMemoryStore()

	// Writer creates segments
	writerDir := t.TempDir()
	writer, err := walfs.NewWALog(writerDir, ".wal",
		walfs.WithMaxSegmentSize(512),
		walfs.WithRemoteStore(store),
		walfs.WithUploadOnSeal(true),
	)
	require.NoError(t, err)

	// Write data to create segments
	for i := 0; i < 10; i++ {
		_, err := writer.Write([]byte("concurrent test data"), 0)
		require.NoError(t, err)
	}
	writer.RotateSegment()
	writer.Close()

	// Multiple readers start concurrently
	const numReaders = 5
	var wg sync.WaitGroup
	errChan := make(chan error, numReaders)

	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()

			readerDir := t.TempDir()
			reader, err := walfs.NewWALog(readerDir, ".wal",
				walfs.WithRemoteStore(store),
				walfs.WithDownloadOnRead(true),
			)
			if err != nil {
				errChan <- err
				return
			}
			defer reader.Close()

			// Verify reader has segments
			if reader.Current() == nil {
				errChan <- errors.New("reader has no current segment")
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("reader error: %v", err)
	}
}

// TestWALog_WithRemoteStore_MultipleRotations tests multiple segment rotations with uploads.
func TestWALog_WithRemoteStore_MultipleRotations(t *testing.T) {
	dir := t.TempDir()
	store := walfs.NewMemoryStore()

	wal, err := walfs.NewWALog(dir, ".wal",
		walfs.WithMaxSegmentSize(256),
		walfs.WithRemoteStore(store),
		walfs.WithUploadOnSeal(true),
	)
	require.NoError(t, err)
	defer wal.Close()

	// Perform multiple rotations
	for i := 0; i < 5; i++ {
		_, err := wal.Write(bytes.Repeat([]byte{byte('A' + i)}, 100), 0)
		require.NoError(t, err)

		err = wal.RotateSegment()
		require.NoError(t, err)
	}

	// Verify all sealed segments are in store
	ids, err := store.List(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 5, len(ids), "all rotated segments should be uploaded")
}

// TestWALog_WithRemoteStore_LargeSegment tests uploading large segments.
func TestWALog_WithRemoteStore_LargeSegment(t *testing.T) {
	dir := t.TempDir()
	store := walfs.NewMemoryStore()

	segmentSize := int64(1024 * 1024) // 1MB
	wal, err := walfs.NewWALog(dir, ".wal",
		walfs.WithMaxSegmentSize(segmentSize),
		walfs.WithRemoteStore(store),
		walfs.WithUploadOnSeal(true),
	)
	require.NoError(t, err)
	defer wal.Close()

	// Write large data
	largeData := bytes.Repeat([]byte("LARGE"), 100000) // 500KB
	_, err = wal.Write(largeData, 0)
	require.NoError(t, err)

	// Rotate to upload
	err = wal.RotateSegment()
	require.NoError(t, err)

	// Verify upload
	ids, err := store.List(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, len(ids))

	// Verify size
	data, ok := store.GetData(ids[0])
	assert.True(t, ok)
	assert.Greater(t, len(data), 500000, "uploaded segment should contain large data")
}

// TestWALog_WithRemoteStore_RecoveryFromRemote tests full recovery from remote store.
func TestWALog_WithRemoteStore_RecoveryFromRemote(t *testing.T) {
	store := walfs.NewMemoryStore()

	// Phase 1: Writer creates and uploads segments
	writerDir := t.TempDir()
	{
		writer, err := walfs.NewWALog(writerDir, ".wal",
			walfs.WithMaxSegmentSize(256),
			walfs.WithRemoteStore(store),
			walfs.WithUploadOnSeal(true),
		)
		require.NoError(t, err)

		// Write test data
		testRecords := [][]byte{
			[]byte("record-1"),
			[]byte("record-2"),
			[]byte("record-3"),
		}

		for _, rec := range testRecords {
			_, err := writer.Write(rec, 0)
			require.NoError(t, err)
		}

		// Force upload
		err = writer.RotateSegment()
		require.NoError(t, err)
		writer.Close()
	}

	// Phase 2: Reader recovers from remote (fresh directory)
	readerDir := t.TempDir()
	{
		reader, err := walfs.NewWALog(readerDir, ".wal",
			walfs.WithMaxSegmentSize(256),
			walfs.WithRemoteStore(store),
			walfs.WithDownloadOnRead(true),
		)
		require.NoError(t, err)
		defer reader.Close()

		// Verify segments exist
		segments := reader.Segments()
		assert.GreaterOrEqual(t, len(segments), 1, "should have recovered segments")

		// Read data
		r := reader.NewReader()
		defer r.Close()

		recordsRead := 0
		for {
			_, _, err := r.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("unexpected error reading: %v", err)
			}
			recordsRead++
		}
		assert.GreaterOrEqual(t, recordsRead, 1, "should read back records")
	}
}

// TestWALog_WithRemoteStore_PartialDownloadRecovery tests recovery with valid remote segment.
func TestWALog_WithRemoteStore_PartialDownloadRecovery(t *testing.T) {
	store := walfs.NewMemoryStore()

	// Create a valid segment by using the actual segment file
	tmpDir := t.TempDir()
	seg, err := walfs.OpenSegmentFile(tmpDir, ".wal", 1, walfs.WithSegmentSize(256))
	require.NoError(t, err)
	_, err = seg.Write([]byte("test data"), 0)
	require.NoError(t, err)
	seg.SealSegment()
	seg.Close()

	// Read the segment file and upload to store
	segPath := tmpDir + "/000000001.wal"
	segData, err := os.ReadFile(segPath)
	require.NoError(t, err)
	err = store.Upload(context.Background(), 1, bytes.NewReader(segData), int64(len(segData)))
	require.NoError(t, err)

	// Reader downloads and opens from fresh directory
	readerDir := t.TempDir()
	reader, err := walfs.NewWALog(readerDir, ".wal",
		walfs.WithRemoteStore(store),
		walfs.WithDownloadOnRead(true),
	)
	require.NoError(t, err)
	defer reader.Close()

	// Should have the downloaded segment
	assert.NotNil(t, reader.Current())
	assert.GreaterOrEqual(t, len(reader.Segments()), 1)
}

// TestWALog_WithRemoteStore_EmptyRemoteStore tests behavior with empty remote store.
func TestWALog_WithRemoteStore_EmptyRemoteStore(t *testing.T) {
	dir := t.TempDir()
	store := walfs.NewMemoryStore() // Empty store

	wal, err := walfs.NewWALog(dir, ".wal",
		walfs.WithRemoteStore(store),
		walfs.WithDownloadOnRead(true),
	)
	require.NoError(t, err)
	defer wal.Close()

	// Should create initial segment locally
	assert.Equal(t, walfs.SegmentID(1), wal.Current().ID())

	// Write should work
	_, err = wal.Write([]byte("data"), 0)
	require.NoError(t, err)
}

// TestWALog_WithRemoteStore_MixedLocalAndRemote tests with some segments local, some remote.
func TestWALog_WithRemoteStore_MixedLocalAndRemote(t *testing.T) {
	store := walfs.NewMemoryStore()
	dir := t.TempDir()

	// Create local segment 1
	seg1, err := walfs.OpenSegmentFile(dir, ".wal", 1, walfs.WithSegmentSize(256))
	require.NoError(t, err)
	_, err = seg1.Write([]byte("local-1"), 0)
	require.NoError(t, err)
	seg1.SealSegment()
	seg1.Close()

	// Create a valid segment 2 in a temp dir and upload to store
	tmpDir := t.TempDir()
	seg2, err := walfs.OpenSegmentFile(tmpDir, ".wal", 2, walfs.WithSegmentSize(256))
	require.NoError(t, err)
	_, err = seg2.Write([]byte("remote-2"), 0)
	require.NoError(t, err)
	seg2.SealSegment()
	seg2.Close()

	// Read and upload segment 2
	seg2Path := tmpDir + "/000000002.wal"
	seg2Data, err := os.ReadFile(seg2Path)
	require.NoError(t, err)
	err = store.Upload(context.Background(), 2, bytes.NewReader(seg2Data), int64(len(seg2Data)))
	require.NoError(t, err)

	// Open WAL with download enabled
	wal, err := walfs.NewWALog(dir, ".wal",
		walfs.WithMaxSegmentSize(256),
		walfs.WithRemoteStore(store),
		walfs.WithDownloadOnRead(true),
	)
	require.NoError(t, err)
	defer wal.Close()

	// Should have both segments
	segments := wal.Segments()
	assert.GreaterOrEqual(t, len(segments), 2, "should have both local and remote segments")
}
