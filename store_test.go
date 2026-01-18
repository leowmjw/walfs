package walfs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNoOpStore_Upload(t *testing.T) {
	store := &NoOpStore{}
	ctx := context.Background()

	err := store.Upload(ctx, 1, bytes.NewReader([]byte("test")), 4)
	assert.NoError(t, err)
}

func TestNoOpStore_Download(t *testing.T) {
	store := &NoOpStore{}
	ctx := context.Background()

	reader, size, err := store.Download(ctx, 1)
	assert.Nil(t, reader)
	assert.Equal(t, int64(0), size)
	assert.ErrorIs(t, err, ErrSegmentNotFound)
}

func TestNoOpStore_Exists(t *testing.T) {
	store := &NoOpStore{}
	ctx := context.Background()

	exists, err := store.Exists(ctx, 1)
	assert.False(t, exists)
	assert.NoError(t, err)
}

func TestNoOpStore_List(t *testing.T) {
	store := &NoOpStore{}
	ctx := context.Background()

	ids, err := store.List(ctx)
	assert.Nil(t, ids)
	assert.NoError(t, err)
}

func TestNoOpStore_Delete(t *testing.T) {
	store := &NoOpStore{}
	ctx := context.Background()

	err := store.Delete(ctx, 1)
	assert.NoError(t, err)
}

func TestMemoryStore_UploadAndDownload(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	// Upload test data
	testData := []byte("hello world segment data")
	err := store.Upload(ctx, 1, bytes.NewReader(testData), int64(len(testData)))
	require.NoError(t, err)

	// Download and verify
	reader, size, err := store.Download(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, int64(len(testData)), size)

	downloadedData, err := io.ReadAll(reader)
	require.NoError(t, err)
	reader.Close()

	assert.Equal(t, testData, downloadedData)
}

func TestMemoryStore_DownloadNonExistent(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	reader, size, err := store.Download(ctx, 999)
	assert.Nil(t, reader)
	assert.Equal(t, int64(0), size)
	assert.ErrorIs(t, err, ErrSegmentNotFound)
}

func TestMemoryStore_Exists(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	// Initially does not exist
	exists, err := store.Exists(ctx, 1)
	assert.NoError(t, err)
	assert.False(t, exists)

	// Upload
	err = store.Upload(ctx, 1, bytes.NewReader([]byte("data")), 4)
	require.NoError(t, err)

	// Now exists
	exists, err = store.Exists(ctx, 1)
	assert.NoError(t, err)
	assert.True(t, exists)
}

func TestMemoryStore_List(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	// Empty list initially
	ids, err := store.List(ctx)
	assert.NoError(t, err)
	assert.Empty(t, ids)

	// Add segments in non-sequential order
	for _, id := range []SegmentID{5, 1, 3, 2, 4} {
		err := store.Upload(ctx, id, bytes.NewReader([]byte("data")), 4)
		require.NoError(t, err)
	}

	// List should return sorted IDs
	ids, err = store.List(ctx)
	assert.NoError(t, err)
	assert.Equal(t, []SegmentID{1, 2, 3, 4, 5}, ids)
}

func TestMemoryStore_Delete(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	// Upload
	err := store.Upload(ctx, 1, bytes.NewReader([]byte("data")), 4)
	require.NoError(t, err)

	// Verify exists
	exists, _ := store.Exists(ctx, 1)
	assert.True(t, exists)

	// Delete
	err = store.Delete(ctx, 1)
	assert.NoError(t, err)

	// Verify deleted
	exists, _ = store.Exists(ctx, 1)
	assert.False(t, exists)

	// Delete non-existent (should not error - idempotent)
	err = store.Delete(ctx, 999)
	assert.NoError(t, err)
}

func TestMemoryStore_ErrorInjection(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	injectedErr := errors.New("injected error")

	// Test upload error
	store.UploadError = injectedErr
	err := store.Upload(ctx, 1, bytes.NewReader([]byte("data")), 4)
	assert.ErrorIs(t, err, injectedErr)
	store.UploadError = nil

	// Test download error
	store.Upload(ctx, 1, bytes.NewReader([]byte("data")), 4)
	store.DownloadError = injectedErr
	_, _, err = store.Download(ctx, 1)
	assert.ErrorIs(t, err, injectedErr)
	store.DownloadError = nil

	// Test list error
	store.ListError = injectedErr
	_, err = store.List(ctx)
	assert.ErrorIs(t, err, injectedErr)
	store.ListError = nil

	// Test delete error
	store.DeleteError = injectedErr
	err = store.Delete(ctx, 1)
	assert.ErrorIs(t, err, injectedErr)
}

func TestMemoryStore_ContextCancellation(t *testing.T) {
	store := NewMemoryStore()

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// All operations should return context error
	err := store.Upload(ctx, 1, bytes.NewReader([]byte("data")), 4)
	assert.ErrorIs(t, err, context.Canceled)

	_, _, err = store.Download(ctx, 1)
	assert.ErrorIs(t, err, context.Canceled)

	_, err = store.Exists(ctx, 1)
	assert.ErrorIs(t, err, context.Canceled)

	_, err = store.List(ctx)
	assert.ErrorIs(t, err, context.Canceled)

	err = store.Delete(ctx, 1)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestMemoryStore_ConcurrentAccess(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	const numGoroutines = 10
	const numOperations = 100

	var wg sync.WaitGroup

	// Concurrent uploads
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				segID := SegmentID(base*numOperations + j)
				data := []byte("test data")
				_ = store.Upload(ctx, segID, bytes.NewReader(data), int64(len(data)))
			}
		}(i)
	}

	wg.Wait()

	// Verify all segments uploaded
	ids, err := store.List(ctx)
	assert.NoError(t, err)
	assert.Len(t, ids, numGoroutines*numOperations)

	// Concurrent reads
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				segID := SegmentID(base*numOperations + j)
				reader, _, err := store.Download(ctx, segID)
				if err == nil {
					reader.Close()
				}
			}
		}(i)
	}

	wg.Wait()
}

func TestMemoryStore_SegmentCount(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	assert.Equal(t, 0, store.SegmentCount())

	store.Upload(ctx, 1, bytes.NewReader([]byte("data")), 4)
	assert.Equal(t, 1, store.SegmentCount())

	store.Upload(ctx, 2, bytes.NewReader([]byte("data")), 4)
	assert.Equal(t, 2, store.SegmentCount())

	store.Delete(ctx, 1)
	assert.Equal(t, 1, store.SegmentCount())
}

func TestMemoryStore_GetData(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	// Non-existent
	data, ok := store.GetData(1)
	assert.False(t, ok)
	assert.Nil(t, data)

	// Upload and retrieve
	testData := []byte("test segment data")
	store.Upload(ctx, 1, bytes.NewReader(testData), int64(len(testData)))

	data, ok = store.GetData(1)
	assert.True(t, ok)
	assert.Equal(t, testData, data)
}

func TestMemoryStore_DataIsolation(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	// Upload data
	originalData := []byte("original data")
	store.Upload(ctx, 1, bytes.NewReader(originalData), int64(len(originalData)))

	// Get copy via GetData
	dataCopy, _ := store.GetData(1)

	// Modify the copy
	dataCopy[0] = 'X'

	// Original should be unchanged
	reader, _, _ := store.Download(ctx, 1)
	downloadedData, _ := io.ReadAll(reader)
	reader.Close()

	assert.Equal(t, originalData, downloadedData)
}

func TestMemoryStore_TimeoutContext(t *testing.T) {
	store := NewMemoryStore()

	// Create context with very short timeout (already expired)
	ctx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()

	// Give the timeout time to expire
	time.Sleep(1 * time.Millisecond)

	// Operations should return deadline exceeded
	err := store.Upload(ctx, 1, bytes.NewReader([]byte("data")), 4)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestMemoryStore_LargeSegment(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	// Create a large segment (1MB)
	largeData := make([]byte, 1024*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	// Upload
	err := store.Upload(ctx, 1, bytes.NewReader(largeData), int64(len(largeData)))
	require.NoError(t, err)

	// Download and verify
	reader, size, err := store.Download(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, int64(len(largeData)), size)

	downloadedData, err := io.ReadAll(reader)
	require.NoError(t, err)
	reader.Close()

	assert.Equal(t, largeData, downloadedData)
}

func TestMemoryStore_OverwriteSegment(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	// Upload initial data
	data1 := []byte("first version")
	err := store.Upload(ctx, 1, bytes.NewReader(data1), int64(len(data1)))
	require.NoError(t, err)

	// Upload different data to same segment (overwrite)
	data2 := []byte("second version with more data")
	err = store.Upload(ctx, 1, bytes.NewReader(data2), int64(len(data2)))
	require.NoError(t, err)

	// Should have the new data
	reader, size, err := store.Download(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, int64(len(data2)), size)

	downloadedData, _ := io.ReadAll(reader)
	reader.Close()

	assert.Equal(t, data2, downloadedData)

	// Should still be only one segment
	assert.Equal(t, 1, store.SegmentCount())
}
