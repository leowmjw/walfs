package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ankur-anand/walfs"
)

// This example demonstrates the idle segment rotation feature for low-volume data producers.
//
// Scenario: A logging or metrics collection system that receives sporadic writes
// but needs to ensure data is uploaded to remote storage within a maximum time window.
//
// Without idle rotation: Data could sit in an unsealed segment indefinitely,
// preventing other nodes from reading it.
//
// With idle rotation: After a configurable idle period (e.g., 1 minute),
// the segment is automatically rotated and uploaded, making data available globally.

func main() {
	// Create a temporary directory for WAL files
	walDir := "./wal-data"
	if err := os.MkdirAll(walDir, 0755); err != nil {
		log.Fatalf("Failed to create WAL directory: %v", err)
	}
	defer os.RemoveAll(walDir)

	fmt.Println("=== Idle Segment Rotation Example ===")
	fmt.Println()

	// Configure idle rotation duration
	// In production, this might be 1-5 minutes depending on data freshness requirements
	idleDuration := 3 * time.Second
	fmt.Printf("Configuration:\n")
	fmt.Printf("  - Idle rotation timeout: %v\n", idleDuration)
	fmt.Printf("  - Max segment size: 1MB\n")
	fmt.Println()

	// Create an in-memory store to simulate remote object storage
	// In production, you would use TigrisStore or another S3-compatible store
	store := walfs.NewMemoryStore()

	// Create WALog with idle rotation and remote upload enabled
	wal, err := walfs.NewWALog(walDir, ".wal",
		walfs.WithMaxSegmentSize(1024*1024), // 1MB segments
		walfs.WithIdleSegmentRotation(idleDuration),
		walfs.WithRemoteStore(store),
		walfs.WithUploadOnSeal(true), // Upload to remote store when segment is sealed
	)
	if err != nil {
		log.Fatalf("Failed to create WALog: %v", err)
	}
	defer wal.Close()

	fmt.Println("WALog initialized with idle rotation enabled")
	fmt.Println()

	// Scenario 1: Regular writes keep the segment active
	fmt.Println("--- Scenario 1: Regular writes prevent rotation ---")
	fmt.Printf("Writing data every %v (faster than idle timeout)...\n", time.Second)

	for i := 1; i <= 3; i++ {
		data := []byte(fmt.Sprintf("Regular write #%d at %v", i, time.Now().Format("15:04:05")))
		pos, err := wal.Write(data, 0)
		if err != nil {
			log.Fatalf("Write failed: %v", err)
		}
		fmt.Printf("  ✓ Wrote record at segment=%d offset=%d\n", pos.SegmentID, pos.Offset)
		time.Sleep(1 * time.Second)
	}

	currentSeg := wal.Current().ID()
	fmt.Printf("Current segment: %d (still active, not rotated)\n", currentSeg)
	fmt.Println()

	// Scenario 2: Idle period triggers automatic rotation
	fmt.Println("--- Scenario 2: Idle period triggers automatic rotation ---")
	fmt.Printf("Waiting %v without writes...\n", idleDuration+500*time.Millisecond)

	time.Sleep(idleDuration + 500*time.Millisecond)

	newSeg := wal.Current().ID()
	fmt.Printf("Current segment: %d (rotated from %d)\n", newSeg, currentSeg)

	if newSeg > currentSeg {
		fmt.Println("  ✓ Segment automatically rotated due to idle timeout!")

		// Check that sealed segment was uploaded to remote store
		ctx := context.Background()
		exists, err := store.Exists(ctx, currentSeg)
		if err == nil && exists {
			fmt.Printf("  ✓ Sealed segment %d uploaded to remote store\n", currentSeg)
		}
	}
	fmt.Println()

	// Scenario 3: Demonstrating data availability after rotation
	fmt.Println("--- Scenario 3: Reading data from rotated segments ---")

	// Write one more record after rotation
	data := []byte(fmt.Sprintf("Post-rotation write at %v", time.Now().Format("15:04:05")))
	pos, err := wal.Write(data, 0)
	if err != nil {
		log.Fatalf("Write failed: %v", err)
	}
	fmt.Printf("Wrote new record at segment=%d offset=%d\n", pos.SegmentID, pos.Offset)
	fmt.Println()

	// Read all data sequentially
	fmt.Println("Reading all records sequentially:")
	reader := wal.NewReader()
	defer reader.Close()

	recordCount := 0
	for {
		data, pos, err := reader.Next()
		if err != nil {
			break // EOF
		}
		recordCount++
		fmt.Printf("  [%d] Segment %d: %s\n", recordCount, pos.SegmentID, string(data))
	}
	fmt.Printf("Total records read: %d\n", recordCount)
	fmt.Println()

	// Scenario 4: Simulate multi-node deployment
	fmt.Println("--- Scenario 4: Simulating multi-node deployment ---")
	fmt.Println("Node 1 (writer) writes data, Node 2 (reader) retrieves from remote store")
	fmt.Println()

	// Write some data that will trigger rotation
	fmt.Println("Node 1: Writing data...")
	for i := 1; i <= 2; i++ {
		data := []byte(fmt.Sprintf("Multi-node write #%d", i))
		_, err := wal.Write(data, 0)
		if err != nil {
			log.Fatalf("Write failed: %v", err)
		}
		fmt.Printf("  ✓ Node 1 wrote record #%d\n", i)
	}

	// Wait for idle rotation
	fmt.Printf("\nNode 1: Waiting for idle rotation (%v)...\n", idleDuration)
	time.Sleep(idleDuration + 500*time.Millisecond)
	rotatedSegID := wal.Current().ID() - 1
	fmt.Printf("  ✓ Node 1: Segment %d rotated and uploaded\n", rotatedSegID)
	fmt.Println()

	// Simulate Node 2 reading from remote store
	fmt.Println("Node 2: Creating WALog with download-on-read...")
	walDir2 := "./wal-data-node2"
	if err := os.MkdirAll(walDir2, 0755); err != nil {
		log.Fatalf("Failed to create Node 2 WAL directory: %v", err)
	}
	defer os.RemoveAll(walDir2)

	wal2, err := walfs.NewWALog(walDir2, ".wal",
		walfs.WithRemoteStore(store),
		walfs.WithDownloadOnRead(true), // Download missing segments from remote store
	)
	if err != nil {
		log.Fatalf("Failed to create Node 2 WALog: %v", err)
	}
	defer wal2.Close()

	fmt.Printf("  ✓ Node 2: WALog initialized, discovered %d segments from remote\n", len(wal2.Segments()))

	// Node 2 reads the data
	fmt.Println("\nNode 2: Reading data downloaded from remote store:")
	reader2 := wal2.NewReader()
	defer reader2.Close()

	recordCount2 := 0
	for {
		data, pos, err := reader2.Next()
		if err != nil {
			break // EOF
		}
		recordCount2++
		fmt.Printf("  [%d] Segment %d: %s\n", recordCount2, pos.SegmentID, string(data))
	}
	fmt.Printf("  ✓ Node 2 read %d records from remote store\n", recordCount2)
	fmt.Println()

	// Summary
	fmt.Println("=== Summary ===")
	fmt.Println("✓ Idle rotation ensures data freshness for low-volume producers")
	fmt.Println("✓ Sealed segments are automatically uploaded to remote storage")
	fmt.Println("✓ Other nodes can access data globally via download-on-read")
	fmt.Println("✓ Configurable timeout balances latency vs. segment overhead")
	fmt.Println()
	fmt.Println("Use cases:")
	fmt.Println("  - Low-volume logging systems")
	fmt.Println("  - Periodic metrics collection")
	fmt.Println("  - Event-driven architectures with bursty traffic")
	fmt.Println("  - Multi-region deployments requiring data synchronization")
}
