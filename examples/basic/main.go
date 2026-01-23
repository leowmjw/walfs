package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ankur-anand/walfs"
)

// This example demonstrates basic WAL operations with Tigris object storage.
//
// It shows:
// - Writing records to a local WAL
// - Automatic segment rotation
// - Uploading sealed segments to Tigris
// - Reading back data from local and remote storage

func main() {
	fmt.Println("=== Basic WALFS with Tigris Object Store ===")
	fmt.Println()

	// Load Tigris configuration from environment variables
	cfg := walfs.TigrisConfig{
		Endpoint:        getEnv("TIGRIS_ENDPOINT", "https://fly.storage.tigris.dev"),
		AccessKeyID:     getEnv("TIGRIS_ACCESS_KEY_ID", ""),
		SecretAccessKey: getEnv("TIGRIS_SECRET_ACCESS_KEY", ""),
		BucketName:      getEnv("TIGRIS_BUCKET_NAME", ""),
		Prefix:          getEnv("TIGRIS_PREFIX", "walfs/basic/"),
		Region:          getEnv("TIGRIS_REGION", "auto"),
	}

	// Validate required configuration
	if cfg.AccessKeyID == "" || cfg.SecretAccessKey == "" || cfg.BucketName == "" {
		log.Println("⚠️  Tigris credentials not configured. Running in local-only mode.")
		log.Println("    To enable remote storage, set the following environment variables:")
		log.Println("    - TIGRIS_ACCESS_KEY_ID")
		log.Println("    - TIGRIS_SECRET_ACCESS_KEY")
		log.Println("    - TIGRIS_BUCKET_NAME")
		fmt.Println()
		runLocalOnly()
		return
	}

	fmt.Printf("Configuration:\n")
	fmt.Printf("  Endpoint: %s\n", cfg.Endpoint)
	fmt.Printf("  Bucket: %s\n", cfg.BucketName)
	fmt.Printf("  Prefix: %s\n", cfg.Prefix)
	fmt.Printf("  Region: %s\n", cfg.Region)
	fmt.Println()

	// Create Tigris store
	ctx := context.Background()
	store, err := walfs.NewTigrisStore(ctx, cfg)
	if err != nil {
		log.Fatalf("Failed to create Tigris store: %v", err)
	}

	// Create WAL directory
	walDir := "./wal-data"
	if err := os.MkdirAll(walDir, 0755); err != nil {
		log.Fatalf("Failed to create WAL directory: %v", err)
	}
	defer os.RemoveAll(walDir)

	// Create WALog with Tigris integration
	wal, err := walfs.NewWALog(walDir, ".wal",
		walfs.WithMaxSegmentSize(512*1024), // 512KB segments for demo
		walfs.WithRemoteStore(store),
		walfs.WithUploadOnSeal(true), // Upload segments when sealed
	)
	if err != nil {
		log.Fatalf("Failed to create WALog: %v", err)
	}
	defer wal.Close()

	fmt.Println("✓ WALog initialized with Tigris storage")
	fmt.Println()

	// Write some records
	fmt.Println("--- Writing Records ---")
	records := []string{
		"User login: alice@example.com",
		"Order created: order-12345",
		"Payment processed: $99.99",
		"Shipment dispatched: tracking-67890",
		"User logout: alice@example.com",
	}

	var positions []walfs.RecordPosition
	for i, record := range records {
		data := []byte(fmt.Sprintf("[%s] %s", time.Now().Format("15:04:05"), record))
		pos, err := wal.Write(data, 0)
		if err != nil {
			log.Fatalf("Write failed: %v", err)
		}
		positions = append(positions, pos)
		fmt.Printf("  ✓ Record %d: segment=%d offset=%d - %s\n", i+1, pos.SegmentID, pos.Offset, record)
	}
	fmt.Println()

	// Force segment rotation to trigger upload
	fmt.Println("--- Rotating Segment ---")
	if err := wal.RotateSegment(); err != nil {
		log.Fatalf("Segment rotation failed: %v", err)
	}
	fmt.Println("  ✓ Segment rotated and uploaded to Tigris")
	fmt.Println()

	// Read back the records
	fmt.Println("--- Reading Records ---")
	for i, pos := range positions {
		data, err := wal.Read(pos)
		if err != nil {
			log.Fatalf("Read failed: %v", err)
		}
		fmt.Printf("  [%d] %s\n", i+1, string(data))
	}
	fmt.Println()

	// List segments in remote store
	fmt.Println("--- Remote Storage Status ---")
	segmentIDs, err := store.List(ctx)
	if err != nil {
		log.Fatalf("Failed to list remote segments: %v", err)
	}
	fmt.Printf("  Remote segments: %v\n", segmentIDs)

	for _, segID := range segmentIDs {
		exists, _ := store.Exists(ctx, segID)
		if exists {
			fmt.Printf("  ✓ Segment %d available in Tigris\n", segID)
		}
	}
	fmt.Println()

	// Demonstrate sequential reading
	fmt.Println("--- Sequential Reader ---")
	reader := wal.NewReader()
	defer reader.Close()

	count := 0
	for {
		data, pos, err := reader.Next()
		if err != nil {
			break // EOF
		}
		count++
		fmt.Printf("  [%d] Segment %d: %s\n", count, pos.SegmentID, string(data))
	}
	fmt.Printf("  Total records read: %d\n", count)
	fmt.Println()

	fmt.Println("=== Success ===")
	fmt.Println("✓ Records written to local WAL")
	fmt.Println("✓ Sealed segment uploaded to Tigris")
	fmt.Println("✓ Data readable from both local and remote storage")
}

// runLocalOnly demonstrates WAL operations without remote storage
func runLocalOnly() {
	fmt.Println("--- Running in Local-Only Mode ---")
	fmt.Println()

	// Create WAL directory
	walDir := "./wal-data"
	if err := os.MkdirAll(walDir, 0755); err != nil {
		log.Fatalf("Failed to create WAL directory: %v", err)
	}
	defer os.RemoveAll(walDir)

	// Create local-only WALog
	wal, err := walfs.NewWALog(walDir, ".wal",
		walfs.WithMaxSegmentSize(512*1024),
	)
	if err != nil {
		log.Fatalf("Failed to create WALog: %v", err)
	}
	defer wal.Close()

	fmt.Println("✓ WALog initialized (local only)")
	fmt.Println()

	// Write some records
	fmt.Println("--- Writing Records ---")
	for i := 1; i <= 3; i++ {
		data := []byte(fmt.Sprintf("[%s] Local record #%d", time.Now().Format("15:04:05"), i))
		pos, err := wal.Write(data, 0)
		if err != nil {
			log.Fatalf("Write failed: %v", err)
		}
		fmt.Printf("  ✓ Record %d: segment=%d offset=%d\n", i, pos.SegmentID, pos.Offset)
	}
	fmt.Println()

	// Read back
	fmt.Println("--- Reading Records ---")
	reader := wal.NewReader()
	defer reader.Close()

	count := 0
	for {
		data, _, err := reader.Next()
		if err != nil {
			break
		}
		count++
		fmt.Printf("  [%d] %s\n", count, string(data))
	}
	fmt.Println()

	fmt.Println("=== Success ===")
	fmt.Println("✓ Records written and read from local WAL")
	fmt.Println()
	fmt.Println("💡 To enable Tigris integration, configure the environment variables")
}

// getEnv retrieves an environment variable with a fallback default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
