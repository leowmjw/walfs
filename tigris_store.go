package walfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// TigrisConfig holds configuration for connecting to Tigris object storage.
type TigrisConfig struct {
	// Endpoint is the Tigris endpoint URL.
	// Use "https://fly.storage.tigris.dev" for Fly.io hosted apps.
	// Use "https://t3.storage.dev" for external access.
	Endpoint string

	// AccessKeyID is the Tigris access key.
	AccessKeyID string

	// SecretAccessKey is the Tigris secret key.
	SecretAccessKey string

	// BucketName is the bucket to use for storing segments.
	BucketName string

	// Prefix is an optional prefix for all segment objects (e.g., "wal/production/").
	// If provided, it should end with a slash.
	Prefix string

	// Region is the region for the S3 client (typically "auto" for Tigris).
	Region string
}

// TigrisStore implements SegmentStore using Tigris (S3-compatible) storage.
type TigrisStore struct {
	client     *s3.Client
	bucketName string
	prefix     string
}

// NewTigrisStore creates a new TigrisStore with the given configuration.
func NewTigrisStore(ctx context.Context, cfg TigrisConfig) (*TigrisStore, error) {
	if cfg.Endpoint == "" {
		return nil, errors.New("tigris endpoint is required")
	}
	if cfg.BucketName == "" {
		return nil, errors.New("bucket name is required")
	}
	if cfg.AccessKeyID == "" || cfg.SecretAccessKey == "" {
		return nil, errors.New("access credentials are required")
	}

	if cfg.Region == "" {
		cfg.Region = "auto"
	}

	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(cfg.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.AccessKeyID,
			cfg.SecretAccessKey,
			"",
		)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.Endpoint)
		o.Region = cfg.Region
		o.UsePathStyle = false // Tigris prefers virtual-hosted style
	})

	return &TigrisStore{
		client:     client,
		bucketName: cfg.BucketName,
		prefix:     cfg.Prefix,
	}, nil
}

// objectKey generates the S3 object key for a segment ID.
// Format: {prefix}{segmentID:09d}.wal (matches local file naming)
func (t *TigrisStore) objectKey(segmentID SegmentID) string {
	return fmt.Sprintf("%s%09d.wal", t.prefix, segmentID)
}

// parseSegmentID extracts the segment ID from an object key.
func (t *TigrisStore) parseSegmentID(key string) (SegmentID, bool) {
	// Remove prefix
	key = strings.TrimPrefix(key, t.prefix)
	// Remove .wal suffix
	key = strings.TrimSuffix(key, ".wal")

	id, err := strconv.ParseUint(key, 10, 32)
	if err != nil {
		return 0, false
	}
	return SegmentID(id), true
}

// Upload uploads a sealed segment to Tigris.
// This operation is synchronous - when it returns nil, the data is durably stored
// and visible to all readers globally (Tigris provides strong read-after-write consistency).
func (t *TigrisStore) Upload(ctx context.Context, segmentID SegmentID, data io.Reader, size int64) error {
	// Read all data into memory for reliable upload
	// For segments up to 16MB this is acceptable
	// For larger segments, consider multipart upload
	buf := new(bytes.Buffer)
	if _, err := io.Copy(buf, data); err != nil {
		return fmt.Errorf("failed to read segment data: %w", err)
	}

	key := t.objectKey(segmentID)

	_, err := t.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(t.bucketName),
		Key:           aws.String(key),
		Body:          bytes.NewReader(buf.Bytes()),
		ContentLength: aws.Int64(int64(buf.Len())),
		ContentType:   aws.String("application/octet-stream"),
	})
	if err != nil {
		return fmt.Errorf("failed to upload segment %d to Tigris: %w", segmentID, err)
	}

	return nil
}

// Download retrieves a segment from Tigris.
// Returns ErrSegmentNotFound if the segment does not exist.
// The caller is responsible for closing the returned ReadCloser.
func (t *TigrisStore) Download(ctx context.Context, segmentID SegmentID) (io.ReadCloser, int64, error) {
	key := t.objectKey(segmentID)

	result, err := t.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(t.bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		// Check if it's a "not found" error
		var noSuchKey *types.NoSuchKey
		if errors.As(err, &noSuchKey) {
			return nil, 0, ErrSegmentNotFound
		}
		// Also check for generic not found status
		var notFound *types.NotFound
		if errors.As(err, &notFound) {
			return nil, 0, ErrSegmentNotFound
		}
		return nil, 0, fmt.Errorf("failed to download segment %d from Tigris: %w", segmentID, err)
	}

	size := int64(0)
	if result.ContentLength != nil {
		size = *result.ContentLength
	}

	return result.Body, size, nil
}

// Exists checks if a segment exists in Tigris.
func (t *TigrisStore) Exists(ctx context.Context, segmentID SegmentID) (bool, error) {
	key := t.objectKey(segmentID)

	_, err := t.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(t.bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		// Check if it's a "not found" error
		var notFound *types.NotFound
		if errors.As(err, &notFound) {
			return false, nil
		}
		var noSuchKey *types.NoSuchKey
		if errors.As(err, &noSuchKey) {
			return false, nil
		}
		// For other errors, return false with no error (safe default)
		// This allows recovery to continue even if HeadObject fails
		return false, nil
	}

	return true, nil
}

// List returns all segment IDs available in Tigris.
// Results are returned in ascending order by segment ID.
func (t *TigrisStore) List(ctx context.Context) ([]SegmentID, error) {
	var segmentIDs []SegmentID

	paginator := s3.NewListObjectsV2Paginator(t.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(t.bucketName),
		Prefix: aws.String(t.prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list segments from Tigris: %w", err)
		}

		for _, obj := range page.Contents {
			if obj.Key != nil {
				if id, ok := t.parseSegmentID(*obj.Key); ok {
					segmentIDs = append(segmentIDs, id)
				}
			}
		}
	}

	// Sort in ascending order
	slices.Sort(segmentIDs)

	return segmentIDs, nil
}

// Delete removes a segment from Tigris.
// Returns nil if segment does not exist (idempotent).
func (t *TigrisStore) Delete(ctx context.Context, segmentID SegmentID) error {
	key := t.objectKey(segmentID)

	_, err := t.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(t.bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		// DeleteObject is idempotent - ignore not found errors
		var notFound *types.NotFound
		if errors.As(err, &notFound) {
			return nil
		}
		var noSuchKey *types.NoSuchKey
		if errors.As(err, &noSuchKey) {
			return nil
		}
		return fmt.Errorf("failed to delete segment %d from Tigris: %w", segmentID, err)
	}

	return nil
}

// Compile-time check that TigrisStore implements SegmentStore
var _ SegmentStore = (*TigrisStore)(nil)
