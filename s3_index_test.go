package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"os"
	"testing"
	"time"
)

// S3 test URL - requires a running MinIO instance.
// Set S3_TEST_URL env var to override, e.g.:
// S3_TEST_URL=s3://minioadmin:minioadmin@localhost:9000/test-bucket/metadata-test?secure=false
func getS3TestURL() string {
	if url := os.Getenv("S3_TEST_URL"); url != "" {
		return url
	}
	return "s3://minioadmin:minioadmin@localhost:9000/test-bucket/metadata-test?secure=false"
}

var s3TestLayers = []Layer{
	{getS3TestURL(), "l1", "s3", 20, "", ""},
}

func skipIfNoS3(t *testing.T) {
	t.Helper()
	url := getS3TestURL()
	cfg, err := parseS3URL(url)
	if err != nil {
		t.Skipf("Skipping S3 test: invalid URL: %v", err)
	}
	sl, err := newS3Layer(Layer{URL: url, Type: "s3"})
	if err != nil {
		t.Skipf("Skipping S3 test: cannot create client: %v", err)
	}
	// Try to list objects to check connectivity
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	exists, err := sl.Client.BucketExists(ctx, cfg.Bucket)
	if err != nil || !exists {
		t.Skipf("Skipping S3 test: bucket %q not available (err=%v, exists=%v)", cfg.Bucket, err, exists)
	}
}

func TestS3URLParsing(t *testing.T) {
	tests := []struct {
		url      string
		bucket   string
		prefix   string
		secure   bool
		hasError bool
	}{
		{
			url:    "s3://key:secret@localhost:9000/mybucket/myprefix?secure=false&region=us-east-1",
			bucket: "mybucket", prefix: "myprefix", secure: false,
		},
		{
			url:    "s3://key:secret@localhost:9000/mybucket?secure=true",
			bucket: "mybucket", prefix: "", secure: true,
		},
		{
			url:    "s3://key:secret@s3.amazonaws.com/mybucket/deep/prefix",
			bucket: "mybucket", prefix: "deep/prefix", secure: true,
		},
		{
			url:      "http://invalid",
			hasError: true,
		},
	}

	for _, tt := range tests {
		cfg, err := parseS3URL(tt.url)
		if tt.hasError {
			if err == nil {
				t.Errorf("expected error for URL %q", tt.url)
			}
			continue
		}
		if err != nil {
			t.Errorf("unexpected error for URL %q: %v", tt.url, err)
			continue
		}
		if cfg.Bucket != tt.bucket {
			t.Errorf("bucket: got %q, want %q", cfg.Bucket, tt.bucket)
		}
		if cfg.Prefix != tt.prefix {
			t.Errorf("prefix: got %q, want %q", cfg.Prefix, tt.prefix)
		}
		if cfg.Secure != tt.secure {
			t.Errorf("secure: got %v, want %v", cfg.Secure, tt.secure)
		}
	}
}

func TestS3Save(t *testing.T) {
	skipIfNoS3(t)
	MergeConfigurations = []MergeConfigurationsConf{
		{10, 10 * 1024 * 1024, 1},
	}
	idx, err := NewS3Index("default", "test_s3", s3TestLayers)
	if err != nil {
		t.Fatalf("Failed to create S3 index: %v", err)
	}
	defer idx.Stop()

	var ents []*IndexEntry
	now := time.Now()
	oneHourAgo := now.Add(-1 * time.Hour)

	for ts := oneHourAgo; ts.Before(now); ts = ts.Add(5 * time.Minute) {
		ents = append(ents, &IndexEntry{
			Database: "default",
			Table:    "test_s3",
			MinTime:  ts.UnixNano(),
			MaxTime:  ts.Add(5 * time.Minute).UnixNano(),
			Path: fmt.Sprintf("date=%s/hour=%02d/%s.1.parquet",
				ts.UTC().Format("2006-01-02"),
				ts.UTC().Hour(),
				uuid.New().String()),
			SizeBytes: 1000000,
			ChunkTime: time.Now().UnixNano(),
			Layer:     "l1",
		})
	}

	p := idx.Batch(ents, nil)
	_, err = p.Get()
	if err != nil {
		t.Fatalf("Batch add failed: %v", err)
	}
	t.Logf("Items saved: %d", len(ents))

	// Verify data via GetAll
	all, err := idx.GetAll()
	if err != nil {
		t.Fatalf("GetAll failed: %v", err)
	}
	if len(all) != len(ents) {
		t.Errorf("GetAll: got %d entries, want %d", len(all), len(ents))
	}
}

func TestS3SaveAndRM(t *testing.T) {
	skipIfNoS3(t)
	MergeConfigurations = []MergeConfigurationsConf{
		{10, 10 * 1024 * 1024, 1},
	}
	idx, err := NewS3Index("default", "test_s3_rm", s3TestLayers)
	if err != nil {
		t.Fatalf("Failed to create S3 index: %v", err)
	}
	defer idx.Stop()

	var ents []*IndexEntry
	now := time.Now()
	oneHourAgo := now.Add(-1 * time.Hour)

	for ts := oneHourAgo; ts.Before(now); ts = ts.Add(10 * time.Minute) {
		ents = append(ents, &IndexEntry{
			Database: "default",
			Table:    "test_s3_rm",
			MinTime:  ts.UnixNano(),
			MaxTime:  ts.Add(10 * time.Minute).UnixNano(),
			Path: fmt.Sprintf("date=%s/hour=%02d/%s.1.parquet",
				ts.UTC().Format("2006-01-02"),
				ts.UTC().Hour(),
				uuid.New().String()),
			SizeBytes: 1000000,
			ChunkTime: time.Now().UnixNano(),
			Layer:     "l1",
		})
	}

	// Add entries
	p := idx.Batch(ents, nil)
	_, err = p.Get()
	if err != nil {
		t.Fatalf("Batch add failed: %v", err)
	}
	t.Logf("Items saved: %d", len(ents))

	// Remove entries
	p = idx.Batch(nil, ents)
	_, err = p.Get()
	if err != nil {
		t.Fatalf("Batch rm failed: %v", err)
	}

	// Verify all removed
	all, err := idx.GetAll()
	if err != nil {
		t.Fatalf("GetAll failed: %v", err)
	}
	if len(all) != 0 {
		t.Errorf("GetAll after rm: got %d entries, want 0", len(all))
	}
}

func TestS3Query(t *testing.T) {
	skipIfNoS3(t)
	MergeConfigurations = []MergeConfigurationsConf{
		{10, 10 * 1024 * 1024, 1},
	}
	idx, err := NewS3Index("default", "test_s3_query", s3TestLayers)
	if err != nil {
		t.Fatalf("Failed to create S3 index: %v", err)
	}
	defer idx.Stop()

	now := time.Now().UTC()
	var ents []*IndexEntry
	for i := 0; i < 5; i++ {
		ts := now.Add(-time.Duration(i) * time.Hour)
		ents = append(ents, &IndexEntry{
			Database: "default",
			Table:    "test_s3_query",
			MinTime:  ts.UnixNano(),
			MaxTime:  ts.Add(30 * time.Minute).UnixNano(),
			Path: fmt.Sprintf("date=%s/hour=%02d/%s.1.parquet",
				ts.Format("2006-01-02"),
				ts.Hour(),
				uuid.New().String()),
			SizeBytes: 500000,
			ChunkTime: time.Now().UnixNano(),
			Layer:     "l1",
		})
	}

	p := idx.Batch(ents, nil)
	_, err = p.Get()
	if err != nil {
		t.Fatalf("Batch add failed: %v", err)
	}

	// Query last 2 hours
	results, err := idx.GetQuerier().Query(QueryOptions{
		After: now.Add(-2 * time.Hour),
	})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	t.Logf("Query returned %d entries (expected ~2-3)", len(results))
}

func TestS3ConditionalPut(t *testing.T) {
	skipIfNoS3(t)
	MergeConfigurations = []MergeConfigurationsConf{
		{10, 10 * 1024 * 1024, 1},
	}

	// Create index and add an entry
	idx, err := NewS3Index("default", "test_s3_cond", s3TestLayers)
	if err != nil {
		t.Fatalf("Failed to create S3 index: %v", err)
	}
	defer idx.Stop()

	now := time.Now().UTC()
	entry := &IndexEntry{
		Database: "default",
		Table:    "test_s3_cond",
		MinTime:  now.UnixNano(),
		MaxTime:  now.Add(time.Hour).UnixNano(),
		Path: fmt.Sprintf("date=%s/hour=%02d/%s.1.parquet",
			now.Format("2006-01-02"),
			now.Hour(),
			uuid.New().String()),
		SizeBytes: 500000,
		ChunkTime: now.UnixNano(),
		Layer:     "l1",
	}

	p := idx.Batch([]*IndexEntry{entry}, nil)
	_, err = p.Get()
	if err != nil {
		t.Fatalf("Batch add failed: %v", err)
	}

	// Externally modify the metadata.json to simulate concurrent write
	sl, err := newS3Layer(s3TestLayers[0])
	if err != nil {
		t.Fatalf("Failed to create S3 layer: %v", err)
	}
	key := sl.objectPath("default", "test_s3_cond",
		fmt.Sprintf("date=%s/hour=%02d", now.Format("2006-01-02"), now.Hour()))
	externalData, _ := json.Marshal(map[string]any{
		"type":               "test_s3_cond",
		"parquet_size_bytes": 0,
		"row_count":          0,
		"min_time":           0,
		"max_time":           0,
		"wal_sequence":       0,
		"drop_queue":         []any{},
		"files":              []any{},
	})
	_, err = sl.putObject(context.Background(), key, externalData)
	if err != nil {
		t.Fatalf("External write failed: %v", err)
	}

	// Now add another entry - this should detect the ETag mismatch, re-read, merge, and succeed
	entry2 := &IndexEntry{
		Database: "default",
		Table:    "test_s3_cond",
		MinTime:  now.Add(time.Hour).UnixNano(),
		MaxTime:  now.Add(2 * time.Hour).UnixNano(),
		Path: fmt.Sprintf("date=%s/hour=%02d/%s.1.parquet",
			now.Format("2006-01-02"),
			now.Hour(),
			uuid.New().String()),
		SizeBytes: 300000,
		ChunkTime: now.UnixNano(),
		Layer:     "l1",
	}

	p = idx.Batch([]*IndexEntry{entry2}, nil)
	_, err = p.Get()
	if err != nil {
		t.Fatalf("Batch add after external write failed: %v", err)
	}
	t.Log("Conditional put with conflict resolution succeeded")
}

func TestS3KVStore(t *testing.T) {
	skipIfNoS3(t)

	kv, err := NewS3KVStoreIndex(getS3TestURL())
	if err != nil {
		t.Fatalf("Failed to create S3 KV store: %v", err)
	}
	defer kv.Destroy()

	// Put
	err = kv.Put("testkey", []byte("testvalue"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Get
	val, err := kv.Get("testkey")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(val) != "testvalue" {
		t.Errorf("Get: got %q, want %q", string(val), "testvalue")
	}

	// Delete
	err = kv.Delete("testkey")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify deleted
	val, err = kv.Get("testkey")
	if err != nil {
		t.Fatalf("Get after delete failed: %v", err)
	}
	if val != nil {
		t.Errorf("Get after delete: got %q, want nil", string(val))
	}
}

func TestS3DBIndex(t *testing.T) {
	skipIfNoS3(t)
	MergeConfigurations = []MergeConfigurationsConf{
		{10, 10 * 1024 * 1024, 1},
	}

	// First create some data so we have something to discover
	idx, err := NewS3Index("testdb", "testtable", s3TestLayers)
	if err != nil {
		t.Fatalf("Failed to create S3 index: %v", err)
	}

	now := time.Now().UTC()
	entry := &IndexEntry{
		Database: "testdb",
		Table:    "testtable",
		MinTime:  now.UnixNano(),
		MaxTime:  now.Add(time.Hour).UnixNano(),
		Path: fmt.Sprintf("date=%s/hour=%02d/%s.1.parquet",
			now.Format("2006-01-02"),
			now.Hour(),
			uuid.New().String()),
		SizeBytes: 500000,
		ChunkTime: now.UnixNano(),
		Layer:     "l1",
	}
	p := idx.Batch([]*IndexEntry{entry}, nil)
	_, err = p.Get()
	if err != nil {
		t.Fatalf("Batch failed: %v", err)
	}
	idx.Stop()

	// Now test DBIndex
	dbIdx, err := NewS3DBIndex(s3TestLayers)
	if err != nil {
		t.Fatalf("Failed to create S3 DB index: %v", err)
	}

	dbs, err := dbIdx.Databases()
	if err != nil {
		t.Fatalf("Databases failed: %v", err)
	}
	t.Logf("Databases: %v", dbs)

	tbls, err := dbIdx.Tables("testdb")
	if err != nil {
		t.Fatalf("Tables failed: %v", err)
	}
	t.Logf("Tables: %v", tbls)

	paths, err := dbIdx.Paths("testdb", "testtable")
	if err != nil {
		t.Fatalf("Paths failed: %v", err)
	}
	t.Logf("Paths: %v", paths)
}
