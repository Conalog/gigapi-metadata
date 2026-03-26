package metadata

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestNewIndexFSOnly(t *testing.T) {
	MergeConfigurations = []MergeConfigurationsConf{
		{10, 10 * 1024 * 1024, 1},
	}
	fsLayers := []Layer{
		{"file://./_testdata", "l1", "fs", 20, "", ""},
	}
	idx, err := NewIndex("_testdata", "default", "test", fsLayers)
	if err != nil {
		t.Fatalf("NewIndex failed: %v", err)
	}
	defer idx.Stop()

	if _, ok := idx.(*JSONIndex); !ok {
		t.Fatalf("expected *JSONIndex for fs-only layers, got %T", idx)
	}

	ents := generateEntries("l1", 100)
	p := idx.Batch(ents, nil)
	if _, err := p.Get(); err != nil {
		t.Fatalf("Batch failed: %v", err)
	}
}

func TestNewIndexMixedLayers(t *testing.T) {
	MergeConfigurations = []MergeConfigurationsConf{
		{10, 10 * 1024 * 1024, 1},
	}
	skipIfNoS3(t)
	mixedLayers := []Layer{
		{"file://./_testdata", "l1", "fs", 20, "", ""},
		{getS3TestURL(), "l2", "s3", 0, "", ""},
	}
	idx, err := NewIndex("_testdata", "default", "test", mixedLayers)
	if err != nil {
		t.Fatalf("NewIndex failed: %v", err)
	}
	defer idx.Stop()

	hybrid, ok := idx.(*HybridIndex)
	if !ok {
		t.Fatalf("expected *HybridIndex for mixed layers, got %T", idx)
	}

	if hybrid.jsonIdx == nil {
		t.Fatal("expected jsonIdx to be set")
	}
	if hybrid.s3Idx == nil {
		t.Fatal("expected s3Idx to be set")
	}
}

func TestHybridMovePlanLayerTo(t *testing.T) {
	MergeConfigurations = []MergeConfigurationsConf{
		{10, 10 * 1024 * 1024, 1},
	}
	skipIfNoS3(t)
	mixedLayers := []Layer{
		{"file://./_testdata", "l1", "fs", 1, "", ""},
		{getS3TestURL(), "l2", "s3", 0, "", ""},
	}
	idx, err := NewIndex("_testdata", "default", "test", mixedLayers)
	if err != nil {
		t.Fatalf("NewIndex failed: %v", err)
	}
	defer idx.Stop()

	ents := generateEntries("l1", 10)
	for _, e := range ents {
		e.ChunkTime = time.Now().Add(-10 * time.Second).UnixNano()
	}
	p := idx.Batch(ents, nil)
	if _, err := p.Get(); err != nil {
		t.Fatalf("Batch failed: %v", err)
	}

	plan, err := idx.GetMovePlanner().GetMovePlan("test-writer", "l1")
	if err != nil {
		t.Fatalf("GetMovePlan failed: %v", err)
	}
	if plan.PathFrom != "" && plan.LayerTo != "l2" {
		t.Errorf("expected LayerTo=l2, got %q", plan.LayerTo)
	}
}

func TestNewIndexUnsupportedType(t *testing.T) {
	badLayers := []Layer{
		{"file://./_testdata", "l1", "unknown", 20, "", ""},
	}
	_, err := NewIndex("_testdata", "default", "test", badLayers)
	if err == nil {
		t.Fatal("expected error for unsupported layer type")
	}
}

func generateEntries(layer string, count int) []*IndexEntry {
	var ents []*IndexEntry
	now := time.Now().UTC()
	for i := 0; i < count; i++ {
		ts := now.Add(-time.Duration(i) * 15 * time.Second)
		ents = append(ents, &IndexEntry{
			Database:  "default",
			Table:     "test",
			MinTime:   ts.UnixNano(),
			MaxTime:   ts.Add(15 * time.Second).UnixNano(),
			Path:      fmt.Sprintf("date=%s/hour=%02d/%s.1.parquet", ts.Format("2006-01-02"), ts.Hour(), uuid.New().String()),
			SizeBytes: 1000,
			ChunkTime: time.Now().UnixNano(),
			Layer:     layer,
		})
	}
	return ents
}
