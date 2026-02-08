package shard

import (
	"os"
	"testing"

	"github.com/blevesearch/bleve/v2"
)

func TestManager(t *testing.T) {
	path := "test_shards"
	defer os.RemoveAll(path)

	m, err := NewManager(path, 3)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}
	defer m.Close()

	idx, err := m.CreateIndex("testindex", 3)
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	docs := []map[string]interface{}{
		{"id": "1", "name": "Apple"},
		{"id": "2", "name": "Banana"},
		{"id": "3", "name": "Cherry"},
		{"id": "4", "name": "Date"},
	}

	for _, doc := range docs {
		if err := idx.Index(doc["id"].(string), doc); err != nil {
			t.Errorf("failed to index doc %s: %v", doc["id"], err)
		}
	}

	// Search
	query := bleve.NewMatchQuery("Apple")
	req := bleve.NewSearchRequest(query)
	res, err := idx.Search(req)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	if res.Total != 1 {
		t.Errorf("expected 1 hit, got %d", res.Total)
	}
}
