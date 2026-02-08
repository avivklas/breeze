package store

import (
	"os"
	"testing"
)

func TestStore(t *testing.T) {
	path := "test_db"
	defer os.RemoveAll(path)

	s, err := Open(path, true)
	if err != nil {
		t.Fatalf("failed to open store: %v", err)
	}

	doc := map[string]interface{}{
		"name": "Breeze",
		"type": "Database",
	}

	if err := s.Index("1", doc); err != nil {
		t.Fatalf("failed to index doc: %v", err)
	}

	s.Close()

	// Re-open and check if data persists (WAL replay or Bleve persistence)
	s, err = Open(path, true)
	if err != nil {
		t.Fatalf("failed to re-open store: %v", err)
	}
	defer s.Close()

	count, err := s.index.DocCount()
	if err != nil {
		t.Fatalf("failed to get doc count: %v", err)
	}

	if count != 1 {
		t.Errorf("expected 1 doc, got %d", count)
	}
}
