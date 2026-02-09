package elasticsearch

import (
	"breeze/internal/cluster"
	"breeze/internal/shard"
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestBulk(t *testing.T) {
	path := "test_bulk"
	defer os.RemoveAll(path)

	c := cluster.NewCluster("node1", []string{"node1=localhost:8080"})
	manager, err := shard.NewManager(path, 1, c)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}
	defer manager.Close()

	service := NewService(manager, "localhost:8080")
	gin.SetMode(gin.TestMode)
	r := gin.Default()
	r.POST("/_bulk", service.Bulk)

	bulkData := `{"index":{"_index":"testindex","_id":"1"}}
{"name":"test1"}
{"index":{"_index":"testindex","_id":"2"}}
{"name":"test2"}
`
	req, _ := http.NewRequest("POST", "/_bulk", bytes.NewBufferString(bulkData))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	idx := manager.GetIndex("testindex")
	if idx == nil {
		t.Fatalf("index testindex not created")
	}

	doc, _ := idx.Get("1")
	if doc["name"] != "test1" {
		t.Errorf("expected test1, got %v", doc["name"])
	}

	doc2, _ := idx.Get("2")
	if doc2["name"] != "test2" {
		t.Errorf("expected test2, got %v", doc2["name"])
	}
}
