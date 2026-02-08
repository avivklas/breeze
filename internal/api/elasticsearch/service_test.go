package elasticsearch

import (
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

	manager, err := shard.NewManager(path, 1)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}
	defer manager.Close()

	service := NewService(manager)
	gin.SetMode(gin.TestMode)
	r := gin.Default()
	r.POST("/_bulk", service.Bulk)

	bulkData := `{"index":{"_id":"1"}}
{"name":"test1"}
{"index":{"_id":"2"}}
{"name":"test2"}
`
	req, _ := http.NewRequest("POST", "/_bulk", bytes.NewBufferString(bulkData))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	doc, _ := manager.Get("1")
	if doc["name"] != "test1" {
		t.Errorf("expected test1, got %v", doc["name"])
	}

	doc2, _ := manager.Get("2")
	if doc2["name"] != "test2" {
		t.Errorf("expected test2, got %v", doc2["name"])
	}
}
