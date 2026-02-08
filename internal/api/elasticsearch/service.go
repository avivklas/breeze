package elasticsearch

import (
	"breeze/internal/shard"
	"net/http"
	"encoding/json"

	"github.com/blevesearch/bleve/v2"
	"github.com/gin-gonic/gin"
)

type Service struct {
	manager *shard.Manager
}

func NewService(m *shard.Manager) *Service {
	return &Service{manager: m}
}

func (s *Service) RegisterHandlers(r *gin.Engine) {
	r.PUT("/:index/_doc/:id", s.Index)
	r.POST("/:index/_doc/:id", s.Index)
	r.GET("/:index/_doc/:id", s.Get)
	r.DELETE("/:index/_doc/:id", s.Delete)
	r.POST("/:index/_search", s.Search)
	r.GET("/:index/_search", s.Search)
}

func (s *Service) Index(c *gin.Context) {
	id := c.Param("id")
	var data map[string]interface{}
	if err := c.BindJSON(&data); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := s.manager.Index(id, data); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"_index":   c.Param("index"),
		"_id":      id,
		"result":   "created",
		"_version": 1,
	})
}

func (s *Service) Get(c *gin.Context) {
	id := c.Param("id")
	doc, err := s.manager.Get(id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if doc == nil {
		c.JSON(http.StatusNotFound, gin.H{"found": false})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"_index":   c.Param("index"),
		"_id":      id,
		"found":    true,
		"_source":  doc,
	})
}

func (s *Service) Delete(c *gin.Context) {
	id := c.Param("id")
	if err := s.manager.Delete(id); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"_index": c.Param("index"),
		"_id":    id,
		"result": "deleted",
	})
}

func (s *Service) Search(c *gin.Context) {
    var queryStr string
    if c.Request.Method == "GET" {
        queryStr = c.Query("q")
    } else {
        var req struct {
            Query interface{} `json:"query"`
        }
        if err := c.BindJSON(&req); err == nil {
            queryStr = "*"
        }
    }

    if queryStr == "" {
        queryStr = "*"
    }

	q := bleve.NewQueryStringQuery(queryStr)
	req := bleve.NewSearchRequest(q)
	req.Fields = []string{"_source"} // Always request _source
	res, err := s.manager.Search(req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	hits := []gin.H{}
	for _, hit := range res.Hits {
		source := make(map[string]interface{})
		if s, ok := hit.Fields["_source"].(string); ok {
			json.Unmarshal([]byte(s), &source)
		}
		hits = append(hits, gin.H{
			"_index":  c.Param("index"),
			"_id":     hit.ID,
			"_score":  hit.Score,
			"_source": source,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"took": res.Took.Milliseconds(),
		"hits": gin.H{
			"total": gin.H{
				"value": res.Total,
			},
			"hits": hits,
		},
	})
}
