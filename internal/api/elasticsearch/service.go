package elasticsearch

import (
	"breeze/internal/shard"
	"bufio"
	"encoding/json"
	"net/http"

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
    r.Use(func(c *gin.Context) {
        c.Header("X-Elastic-Product", "Elasticsearch")
        c.Next()
    })

	r.GET("/", s.Info)
	r.GET("/_cluster/health", s.Health)
	r.GET("/_nodes", s.Nodes)
    r.GET("/_nodes/stats", s.NodeStats)
    r.GET("/_nodes/stats/:metric", s.NodeStats)
	r.PUT("/:index", s.CreateIndex)
	r.PUT("/:index/_doc/:id", s.Index)
	r.POST("/:index/_doc/:id", s.Index)
	r.GET("/:index/_doc/:id", s.Get)
	r.DELETE("/:index/_doc/:id", s.Delete)
	r.POST("/:index/_search", s.Search)
	r.GET("/:index/_search", s.Search)
	r.POST("/_bulk", s.Bulk)
}

func (s *Service) Info(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"name":         "breeze-node",
		"cluster_name": "breeze-cluster",
        "cluster_uuid": "breeze-cluster-uuid",
		"version": gin.H{
			"number": "8.10.0",
            "build_flavor": "default",
            "build_type": "tar",
            "build_hash": "breeze-hash",
            "build_date": "2023-01-01T00:00:00Z",
            "build_snapshot": false,
            "lucene_version": "9.7.0",
		},
		"tagline": "You Know, for Search",
	})
}

func (s *Service) Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"cluster_name": "breeze-cluster",
		"status":       "green",
	})
}

func (s *Service) Nodes(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"nodes": gin.H{
			"breeze-node-id": gin.H{
				"name": "breeze-node",
                "roles": []string{"master", "data", "ingest"},
			},
		},
	})
}

func (s *Service) NodeStats(c *gin.Context) {
    c.JSON(http.StatusOK, gin.H{
        "cluster_name": "breeze-cluster",
        "nodes": gin.H{
            "breeze-node-id": gin.H{
                "name": "breeze-node",
                "jvm": gin.H{
                    "timestamp": 0,
                    "uptime_in_millis": 0,
                    "mem": gin.H{
                        "heap_used_in_bytes": 0,
                        "heap_max_in_bytes": 0,
                        "pools": gin.H{
                            "young": gin.H{"used_in_bytes": 0, "max_in_bytes": 0, "peak_used_in_bytes": 0, "peak_max_in_bytes": 0},
                            "old": gin.H{"used_in_bytes": 0, "max_in_bytes": 0, "peak_used_in_bytes": 0, "peak_max_in_bytes": 0},
                            "survivor": gin.H{"used_in_bytes": 0, "max_in_bytes": 0, "peak_used_in_bytes": 0, "peak_max_in_bytes": 0},
                        },
                    },
                    "gc": gin.H{
                        "collectors": gin.H{
                            "young": gin.H{"collection_count": 0, "collection_time_in_millis": 0},
                            "old": gin.H{"collection_count": 0, "collection_time_in_millis": 0},
                        },
                    },
                },
                "ingest": gin.H{
                    "total": gin.H{
                        "count": 0,
                        "time_in_millis": 0,
                        "current": 0,
                        "failed": 0,
                    },
                    "pipelines": gin.H{},
                },
            },
        },
    })
}

func (s *Service) CreateIndex(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"acknowledged": true, "shards_acknowledged": true, "index": c.Param("index")})
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

func (s *Service) Bulk(c *gin.Context) {
	scanner := bufio.NewScanner(c.Request.Body)
	var ids []string
	var data []map[string]interface{}

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var action map[string]map[string]interface{}
		if err := json.Unmarshal(line, &action); err != nil {
			continue
		}

		if meta, ok := action["index"]; ok {
			id, _ := meta["_id"].(string)
			if scanner.Scan() {
				var doc map[string]interface{}
				if err := json.Unmarshal(scanner.Bytes(), &doc); err == nil {
					ids = append(ids, id)
					data = append(data, doc)
				}
			}
		}
	}

	if len(ids) > 0 {
		if err := s.manager.BatchIndex(ids, data); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
	}

	items := make([]interface{}, len(ids))
	for i := range ids {
		items[i] = gin.H{
			"index": gin.H{
				"_id":    ids[i],
				"status": 201,
			},
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"took":   0,
		"errors": false,
		"items":  items,
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
		"_index":  c.Param("index"),
		"_id":     id,
		"found":   true,
		"_source": doc,
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
		var req map[string]interface{}
		if err := c.BindJSON(&req); err == nil {
			if q, ok := req["query"].(map[string]interface{}); ok {
				if m, ok := q["match_all"].(map[string]interface{}); ok && len(m) == 0 {
					queryStr = "*"
				} else if query, ok := q["query_string"].(map[string]interface{}); ok {
					if qs, ok := query["query"].(string); ok {
						queryStr = qs
					}
				}
			}
		}
	}

	if queryStr == "" {
		queryStr = "*"
	}

	q := bleve.NewQueryStringQuery(queryStr)
	req := bleve.NewSearchRequest(q)
	req.Fields = []string{"_source"}
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
