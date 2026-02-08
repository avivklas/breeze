package elasticsearch

import (
	"breeze/internal/shard"
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

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
	r.GET("/_cluster/health/:index", s.Health)
	r.GET("/_cluster/settings", s.ClusterSettings)
	r.GET("/_nodes", s.Nodes)
	r.GET("/_nodes/stats", s.NodeStats)
	r.GET("/_nodes/stats/:metric", s.NodeStats)
	r.GET("/_license", s.License)
	r.GET("/_xpack", s.XPack)
	r.GET("/_cat/indices", s.CatIndices)
	r.GET("/_mapping", s.Mapping)
	r.GET("/:index/_mapping", s.Mapping)
	r.GET("/_template", s.Empty)
	r.GET("/_template/*name", s.Empty)

	r.PUT("/:index", s.CreateIndex)
	r.GET("/:index", s.GetIndexInfo)
	r.HEAD("/:index", s.HeadIndex)
	r.PUT("/:index/_doc/:id", s.Index)
	r.POST("/:index/_doc/:id", s.Index)
	r.PUT("/:index/_create/:id", s.Index)
	r.POST("/:index/_create/:id", s.Index)
	r.GET("/:index/_doc/:id", s.Get)
	r.DELETE("/:index/_doc/:id", s.Delete)
	r.POST("/:index/_search", s.Search)
	r.GET("/:index/_search", s.Search)
	r.POST("/_aliases", s.Aliases)
	r.POST("/:index/_update_by_query", s.UpdateByQuery)

	r.POST("/_bulk", s.Bulk)
	r.POST("/_mget", s.MGet)
	r.POST("/:index/_mget", s.MGet)
	r.POST("/_msearch", s.MSearch)

	r.POST("/_monitoring/bulk", s.MonitoringBulk)
}

func (s *Service) Empty(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{})
}

func (s *Service) MonitoringBulk(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"took": 0, "errors": false})
}

func (s *Service) Aliases(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"acknowledged": true})
}

func (s *Service) UpdateByQuery(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"took":                   0,
		"timed_out":              false,
		"total":                  0,
		"updated":                0,
		"deleted":                0,
		"batches":                0,
		"version_conflicts":      0,
		"noops":                  0,
		"retries":                gin.H{"bulk": 0, "search": 0},
		"throttled_millis":       0,
		"requests_per_second":    -1.0,
		"throttled_until_millis": 0,
		"failures":               []interface{}{},
	})
}

func (s *Service) getOrCreateIndex(name string) (*shard.Index, error) {
	idx := s.manager.GetIndex(name)
	if idx != nil {
		return idx, nil
	}
	return s.manager.CreateIndex(name, 0)
}

func (s *Service) Info(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"name":         "breeze-node",
		"cluster_name": "breeze-cluster",
		"cluster_uuid": "breeze-cluster-uuid",
		"version": gin.H{
			"number":         "8.10.2",
			"build_flavor":   "default",
			"build_type":     "tar",
			"build_hash":     "breeze-hash",
			"build_date":     "2023-01-01T00:00:00Z",
			"build_snapshot": false,
			"lucene_version": "9.7.0",
		},
		"tagline": "You Know, for Search",
	})
}

func (s *Service) License(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"license": gin.H{
			"status":                "active",
			"uid":                   "breeze-license-uuid",
			"type":                  "basic",
			"issue_date":            "2023-01-01T00:00:00.000Z",
			"issue_date_in_millis":  1672531200000,
			"expiry_date":           "2099-01-01T00:00:00.000Z",
			"expiry_date_in_millis": 4070908800000,
			"max_nodes":             1000,
			"issued_to":             "breeze",
			"issuer":                "breeze",
			"start_date_in_millis":  -1,
		},
	})
}

func (s *Service) XPack(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"features": gin.H{
			"security":             gin.H{"available": true, "enabled": false},
			"monitoring":           gin.H{"available": true, "enabled": true},
			"ml":                   gin.H{"available": true, "enabled": false},
			"graph":                gin.H{"available": true, "enabled": false},
			"searchable_snapshots": gin.H{"available": true, "enabled": false},
			"sql":                  gin.H{"available": true, "enabled": true},
		},
		"license": gin.H{
			"uid":                   "breeze-license-uuid",
			"type":                  "basic",
			"mode":                  "basic",
			"status":                "active",
			"expiry_date_in_millis": 4070908800000,
		},
	})
}

func (s *Service) ClusterSettings(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"persistent": gin.H{},
		"transient":  gin.H{},
		"defaults": gin.H{
			"script.allowed_types":    "inline,stored",
			"script.allowed_contexts": "score,update,ingest",
		},
	})
}

func (s *Service) CatIndices(c *gin.Context) {
	indices := s.manager.ListIndices()
	var sb strings.Builder
	for _, name := range indices {
		sb.WriteString(fmt.Sprintf("green open %s uuid 1 0 0 0 0b 0b\n", name))
	}
	c.String(http.StatusOK, sb.String())
}

func (s *Service) HeadIndex(c *gin.Context) {
	name := c.Param("index")
	idx := s.manager.GetIndex(name)
	if idx != nil {
		c.Status(http.StatusOK)
	} else {
		c.Status(http.StatusNotFound)
	}
}

func (s *Service) GetIndexInfo(c *gin.Context) {
	name := c.Param("index")
	names := strings.Split(name, ",")
	result := make(map[string]interface{})
	foundAny := false

	for _, n := range names {
		n = strings.TrimSpace(n)
		if n == "" {
			continue
		}
		idx := s.manager.GetIndex(n)
		if idx != nil {
			foundAny = true
			result[n] = gin.H{
				"settings": gin.H{
					"index": gin.H{
						"number_of_shards":   "1",
						"number_of_replicas": "0",
						"version": gin.H{
							"created": "8100299",
						},
					},
				},
				"mappings": gin.H{
					"properties": s.convertMapping(idx),
				},
			}
		}
	}

	if !foundAny && !strings.Contains(c.Request.URL.RawQuery, "ignore_unavailable=true") {
		c.JSON(http.StatusNotFound, gin.H{
			"error": gin.H{
				"root_cause": []gin.H{
					{
						"type":   "index_not_found_exception",
						"reason": "no such index",
						"index":  name,
					},
				},
				"type":   "index_not_found_exception",
				"reason": "no such index",
				"index":  name,
			},
			"status": 404,
		})
		return
	}

	c.JSON(http.StatusOK, result)
}

func (s *Service) Mapping(c *gin.Context) {
	name := c.Param("index")
	if name == "" {
		indices := s.manager.ListIndices()
		result := make(map[string]interface{})
		for _, n := range indices {
			idx := s.manager.GetIndex(n)
			if idx != nil {
				result[n] = gin.H{"mappings": gin.H{"properties": s.convertMapping(idx)}}
			}
		}
		c.JSON(http.StatusOK, result)
		return
	}

	names := strings.Split(name, ",")
	result := make(map[string]interface{})
	for _, n := range names {
		n = strings.TrimSpace(n)
		idx := s.manager.GetIndex(n)
		if idx != nil {
			result[n] = gin.H{"mappings": gin.H{"properties": s.convertMapping(idx)}}
		}
	}

	if len(result) == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "index not found"})
		return
	}

	c.JSON(http.StatusOK, result)
}

func (s *Service) convertMapping(idx *shard.Index) map[string]interface{} {
	props := make(map[string]interface{})
	idx.Mapping.Mu.RLock()
	defer idx.Mapping.Mu.RUnlock()
	for k, t := range idx.Mapping.Fields {
		var esType string
		switch t {
		case 0:
			esType = "text"
		case 1:
			esType = "double"
		case 2:
			esType = "boolean"
		default:
			esType = "object"
		}
		props[k] = gin.H{"type": esType}
	}
	return props
}

func (s *Service) Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"cluster_name":                     "breeze-cluster",
		"status":                           "green",
		"timed_out":                        false,
		"number_of_nodes":                  1,
		"number_of_data_nodes":             1,
		"active_primary_shards":            1,
		"active_shards":                    1,
		"relocating_shards":                0,
		"initializing_shards":              0,
		"unassigned_shards":                0,
		"delayed_unassigned_shards":        0,
		"number_of_pending_tasks":          0,
		"number_of_in_flight_fetch":        0,
		"task_max_waiting_in_queue_millis": 0,
		"active_shards_percent_as_number":  100.0,
	})
}

func (s *Service) Nodes(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"nodes": gin.H{
			"breeze-node-id": gin.H{
				"name":    "breeze-node",
				"version": "8.10.2",
				"ip":      "127.0.0.1",
				"roles":   []string{"master", "data", "ingest"},
				"http": gin.H{
					"publish_address": "127.0.0.1:8080",
				},
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
					"timestamp":        0,
					"uptime_in_millis": 0,
					"mem": gin.H{
						"heap_used_in_bytes": 0,
						"heap_max_in_bytes":  0,
						"pools": gin.H{
							"young":    gin.H{"used_in_bytes": 0, "max_in_bytes": 0, "peak_used_in_bytes": 0, "peak_max_in_bytes": 0},
							"old":      gin.H{"used_in_bytes": 0, "max_in_bytes": 0, "peak_used_in_bytes": 0, "peak_max_in_bytes": 0},
							"survivor": gin.H{"used_in_bytes": 0, "max_in_bytes": 0, "peak_used_in_bytes": 0, "peak_max_in_bytes": 0},
						},
					},
					"gc": gin.H{
						"collectors": gin.H{
							"young": gin.H{"collection_count": 0, "collection_time_in_millis": 0},
							"old":   gin.H{"collection_count": 0, "collection_time_in_millis": 0},
						},
					},
				},
				"ingest": gin.H{
					"total": gin.H{
						"count":          0,
						"time_in_millis": 0,
						"current":        0,
						"failed":         0,
					},
					"pipelines": gin.H{},
				},
			},
		},
	})
}

func (s *Service) CreateIndex(c *gin.Context) {
	name := c.Param("index")
	_, err := s.manager.CreateIndex(name, 0)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": gin.H{
				"root_cause": []gin.H{
					{
						"type":   "resource_already_exists_exception",
						"reason": "index already exists",
						"index":  name,
					},
				},
				"type":   "resource_already_exists_exception",
				"reason": "index already exists",
				"index":  name,
			},
			"status": 400,
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{"acknowledged": true, "shards_acknowledged": true, "index": name})
}

func (s *Service) Index(c *gin.Context) {
	name := c.Param("index")
	id := c.Param("id")
	var data map[string]interface{}
	if err := c.BindJSON(&data); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	idx, err := s.getOrCreateIndex(name)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if err := idx.Index(id, data); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"_index":   name,
		"_id":      id,
		"result":   "created",
		"_version": 1,
	})
}

func (s *Service) Bulk(c *gin.Context) {
	scanner := bufio.NewScanner(c.Request.Body)
	type batch struct {
		ids  []string
		docs []map[string]interface{}
	}
	batches := make(map[string]*batch)

	var lastIndex string
	var lastId string

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
			lastIndex, _ = meta["_index"].(string)
			if lastIndex == "" {
				lastIndex = c.Param("index")
			}
			lastId, _ = meta["_id"].(string)
			if scanner.Scan() {
				var doc map[string]interface{}
				if err := json.Unmarshal(scanner.Bytes(), &doc); err == nil {
					if lastIndex == "" {
						continue
					}
					b, ok := batches[lastIndex]
					if !ok {
						b = &batch{}
						batches[lastIndex] = b
					}
					b.ids = append(b.ids, lastId)
					b.docs = append(b.docs, doc)
				}
			}
		}
	}

	var responseItems []interface{}
	for name, b := range batches {
		idx, err := s.getOrCreateIndex(name)
		if err != nil {
			continue
		}
		idx.BatchIndex(b.ids, b.docs)
		for _, id := range b.ids {
			responseItems = append(responseItems, gin.H{
				"index": gin.H{
					"_index": name,
					"_id":    id,
					"status": 201,
				},
			})
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"took":   0,
		"errors": false,
		"items":  responseItems,
	})
}

func (s *Service) MGet(c *gin.Context) {
	var req struct {
		Docs []struct {
			Index string `json:"_index"`
			ID    string `json:"_id"`
		} `json:"docs"`
		IDs []string `json:"ids"`
	}

	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	indexName := c.Param("index")
	var results []interface{}

	if len(req.IDs) > 0 {
		idx := s.manager.GetIndex(indexName)
		for _, id := range req.IDs {
			if idx == nil {
				results = append(results, gin.H{"found": false})
				continue
			}
			doc, _ := idx.Get(id)
			if doc != nil {
				results = append(results, gin.H{"_index": indexName, "_id": id, "found": true, "_source": doc})
			} else {
				results = append(results, gin.H{"_index": indexName, "_id": id, "found": false})
			}
		}
	} else {
		for _, d := range req.Docs {
			n := d.Index
			if n == "" {
				n = indexName
			}
			idx := s.manager.GetIndex(n)
			if idx == nil {
				results = append(results, gin.H{"found": false})
				continue
			}
			doc, _ := idx.Get(d.ID)
			if doc != nil {
				results = append(results, gin.H{"_index": n, "_id": d.ID, "found": true, "_source": doc})
			} else {
				results = append(results, gin.H{"_index": n, "_id": d.ID, "found": false})
			}
		}
	}

	c.JSON(http.StatusOK, gin.H{"docs": results})
}

func (s *Service) MSearch(c *gin.Context) {
	scanner := bufio.NewScanner(c.Request.Body)
	var responses []interface{}

	for scanner.Scan() {
		var header map[string]interface{}
		json.Unmarshal(scanner.Bytes(), &header)
		indexName, _ := header["index"].(string)
		if indexName == "" {
			indexName = c.Param("index")
		}

		if !scanner.Scan() {
			break
		}
		var body map[string]interface{}
		json.Unmarshal(scanner.Bytes(), &body)

		idx := s.manager.GetIndex(indexName)
		if idx == nil {
			responses = append(responses, gin.H{"hits": gin.H{"total": gin.H{"value": 0}, "hits": []interface{}{}}})
			continue
		}

		queryStr := "*"
		if q, ok := body["query"].(map[string]interface{}); ok {
			if m, ok := q["match_all"].(map[string]interface{}); ok && len(m) == 0 {
				queryStr = "*"
			}
		}

		q := bleve.NewQueryStringQuery(queryStr)
		req := bleve.NewSearchRequest(q)
		req.Fields = []string{"_source"}
		res, _ := idx.Search(req)

		hits := []gin.H{}
		if res != nil {
			for _, hit := range res.Hits {
				source := make(map[string]interface{})
				if s, ok := hit.Fields["_source"].(string); ok {
					json.Unmarshal([]byte(s), &source)
				}
				hits = append(hits, gin.H{
					"_index":  indexName,
					"_id":     hit.ID,
					"_score":  hit.Score,
					"_source": source,
				})
			}
		}

		responses = append(responses, gin.H{
			"took": 0,
			"hits": gin.H{
				"total": gin.H{"value": 0},
				"hits":  hits,
			},
		})
	}

	c.JSON(http.StatusOK, gin.H{"responses": responses})
}

func (s *Service) Get(c *gin.Context) {
	name := c.Param("index")
	id := c.Param("id")

	idx := s.manager.GetIndex(name)
	if idx == nil {
		c.JSON(http.StatusNotFound, gin.H{"found": false})
		return
	}

	doc, err := idx.Get(id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if doc == nil {
		c.JSON(http.StatusNotFound, gin.H{"found": false})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"_index":  name,
		"_id":     id,
		"found":   true,
		"_source": doc,
	})
}

func (s *Service) Delete(c *gin.Context) {
	name := c.Param("index")
	id := c.Param("id")

	idx := s.manager.GetIndex(name)
	if idx == nil {
		c.JSON(http.StatusNotFound, gin.H{"found": false})
		return
	}

	if err := idx.Delete(id); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"_index": name,
		"_id":    id,
		"result": "deleted",
	})
}

func (s *Service) Search(c *gin.Context) {
	name := c.Param("index")
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

	idx := s.manager.GetIndex(name)
	if idx == nil {
		c.JSON(http.StatusOK, gin.H{
			"took": 0,
			"hits": gin.H{
				"total": gin.H{"value": 0},
				"hits":  []interface{}{},
			},
		})
		return
	}

	q := bleve.NewQueryStringQuery(queryStr)
	req := bleve.NewSearchRequest(q)
	req.Fields = []string{"_source"}
	res, err := idx.Search(req)
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
			"_index":  name,
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
