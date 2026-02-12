package elasticsearch

import (
	"breeze/internal/cluster"
	"breeze/internal/shard"
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/search/query"
	"github.com/gin-gonic/gin"
)

type Service struct {
	manager    *shard.Manager
	publicAddr string
}

func NewService(m *shard.Manager, publicAddr string) *Service {
	return &Service{
		manager:    m,
		publicAddr: publicAddr,
	}
}

func (s *Service) RegisterHandlers(r *gin.Engine) {
	r.Use(func(c *gin.Context) {
		c.Header("X-Elastic-Product", "Elasticsearch")
		c.Next()
	})

	r.GET("/", s.Info)
	r.HEAD("/", s.Info)
	r.GET("/_cluster/health", s.Health)
	r.GET("/_cluster/health/:index", s.Health)
	r.GET("/_cluster/settings", s.ClusterSettings)
	r.PUT("/_cluster/settings", s.Acknowledged)
	r.GET("/_nodes", s.Nodes)
	r.GET("/_nodes/:nodeId", s.Nodes)
	r.GET("/_nodes/_local", s.Nodes)
	r.GET("/_nodes/:nodeId/:metric", s.Nodes)
	r.GET("/_nodes/stats", s.NodeStats)
	r.GET("/_nodes/stats/*metric", s.NodeStats)
	r.GET("/_license", s.License)
	r.GET("/_xpack", s.XPack)
	r.GET("/_cat/indices", s.CatIndices)
	r.GET("/_mapping", s.Mapping)
	r.GET("/:index/_mapping", s.Mapping)
	r.PUT("/:index/_mapping", s.PutMapping)
	r.POST("/:index/_mapping", s.PutMapping)
	r.GET("/_template", s.GetTemplate)
	r.GET("/_template/:name", s.GetTemplate)
	r.PUT("/_template/:name", s.PutTemplate)
	r.POST("/_template/:name", s.PutTemplate)
	r.DELETE("/_template/:name", s.DeleteTemplate)
	r.GET("/_index_template", s.GetTemplate)
	r.GET("/_index_template/:name", s.GetTemplate)
	r.PUT("/_index_template/:name", s.PutTemplate)
	r.POST("/_index_template/:name", s.PutTemplate)
	r.DELETE("/_index_template/:name", s.DeleteTemplate)

	r.GET("/_component_template", s.Acknowledged)
	r.GET("/_component_template/:name", s.Acknowledged)
	r.PUT("/_component_template/:name", s.Acknowledged)
	r.POST("/_component_template/:name", s.Acknowledged)

	r.PUT("/:index", s.CreateIndex)
	r.GET("/:index", s.GetIndexInfo)
	r.HEAD("/:index", s.HeadIndex)
	r.PUT("/:index/_settings", s.Acknowledged)
	r.PUT("/_settings", s.Acknowledged)
	r.PUT("/:index/_doc/:id", s.Index)
	r.POST("/:index/_doc/:id", s.Index)
	r.PUT("/:index/_create/:id", s.Index)
	r.POST("/:index/_create/:id", s.Index)
	r.POST("/:index/_update/:id", s.Update)
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

	r.PUT("/_ilm/policy/:name", s.Acknowledged)
	r.GET("/_ilm/policy/:name", s.ILMPolicy)

	r.POST("/_monitoring/bulk", s.MonitoringBulk)
}

func (s *Service) PutTemplate(c *gin.Context) {
	name := c.Param("name")
	var t shard.IndexTemplate
	if err := c.BindJSON(&t); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := s.manager.PutTemplate(name, t); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"acknowledged": true})
}

func (s *Service) GetTemplate(c *gin.Context) {
	name := c.Param("name")
	if name == "" {
		// List all templates
		// For simplicity, returning empty for now or implement ListTemplates in manager
		c.JSON(http.StatusOK, gin.H{})
		return
	}

	t, ok := s.manager.GetTemplate(name)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "template not found"})
		return
	}

	// For _index_template ES returns a slightly different format
	if strings.Contains(c.Request.URL.Path, "_index_template") {
		c.JSON(http.StatusOK, gin.H{
			"index_templates": []interface{}{
				gin.H{
					"name":           name,
					"index_template": t,
				},
			},
		})
	} else {
		c.JSON(http.StatusOK, gin.H{name: t})
	}
}

func (s *Service) DeleteTemplate(c *gin.Context) {
	// Not implemented in manager yet, but acknowledged to satisfy Kibana
	c.JSON(http.StatusOK, gin.H{"acknowledged": true})
}

func (s *Service) Acknowledged(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"acknowledged": true})
}

func (s *Service) ILMPolicy(c *gin.Context) {
	name := c.Param("name")
	c.JSON(http.StatusOK, gin.H{
		name: gin.H{
			"version":   1,
			"modified_date": 0,
			"policy": gin.H{
				"phases": gin.H{
					"hot": gin.H{
						"actions": gin.H{
							"rollover": gin.H{"max_age": "30d", "max_size": "50gb"},
						},
					},
				},
			},
		},
	})
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
	return s.manager.CreateIndex(name, 0, true)
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
	sort.Strings(indices)
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

func (s *Service) PutMapping(c *gin.Context) {
	name := c.Param("index")
	var req struct {
		Properties map[string]interface{} `json:"properties"`
	}
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	idx := s.manager.GetIndex(name)
	if idx == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "index not found"})
		return
	}

	idx.UpdateMapping(req.Properties)
	c.JSON(http.StatusOK, gin.H{"acknowledged": true})
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
	numNodes := len(s.manager.Cluster.Nodes)
	if numNodes == 0 {
		numNodes = 1
	}
	c.JSON(http.StatusOK, gin.H{
		"cluster_name":                     "breeze-cluster",
		"status":                           "green",
		"timed_out":                        false,
		"number_of_nodes":                  numNodes,
		"number_of_data_nodes":             numNodes,
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
	nodeIdParam := c.Param("nodeId")
	metricParam := c.Param("metric")

	// Handle /_nodes/settings which some clients use
	if nodeIdParam == "settings" {
		nodeIdParam = ""
	}
	if strings.Contains(c.Request.URL.Path, "/_local") {
		nodeIdParam = s.manager.Cluster.SelfID
	}

	nodes := make(map[string]interface{})
	clusterNodes := s.manager.Cluster.Nodes
	if len(clusterNodes) == 0 {
		clusterNodes = []cluster.Node{{ID: s.manager.Cluster.SelfID, Addr: s.publicAddr}}
	}

	for _, n := range clusterNodes {
		if nodeIdParam != "" && nodeIdParam != "_all" && nodeIdParam != n.ID {
			continue
		}

		// Try to extract host from addr
		host := n.Addr
		publishAddr := n.Addr
		if parts := strings.Split(n.Addr, ":"); len(parts) > 0 {
			host = parts[0]
			if n.ID == s.manager.Cluster.SelfID {
				publishAddr = s.publicAddr
			} else {
				// Heuristic: use same port as our publicAddr
				myPort := "8080"
				if p := strings.Split(s.publicAddr, ":"); len(p) > 1 {
					myPort = p[1]
				}
				publishAddr = host + ":" + myPort
			}
		}

		nodeInfo := gin.H{
			"name":             n.ID,
			"version":          "8.10.2",
			"ip":               host,
			"roles":            []string{"master", "data", "ingest"},
			"transport_address": n.Addr,
			"http": gin.H{
				"publish_address": publishAddr,
			},
		}

		if metricParam == "http" || nodeIdParam == "http" {
			nodes[n.ID] = gin.H{
				"name": n.ID,
				"http": nodeInfo["http"],
			}
		} else {
			nodes[n.ID] = nodeInfo
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"cluster_name": "breeze-cluster",
		"nodes":        nodes,
	})
}

func (s *Service) NodeStats(c *gin.Context) {
	nodes := make(map[string]interface{})
	clusterNodes := s.manager.Cluster.Nodes
	if len(clusterNodes) == 0 {
		clusterNodes = []cluster.Node{{ID: s.manager.Cluster.SelfID, Addr: s.publicAddr}}
	}

	for _, n := range clusterNodes {
		nodes[n.ID] = gin.H{
			"name":             n.ID,
			"timestamp":        0,
			"transport_address": n.Addr,
			"host":             n.ID,
			"ip":               n.Addr,
			"roles":            []string{"master", "data", "ingest"},
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
			"os": gin.H{
				"timestamp": 0,
				"cpu":       gin.H{"percent": 0, "load_average": gin.H{"1m": 0, "5m": 0, "15m": 0}},
				"mem":       gin.H{"total_in_bytes": 0, "free_in_bytes": 0, "used_in_bytes": 0, "free_percent": 0, "used_percent": 0},
			},
			"process": gin.H{
				"timestamp": 0,
				"open_file_descriptors": 0,
				"max_file_descriptors":  0,
				"cpu": gin.H{"percent": 0, "total_in_millis": 0},
				"mem": gin.H{"total_virtual_in_bytes": 0},
			},
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"cluster_name": "breeze-cluster",
		"nodes":        nodes,
	})
}


func (s *Service) CreateIndex(c *gin.Context) {
	name := c.Param("index")
	forward := c.Query("forward") != "false"
	shards := 0
	fmt.Sscanf(c.Query("shards"), "%d", &shards)

	_, err := s.manager.CreateIndex(name, shards, forward)

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

	opType := c.Query("op_type")
	isCreate := strings.Contains(c.Request.URL.Path, "/_create/") || opType == "create"

	idx, err := s.getOrCreateIndex(name)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if isCreate {
		existing, _ := idx.Get(id)
		if existing != nil {
			c.JSON(http.StatusConflict, gin.H{
				"error": gin.H{
					"root_cause": []gin.H{{
						"type":   "version_conflict_engine_exception",
						"reason": "document already exists",
						"index":  name,
						"id":     id,
					}},
					"type":   "version_conflict_engine_exception",
					"reason": "document already exists",
					"index":  name,
					"id":     id,
				},
				"status": 409,
			})
			return
		}
	}

	if c.Query("forward") == "false" {
		shardID := idx.GetShardID(id)
		if s, ok := idx.Shards[shardID]; ok {
			s.Index(id, data)
		} else {
			c.JSON(http.StatusBadRequest, gin.H{"error": "shard not local"})
			return
		}
	} else {
		if err := idx.Index(id, data); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"_index":   name,
		"_id":      id,
		"result":   "created",
		"_version": 1,
	})
}

func (s *Service) Update(c *gin.Context) {
	name := c.Param("index")
	id := c.Param("id")

	var req struct {
		Doc           map[string]interface{} `json:"doc"`
		Upsert        map[string]interface{} `json:"upsert"`
		DocAsUpsert   bool                   `json:"doc_as_upsert"`
		Script        interface{}            `json:"script"`
		ScriptedUpsert bool                  `json:"scripted_upsert"`
	}

	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	idx, err := s.getOrCreateIndex(name)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	doc, err := idx.Get(id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if doc == nil {
		if req.DocAsUpsert && req.Doc != nil {
			doc = req.Doc
		} else if req.Upsert != nil {
			doc = req.Upsert
		} else {
			c.JSON(http.StatusNotFound, gin.H{
				"error": gin.H{
					"root_cause": []gin.H{{
						"type":   "document_missing_exception",
						"reason": "document missing",
						"index":  name,
						"id":     id,
					}},
					"type":   "document_missing_exception",
					"reason": "document missing",
					"index":  name,
					"id":     id,
				},
				"status": 404,
			})
			return
		}
	} else {
		if req.Doc != nil {
			for k, v := range req.Doc {
				doc[k] = v
			}
		}
		// Scripts are not supported yet, but we don't want to fail if they are empty
	}

	if err := idx.Index(id, doc); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"_index":   name,
		"_id":      id,
		"result":   "updated",
		"_version": 1,
	})
}

func (s *Service) Bulk(c *gin.Context) {
	scanner := bufio.NewScanner(c.Request.Body)
	var responseItems []interface{}

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var action map[string]interface{}
		if err := json.Unmarshal(line, &action); err != nil {
			continue
		}

		var op string
		var meta map[string]interface{}
		for k, v := range action {
			op = k
			if m, ok := v.(map[string]interface{}); ok {
				meta = m
			}
			break
		}

		indexName, _ := meta["_index"].(string)
		if indexName == "" {
			indexName = c.Param("index")
		}
		id, _ := meta["_id"].(string)

		idx, err := s.getOrCreateIndex(indexName)
		if err != nil {
			responseItems = append(responseItems, gin.H{op: gin.H{"_index": indexName, "_id": id, "status": 400, "error": err.Error()}})
			continue
		}

		switch op {
		case "index", "create":
			if scanner.Scan() {
				var doc map[string]interface{}
				json.Unmarshal(scanner.Bytes(), &doc)

				status := 201
				var errResp gin.H
				if op == "create" {
					existing, _ := idx.Get(id)
					if existing != nil {
						status = 409
						errResp = gin.H{
							"type":   "version_conflict_engine_exception",
							"reason": "document already exists",
							"index":  indexName,
							"id":     id,
						}
					}
				}

				if status == 201 {
					err := idx.Index(id, doc)
					if err != nil {
						status = 500
					}
				}

				resp := gin.H{"_index": indexName, "_id": id, "status": status}
				if errResp != nil {
					resp["error"] = errResp
				}
				responseItems = append(responseItems, gin.H{op: resp})
			}
		case "delete":
			err := idx.Delete(id)
			status := 200
			if err != nil {
				status = 404
			}
			responseItems = append(responseItems, gin.H{op: gin.H{"_index": indexName, "_id": id, "status": status}})
		case "update":
			if scanner.Scan() {
				var updateBody struct {
					Doc         map[string]interface{} `json:"doc"`
					Upsert      map[string]interface{} `json:"upsert"`
					DocAsUpsert bool                   `json:"doc_as_upsert"`
				}
				json.Unmarshal(scanner.Bytes(), &updateBody)

				doc, _ := idx.Get(id)
				if doc == nil {
					if updateBody.DocAsUpsert {
						doc = updateBody.Doc
					} else if updateBody.Upsert != nil {
						doc = updateBody.Upsert
					}
				} else if updateBody.Doc != nil {
					for k, v := range updateBody.Doc {
						doc[k] = v
					}
				}

				status := 200
				if doc != nil {
					if err := idx.Index(id, doc); err != nil {
						status = 500
					}
				} else {
					status = 404
				}
				responseItems = append(responseItems, gin.H{op: gin.H{"_index": indexName, "_id": id, "status": status}})
			}
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
				results = append(results, gin.H{"_index": indexName, "_id": id, "found": false})
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
				results = append(results, gin.H{"_index": n, "_id": d.ID, "found": false})
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
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var header map[string]interface{}
		json.Unmarshal(line, &header)
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
			responses = append(responses, gin.H{
				"took":      0,
				"timed_out": false,
				"_shards": gin.H{
					"total":      1,
					"successful": 1,
					"skipped":    0,
					"failed":     0,
				},
				"hits": gin.H{
					"total": gin.H{"value": 0, "relation": "eq"},
					"hits":  []interface{}{},
				},
				"aggregations": gin.H{},
			})
			continue
		}

		req := s.parseSearchRequest(body)
		res, _ := idx.Search(req)

		hits := []gin.H{}
		total := 0
		if res != nil {
			total = int(res.Total)
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
			"took":      0,
			"timed_out": false,
			"_shards": gin.H{
				"total":      1,
				"successful": 1,
				"skipped":    0,
				"failed":     0,
			},
			"hits": gin.H{
				"total": gin.H{"value": total, "relation": "eq"},
				"hits":  hits,
			},
			"aggregations": gin.H{},
		})
	}

	c.JSON(http.StatusOK, gin.H{"responses": responses})
}

func (s *Service) parseSearchRequest(body map[string]interface{}) *bleve.SearchRequest {
	var bleveQuery query.Query
	bleveQuery = bleve.NewMatchAllQuery()

	if q, ok := body["query"].(map[string]interface{}); ok {
		if _, ok := q["match_all"].(map[string]interface{}); ok {
			bleveQuery = bleve.NewMatchAllQuery()
		} else if qs, ok := q["query_string"].(map[string]interface{}); ok {
			if qss, ok := qs["query"].(string); ok {
				if qss == "*" {
					bleveQuery = bleve.NewMatchAllQuery()
				} else {
					bleveQuery = bleve.NewQueryStringQuery(qss)
				}
			}
		} else if m, ok := q["match"].(map[string]interface{}); ok {
			// Just take first match field for now
			for field, v := range m {
				var matchStr string
				if vs, ok := v.(string); ok {
					matchStr = vs
				} else if vm, ok := v.(map[string]interface{}); ok {
					matchStr, _ = vm["query"].(string)
				}

				if matchStr != "" {
					mq := bleve.NewMatchQuery(matchStr)
					mq.SetField(field)
					bleveQuery = mq
				}
				break
			}
		} else if b, ok := q["bool"].(map[string]interface{}); ok {
			boolQuery := bleve.NewBooleanQuery()

			if must, ok := b["must"]; ok {
				if list, ok := must.([]interface{}); ok {
					for _, item := range list {
						if itemMap, ok := item.(map[string]interface{}); ok {
							subQ := s.parseSearchRequest(map[string]interface{}{"query": itemMap})
							boolQuery.AddMust(subQ.Query)
						}
					}
				}
			}
			if filter, ok := b["filter"]; ok {
				if list, ok := filter.([]interface{}); ok {
					for _, item := range list {
						if itemMap, ok := item.(map[string]interface{}); ok {
							subQ := s.parseSearchRequest(map[string]interface{}{"query": itemMap})
							boolQuery.AddMust(subQ.Query)
						}
					}
				}
			}
			bleveQuery = boolQuery
		}
	}

	req := bleve.NewSearchRequest(bleveQuery)
	req.Fields = []string{"_source"}

	if size, ok := body["size"].(float64); ok {
		req.Size = int(size)
	}
	if from, ok := body["from"].(float64); ok {
		req.From = int(from)
	}

	return req
}

func (s *Service) Get(c *gin.Context) {
	name := c.Param("index")
	id := c.Param("id")

	idx := s.manager.GetIndex(name)
	if idx == nil {
		c.JSON(http.StatusNotFound, gin.H{
			"_index": name,
			"_id":    id,
			"found":  false,
		})
		return
	}

	doc, err := idx.Get(id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if doc == nil {
		c.JSON(http.StatusNotFound, gin.H{
			"_index": name,
			"_id":    id,
			"found":  false,
		})
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

	idx := s.manager.GetIndex(name)
	if idx == nil {
		c.JSON(http.StatusOK, gin.H{
			"took": 0,
			"hits": gin.H{
				"total": gin.H{"value": 0, "relation": "eq"},
				"hits":  []interface{}{},
			},
		})
		return
	}

	var req *bleve.SearchRequest
	if c.Request.Method == "GET" {
		queryStr := c.Query("q")
		var q query.Query
		if queryStr == "" || queryStr == "*" {
			q = bleve.NewMatchAllQuery()
		} else {
			q = bleve.NewQueryStringQuery(queryStr)
		}
		req = bleve.NewSearchRequest(q)
		req.Fields = []string{"_source"}
	} else {
		var body map[string]interface{}
		if err := c.BindJSON(&body); err != nil {
			// Handled below if body is nil
		}
		req = s.parseSearchRequest(body)
	}

	var res *bleve.SearchResult
	var err error

	if c.Query("local") == "true" {
		res, err = idx.LocalSearch(req)
	} else {
		res, err = idx.Search(req)
	}

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	hits := []gin.H{}
	total := 0
	if res != nil {
		total = int(res.Total)
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
	}

	c.JSON(http.StatusOK, gin.H{
		"took":      res.Took.Milliseconds(),
		"timed_out": false,
		"_shards": gin.H{
			"total":      1,
			"successful": 1,
			"skipped":    0,
			"failed":     0,
		},
		"hits": gin.H{
			"total": gin.H{
				"value":    total,
				"relation": "eq",
			},
			"hits": hits,
		},
		"aggregations": gin.H{},
	})
}
