package shard

import (
	"breeze/internal/cluster"
	bmapping "breeze/internal/mapping"
	"breeze/internal/store"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/blevesearch/bleve/v2"
)

type Index struct {
	Name      string
	Shards    map[int]*store.Store
	numShards int
	path      string
	Mapping   *bmapping.Mapping
	Cluster   *cluster.Cluster
	Forwarder *Forwarder
	mu        sync.RWMutex
}

type IndexTemplate struct {
	IndexPatterns []string               `json:"index_patterns"`
	Settings      map[string]interface{} `json:"settings,omitempty"`
	Mappings      map[string]interface{} `json:"mappings,omitempty"`
	Template      *struct {
		Settings map[string]interface{} `json:"settings,omitempty"`
		Mappings map[string]interface{} `json:"mappings,omitempty"`
	} `json:"template,omitempty"`
	Priority int `json:"priority,omitempty"`
	Order    int `json:"order,omitempty"`
}

type Manager struct {
	indices          map[string]*Index
	templates        map[string]IndexTemplate
	basePath         string
	defaultNumShards int
	Cluster          *cluster.Cluster
	Forwarder        *Forwarder
	mu               sync.RWMutex
}

func NewManager(basePath string, defaultNumShards int, c *cluster.Cluster) (*Manager, error) {
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, err
	}

	m := &Manager{
		indices:          make(map[string]*Index),
		templates:        make(map[string]IndexTemplate),
		basePath:         basePath,
		defaultNumShards: defaultNumShards,
		Cluster:          c,
		Forwarder:        NewForwarder(),
	}

	// Load templates
	templatePath := filepath.Join(basePath, "_templates")
	os.MkdirAll(templatePath, 0755)
	tEntries, _ := os.ReadDir(templatePath)
	for _, entry := range tEntries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".json" {
			data, err := os.ReadFile(filepath.Join(templatePath, entry.Name()))
			if err == nil {
				var t IndexTemplate
				if err := json.Unmarshal(data, &t); err == nil {
					name := strings.TrimSuffix(entry.Name(), ".json")
					m.templates[name] = t
				}
			}
		}
	}

	// Load existing indices
	entries, err := os.ReadDir(basePath)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			_, err := m.OpenIndex(entry.Name())
			if err != nil {
				fmt.Printf("Failed to open index %s: %v\n", entry.Name(), err)
			}
		}
	}

	return m, nil
}

func (m *Manager) OpenIndex(name string) (*Index, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if idx, ok := m.indices[name]; ok {
		return idx, nil
	}

	indexPath := filepath.Join(m.basePath, name)

	numShards := m.defaultNumShards
	// Simple discovery of shard count by looking for existing shard directories
	// (Even if we don't own them all, they might exist if a node was repurposed)
	maxShardFound := -1
	entries, _ := os.ReadDir(indexPath)
	for _, entry := range entries {
		var sID int
		if n, _ := fmt.Sscanf(entry.Name(), "shard_%d", &sID); n == 1 {
			if sID > maxShardFound {
				maxShardFound = sID
			}
		}
	}
	if maxShardFound >= 0 {
		numShards = maxShardFound + 1
	}

	idx := &Index{
		Name:      name,
		numShards: numShards,
		path:      indexPath,
		Shards:    make(map[int]*store.Store),
		Mapping:   bmapping.NewMapping(),
		Cluster:   m.Cluster,
		Forwarder: m.Forwarder,
	}

	// Load mapping if exists
	mappingPath := filepath.Join(indexPath, "mapping.json")
	if data, err := os.ReadFile(mappingPath); err == nil {
		json.Unmarshal(data, &idx.Mapping.Fields)
	}

	// Only open shards owned by this node
	for i := 0; i < numShards; i++ {
		owner := m.Cluster.GetShardOwner(name, i, numShards)
		if m.Cluster.IsLocal(owner) {
			shardPath := filepath.Join(indexPath, fmt.Sprintf("shard_%d", i))
			s, err := store.Open(shardPath, true)
			if err != nil {
				return nil, err
			}
			idx.Shards[i] = s
		}
	}

	m.indices[name] = idx
	return idx, nil
}

func (m *Manager) PutTemplate(name string, t IndexTemplate) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.templates[name] = t
	templatePath := filepath.Join(m.basePath, "_templates", name+".json")
	data, _ := json.Marshal(t)
	return os.WriteFile(templatePath, data, 0644)
}

func (m *Manager) GetTemplate(name string) (IndexTemplate, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.templates[name]
	return t, ok
}

func (m *Manager) CreateIndex(name string, numShards int, forward bool) (*Index, error) {
	m.mu.Lock()
	if _, ok := m.indices[name]; ok {
		m.mu.Unlock()
		return m.indices[name], nil
	}
	m.mu.Unlock()

	// Apply templates
	var appliedTemplate *IndexTemplate
	m.mu.RLock()
	for tName, t := range m.templates {
		for _, pattern := range t.IndexPatterns {
			matched, _ := filepath.Match(pattern, name)
			if matched {
				fmt.Printf("Applying template %s to index %s\n", tName, name)
				appliedTemplate = &t
				break
			}
		}
		if appliedTemplate != nil {
			break
		}
	}
	m.mu.RUnlock()

	if numShards <= 0 && appliedTemplate != nil {
		settings := appliedTemplate.Settings
		if appliedTemplate.Template != nil && appliedTemplate.Template.Settings != nil {
			settings = appliedTemplate.Template.Settings
		}

		if s, ok := settings["number_of_shards"].(float64); ok {
			numShards = int(s)
		} else if s, ok := settings["index"].(map[string]interface{}); ok {
			if ns, ok := s["number_of_shards"].(float64); ok {
				numShards = int(ns)
			}
		}
	}

	if numShards <= 0 {
		numShards = m.defaultNumShards
	}

	indexPath := filepath.Join(m.basePath, name)
	if err := os.MkdirAll(indexPath, 0755); err != nil {
		return nil, err
	}

	idx, err := m.OpenIndex(name)
	if err != nil {
		return nil, err
	}

	if appliedTemplate != nil {
		mappings := appliedTemplate.Mappings
		if appliedTemplate.Template != nil && appliedTemplate.Template.Mappings != nil {
			mappings = appliedTemplate.Template.Mappings
		}

		if props, ok := mappings["properties"].(map[string]interface{}); ok {
			for k, v := range props {
				if vm, ok := v.(map[string]interface{}); ok {
					tStr, _ := vm["type"].(string)
					var detected bmapping.FieldType
					switch tStr {
					case "text", "keyword":
						detected = bmapping.TypeString
					case "double", "float", "integer", "long":
						detected = bmapping.TypeNumber
					case "boolean":
						detected = bmapping.TypeBoolean
					default:
						detected = bmapping.TypeObject
					}
					idx.Mapping.Mu.Lock()
					idx.Mapping.Fields[k] = detected
					idx.Mapping.Mu.Unlock()
				}
			}
			idx.saveMapping()
		}
	}

	if forward {
		for _, node := range m.Cluster.Nodes {
			if !m.Cluster.IsLocal(node) {
				m.Forwarder.ForwardCreateIndex(node, name, numShards)
			}
		}
	}

	return idx, nil
}

func (m *Manager) GetIndex(name string) *Index {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.indices[name]
}

func (m *Manager) ListIndices() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var names []string
	for name := range m.indices {
		names = append(names, name)
	}
	return names
}

func (idx *Index) saveMapping() {
	// Only save mapping if we are a "leader" or just let every node save its sniffed view.
	// For now, let every node save its local view of the mapping.
	mappingPath := filepath.Join(idx.path, "mapping.json")
	idx.Mapping.Mu.RLock()
	data, _ := json.Marshal(idx.Mapping.Fields)
	idx.Mapping.Mu.RUnlock()
	os.WriteFile(mappingPath, data, 0644)
}

func (idx *Index) saveMappingLocked() {
	mappingPath := filepath.Join(idx.path, "mapping.json")
	data, _ := json.Marshal(idx.Mapping.Fields)
	os.WriteFile(mappingPath, data, 0644)
}

func (idx *Index) UpdateMapping(props map[string]interface{}) {
	idx.Mapping.Mu.Lock()
	defer idx.Mapping.Mu.Unlock()

	for k, v := range props {
		if vm, ok := v.(map[string]interface{}); ok {
			tStr, _ := vm["type"].(string)
			var detected bmapping.FieldType
			switch tStr {
			case "text", "keyword":
				detected = bmapping.TypeString
			case "double", "float", "integer", "long":
				detected = bmapping.TypeNumber
			case "boolean":
				detected = bmapping.TypeBoolean
			default:
				detected = bmapping.TypeObject
			}
			idx.Mapping.Fields[k] = detected
		}
	}
	idx.saveMappingLocked()
}

func (idx *Index) GetShardID(id string) int {
	hash := crc32.ChecksumIEEE([]byte(id))
	return int(hash % uint32(idx.numShards))
}

func (idx *Index) Index(id string, data map[string]interface{}) error {
	if idx.Mapping.Sniff(data) {
		idx.saveMapping()
	}
	shardID := idx.GetShardID(id)
	owner := idx.Cluster.GetShardOwner(idx.Name, shardID, idx.numShards)

	if idx.Cluster.IsLocal(owner) {
		return idx.Shards[shardID].Index(id, data)
	}
	return idx.Forwarder.ForwardIndex(owner, idx.Name, id, data)
}

func (idx *Index) BatchIndex(ids []string, data []map[string]interface{}) error {
	// Split batch into local vs remote groups
	nodeGroupsIds := make(map[string][]string)
	nodeGroupsData := make(map[string][]map[string]interface{})

	for i, id := range ids {
		d := data[i]
		if idx.Mapping.Sniff(d) {
			idx.saveMapping()
		}
		shardID := idx.GetShardID(id)
		owner := idx.Cluster.GetShardOwner(idx.Name, shardID, idx.numShards)

		nodeGroupsIds[owner.ID] = append(nodeGroupsIds[owner.ID], id)
		nodeGroupsData[owner.ID] = append(nodeGroupsData[owner.ID], d)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var finalErr error

	for nodeID, gIds := range nodeGroupsIds {
		nodeID := nodeID
		gIds := gIds
		gData := nodeGroupsData[nodeID]

		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			node, _ := idx.Cluster.GetNodeByID(nodeID)
			if idx.Cluster.IsLocal(node) {
				// Internal batch split by shard
				shardGroupsIds := make(map[int][]string)
				shardGroupsData := make(map[int][]map[string]interface{})
				for j, id := range gIds {
					sID := idx.GetShardID(id)
					shardGroupsIds[sID] = append(shardGroupsIds[sID], id)
					shardGroupsData[sID] = append(shardGroupsData[sID], gData[j])
				}
				for sID, sIds := range shardGroupsIds {
					err = idx.Shards[sID].BatchIndex(sIds, shardGroupsData[sID])
					if err != nil {
						break
					}
				}
			} else {
				err = idx.Forwarder.ForwardBatchIndex(node, idx.Name, gIds, gData)
			}

			if err != nil {
				mu.Lock()
				finalErr = err
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return finalErr
}

func (idx *Index) Get(id string) (map[string]interface{}, error) {
	shardID := idx.GetShardID(id)
	owner := idx.Cluster.GetShardOwner(idx.Name, shardID, idx.numShards)

	if idx.Cluster.IsLocal(owner) {
		return idx.Shards[shardID].Get(id)
	}
	return idx.Forwarder.ForwardGet(owner, idx.Name, id)
}

func (idx *Index) Delete(id string) error {
	shardID := idx.GetShardID(id)
	owner := idx.Cluster.GetShardOwner(idx.Name, shardID, idx.numShards)

	if idx.Cluster.IsLocal(owner) {
		return idx.Shards[shardID].Delete(id)
	}
	return idx.Forwarder.ForwardDelete(owner, idx.Name, id)
}

func (idx *Index) Search(req *bleve.SearchRequest) (*bleve.SearchResult, error) {
	// Distributed Search: Fan-out to all nodes in the cluster
	var wg sync.WaitGroup
	var mu sync.Mutex
	var finalResult *bleve.SearchResult
	var finalErr error

	for _, node := range idx.Cluster.Nodes {
		node := node
		wg.Add(1)
		go func() {
			defer wg.Done()
			var res *bleve.SearchResult
			var err error

			if idx.Cluster.IsLocal(node) {
				res, err = idx.LocalSearch(req)
			} else {
				res, err = idx.Forwarder.ForwardSearch(node, idx.Name, req)
			}

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				finalErr = err
				return
			}
			if finalResult == nil {
				finalResult = res
			} else {
				finalResult.Merge(res)
			}
		}()
	}
	wg.Wait()

	if finalErr != nil {
		return nil, finalErr
	}
	if finalResult == nil {
		return &bleve.SearchResult{}, nil
	}
	return finalResult, nil
}

func (idx *Index) LocalSearch(req *bleve.SearchRequest) (*bleve.SearchResult, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var wg sync.WaitGroup
	var mu sync.Mutex
	results := make(map[int]*bleve.SearchResult)
	errors := make(map[int]error)

	for sID, s := range idx.Shards {
		sID := sID
		s := s
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := s.Search(req)
			mu.Lock()
			results[sID] = res
			errors[sID] = err
			mu.Unlock()
		}()
	}
	wg.Wait()

	var finalResult *bleve.SearchResult
	for sID, res := range results {
		if errors[sID] != nil {
			return nil, errors[sID]
		}
		if res == nil {
			continue
		}
		if finalResult == nil {
			finalResult = res
		} else {
			finalResult.Merge(res)
		}
	}
	if finalResult == nil {
		return &bleve.SearchResult{}, nil
	}
	return finalResult, nil
}

func (idx *Index) Close() error {
	idx.saveMapping()
	for _, s := range idx.Shards {
		if err := s.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, idx := range m.indices {
		idx.Close()
	}
	return nil
}

func (idx *Index) GetMetadata() Metadata {
	md := Metadata{
		NumShards: idx.numShards,
		Shards:    make([]string, idx.numShards),
	}
	for i := 0; i < idx.numShards; i++ {
		md.Shards[i] = fmt.Sprintf("shard_%d", i)
	}
	return md
}

type Metadata struct {
	NumShards int      `json:"num_shards"`
	Shards    []string `json:"shards"`
}
