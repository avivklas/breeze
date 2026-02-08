package shard

import (
	"breeze/internal/mapping"
	"breeze/internal/store"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"

	"github.com/blevesearch/bleve/v2"
)

type Index struct {
	Name      string
	shards    []*store.Store
	numShards int
	path      string
	Mapping   *mapping.Mapping
	mu        sync.RWMutex
}

type Manager struct {
	indices          map[string]*Index
	basePath         string
	defaultNumShards int
	mu               sync.RWMutex
}

func NewManager(basePath string, defaultNumShards int) (*Manager, error) {
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, err
	}

	m := &Manager{
		indices:          make(map[string]*Index),
		basePath:         basePath,
		defaultNumShards: defaultNumShards,
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
	// We need to know how many shards it has. 
	// For now, we'll look for shard_n directories.
	// In a real DB, we'd have a metadata file.
	
	numShards := m.defaultNumShards
	// Simple discovery of shard count
	for i := 0; ; i++ {
		shardPath := filepath.Join(indexPath, fmt.Sprintf("shard_%d", i))
		if _, err := os.Stat(shardPath); os.IsNotExist(err) {
			if i > 0 {
				numShards = i
			}
			break
		}
	}

	idx := &Index{
		Name:      name,
		numShards: numShards,
		path:      indexPath,
		shards:    make([]*store.Store, numShards),
		Mapping:   mapping.NewMapping(),
	}

	// Load mapping if exists
	mappingPath := filepath.Join(indexPath, "mapping.json")
	if data, err := os.ReadFile(mappingPath); err == nil {
		json.Unmarshal(data, &idx.Mapping.Fields)
	}

	for i := 0; i < numShards; i++ {
		shardPath := filepath.Join(indexPath, fmt.Sprintf("shard_%d", i))
		s, err := store.Open(shardPath, true)
		if err != nil {
			return nil, err
		}
		idx.shards[i] = s
	}

	m.indices[name] = idx
	return idx, nil
}

func (m *Manager) CreateIndex(name string, numShards int) (*Index, error) {
	m.mu.Lock()
	if _, ok := m.indices[name]; ok {
		m.mu.Unlock()
		return nil, fmt.Errorf("index %s already exists", name)
	}
	m.mu.Unlock()

	if numShards <= 0 {
		numShards = m.defaultNumShards
	}

	indexPath := filepath.Join(m.basePath, name)
	if err := os.MkdirAll(indexPath, 0755); err != nil {
		return nil, err
	}

	return m.OpenIndex(name)
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
	mappingPath := filepath.Join(idx.path, "mapping.json")
	idx.Mapping.Mu.RLock()
	data, _ := json.Marshal(idx.Mapping.Fields)
	idx.Mapping.Mu.RUnlock()
	os.WriteFile(mappingPath, data, 0644)
}

func (idx *Index) getShardID(id string) int {
	hash := crc32.ChecksumIEEE([]byte(id))
	return int(hash % uint32(idx.numShards))
}

func (idx *Index) Index(id string, data map[string]interface{}) error {
	if idx.Mapping.Sniff(data) {
		idx.saveMapping()
	}
	shardID := idx.getShardID(id)
	return idx.shards[shardID].Index(id, data)
}

func (idx *Index) BatchIndex(ids []string, data []map[string]interface{}) error {
	shardGroupsIds := make([][]string, idx.numShards)
	shardGroupsData := make([][]map[string]interface{}, idx.numShards)

	for i, id := range ids {
		d := data[i]
		if idx.Mapping.Sniff(d) {
			idx.saveMapping()
		}
		shardID := idx.getShardID(id)
		shardGroupsIds[shardID] = append(shardGroupsIds[shardID], id)
		shardGroupsData[shardID] = append(shardGroupsData[shardID], d)
	}

	var wg sync.WaitGroup
	errors := make([]error, idx.numShards)
	for i := 0; i < idx.numShards; i++ {
		if len(shardGroupsIds[i]) == 0 {
			continue
		}
		wg.Add(1)
		go func(i_ int) {
			defer wg.Done()
			errors[i_] = idx.shards[i_].BatchIndex(shardGroupsIds[i_], shardGroupsData[i_])
		}(i)
	}
	wg.Wait()

	for _, err := range errors {
		if err != nil {
			return err
		}
	}
	return nil
}

func (idx *Index) Get(id string) (map[string]interface{}, error) {
	shardID := idx.getShardID(id)
	return idx.shards[shardID].Get(id)
}

func (idx *Index) Delete(id string) error {
	shardID := idx.getShardID(id)
	return idx.shards[shardID].Delete(id)
}

func (idx *Index) Search(req *bleve.SearchRequest) (*bleve.SearchResult, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var wg sync.WaitGroup
	results := make([]*bleve.SearchResult, idx.numShards)
	errors := make([]error, idx.numShards)

	for i := 0; i < idx.numShards; i++ {
		wg.Add(1)
		go func(i_ int) {
			defer wg.Done()
			res, err := idx.shards[i_].Search(req)
			results[i_] = res
			errors[i_] = err
		}(i)
	}
	wg.Wait()

	var finalResult *bleve.SearchResult
	for i, res := range results {
		if errors[i] != nil {
			return nil, errors[i]
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
	for _, s := range idx.shards {
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
