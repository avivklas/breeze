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

type Manager struct {
	shards    []*store.Store
	numShards int
	path      string
	Mapping   *mapping.Mapping
	mu        sync.RWMutex
}

func NewManager(path string, numShards int) (*Manager, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	m := &Manager{
		numShards: numShards,
		path:      path,
		shards:    make([]*store.Store, numShards),
		Mapping:   mapping.NewMapping(),
	}

	// Load mapping if exists
	mappingPath := filepath.Join(path, "mapping.json")
	if data, err := os.ReadFile(mappingPath); err == nil {
		json.Unmarshal(data, &m.Mapping.Fields)
	}

	for i := 0; i < numShards; i++ {
		shardPath := filepath.Join(path, fmt.Sprintf("shard_%d", i))
		s, err := store.Open(shardPath, true)
		if err != nil {
			return nil, err
		}
		m.shards[i] = s
	}

	return m, nil
}

func (m *Manager) saveMapping() {
	mappingPath := filepath.Join(m.path, "mapping.json")
	m.Mapping.Mu.RLock()
	data, _ := json.Marshal(m.Mapping.Fields)
	m.Mapping.Mu.RUnlock()
	os.WriteFile(mappingPath, data, 0644)
}

func (m *Manager) getShardID(id string) int {
	hash := crc32.ChecksumIEEE([]byte(id))
	return int(hash % uint32(m.numShards))
}

func (m *Manager) Index(id string, data map[string]interface{}) error {
	if m.Mapping.Sniff(data) {
		m.saveMapping()
	}
	shardID := m.getShardID(id)
	return m.shards[shardID].Index(id, data)
}

func (m *Manager) Get(id string) (map[string]interface{}, error) {
	shardID := m.getShardID(id)
	return m.shards[shardID].Get(id)
}

func (m *Manager) Delete(id string) error {
	shardID := m.getShardID(id)
	return m.shards[shardID].Delete(id)
}

func (m *Manager) Search(req *bleve.SearchRequest) (*bleve.SearchResult, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var wg sync.WaitGroup
	results := make([]*bleve.SearchResult, m.numShards)
	errors := make([]error, m.numShards)

	for i := 0; i < m.numShards; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			res, err := m.shards[idx].Search(req)
			results[idx] = res
			errors[idx] = err
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

func (m *Manager) Close() error {
	m.saveMapping()
	for _, s := range m.shards {
		if err := s.Close(); err != nil {
			return err
		}
	}
	return nil
}

type Metadata struct {
	NumShards int      `json:"num_shards"`
	Shards    []string `json:"shards"`
}

func (m *Manager) GetMetadata() Metadata {
	md := Metadata{
		NumShards: m.numShards,
		Shards:    make([]string, m.numShards),
	}
	for i := 0; i < m.numShards; i++ {
		md.Shards[i] = fmt.Sprintf("shard_%d", i)
	}
	return md
}
