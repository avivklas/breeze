package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/tidwall/wal"
)

type Store struct {
	index bleve.Index
	log   *wal.Log
	path  string
	mu    sync.Mutex
	sync  bool
}

type Operation string

const (
	OpIndex  Operation = "INDEX"
	OpDelete Operation = "DELETE"
)

type LogEntry struct {
	Op   Operation              `json:"op"`
	ID   string                 `json:"id"`
	Data map[string]interface{} `json:"data,omitempty"`
}

func Open(path string, syncWrites bool) (*Store, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	blevePath := filepath.Join(path, "bleve")
	walPath := filepath.Join(path, "wal")

	var index bleve.Index
	var err error

	if _, err := os.Stat(blevePath); os.IsNotExist(err) {
		indexMapping := GetDefaultMapping()
		index, err = bleve.New(blevePath, indexMapping)
	} else {
		index, err = bleve.Open(blevePath)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to open bleve index: %w", err)
	}

	log, err := wal.Open(walPath, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open wal: %w", err)
	}

	s := &Store{
		index: index,
		log:   log,
		path:  path,
		sync:  syncWrites,
	}

	if err := s.replay(); err != nil {
		return nil, fmt.Errorf("failed to replay wal: %w", err)
	}

	return s, nil
}

func (s *Store) replay() error {
	lastIndex, err := s.log.LastIndex()
	if err != nil {
		return err
	}

	if lastIndex == 0 {
		return nil
	}

	firstIndex, err := s.log.FirstIndex()
	if err != nil {
		return err
	}

	for i := firstIndex; i <= lastIndex; i++ {
		data, err := s.log.Read(i)
		if err != nil {
			return err
		}
		var entry LogEntry
		if err := json.Unmarshal(data, &entry); err != nil {
			continue
		}

		switch entry.Op {
		case OpIndex:
			s.index.Index(entry.ID, entry.Data)
		case OpDelete:
			s.index.Delete(entry.ID)
		}
	}
	return nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if err := s.index.Close(); err != nil {
		return err
	}
	return s.log.Close()
}

func (s *Store) Index(id string, data map[string]interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Add _source field for full document retrieval
	sourceBytes, _ := json.Marshal(data)
	data["_source"] = string(sourceBytes)

	entry := LogEntry{
		Op:   OpIndex,
		ID:   id,
		Data: data,
	}
	
	entryBytes, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	lastIndex, err := s.log.LastIndex()
	if err != nil {
		return err
	}

	if err := s.log.Write(lastIndex+1, entryBytes); err != nil {
		return err
	}

	return s.index.Index(id, data)
}

func (s *Store) BatchIndex(ids []string, data []map[string]interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	batch := s.index.NewBatch()
	for i, id := range ids {
		d := data[i]
		sourceBytes, _ := json.Marshal(d)
		d["_source"] = string(sourceBytes)

		entry := LogEntry{
			Op:   OpIndex,
			ID:   id,
			Data: d,
		}
		entryBytes, _ := json.Marshal(entry)
		lastIndex, _ := s.log.LastIndex()
		s.log.Write(lastIndex+1, entryBytes)

		batch.Index(id, d)
	}

	return s.index.Batch(batch)
}

func (s *Store) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry := LogEntry{
		Op: OpDelete,
		ID: id,
	}
	
	entryBytes, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	lastIndex, err := s.log.LastIndex()
	if err != nil {
		return err
	}

	if err := s.log.Write(lastIndex+1, entryBytes); err != nil {
		return err
	}

	return s.index.Delete(id)
}

func (s *Store) Get(id string) (map[string]interface{}, error) {
    query := bleve.NewDocIDQuery([]string{id})
    searchRequest := bleve.NewSearchRequest(query)
    searchRequest.Fields = []string{"_source"}
    
    res, err := s.index.Search(searchRequest)
    if err != nil {
        return nil, err
    }
    
    if res.Total == 0 {
        return nil, nil
    }

    sourceStr, ok := res.Hits[0].Fields["_source"].(string)
    if !ok {
        return nil, fmt.Errorf("_source field not found or not a string")
    }

    var result map[string]interface{}
    err = json.Unmarshal([]byte(sourceStr), &result)
    return result, err
}

func (s *Store) Search(req *bleve.SearchRequest) (*bleve.SearchResult, error) {
	return s.index.Search(req)
}

func GetDefaultMapping() mapping.IndexMapping {
    m := bleve.NewIndexMapping()
    sourceFieldMapping := bleve.NewTextFieldMapping()
    sourceFieldMapping.Store = true
    sourceFieldMapping.Index = false
    m.DefaultMapping.AddFieldMappingsAt("_source", sourceFieldMapping)
    return m
}
