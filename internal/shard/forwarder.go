package shard

import (
	"breeze/internal/cluster"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/blevesearch/bleve/v2"
)

type RequestType int

const (
	ReqIndex RequestType = iota
	ReqBatchIndex
	ReqGet
	ReqDelete
	ReqSearch
	ReqCreateIndex
)

type InternalRequest struct {
	Type      RequestType              `json:"type"`
	IndexName string                   `json:"index_name"`
	ID        string                   `json:"id,omitempty"`
	Data      map[string]interface{}   `json:"data,omitempty"`
	BatchIDs  []string                 `json:"batch_ids,omitempty"`
	BatchDocs []map[string]interface{} `json:"batch_docs,omitempty"`
	SearchReq *bleve.SearchRequest     `json:"search_req,omitempty"`
	NumShards int                      `json:"num_shards,omitempty"`
}

type InternalResponse struct {
	Data         map[string]interface{} `json:"data,omitempty"`
	SearchResult *bleve.SearchResult     `json:"search_result,omitempty"`
	Err          string                 `json:"err,omitempty"`
}

type nodeConn struct {
	conn    net.Conn
	encoder *json.Encoder
	decoder *json.Decoder
	mu      sync.Mutex
}

type Forwarder struct {
	mu    sync.Mutex
	conns map[string]*nodeConn
}

func NewForwarder() *Forwarder {
	return &Forwarder{
		conns: make(map[string]*nodeConn),
	}
}

func (f *Forwarder) getConn(node cluster.Node) (*nodeConn, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if nc, ok := f.conns[node.ID]; ok {
		return nc, nil
	}

	conn, err := net.Dial("tcp", node.Addr)
	if err != nil {
		return nil, err
	}
	nc := &nodeConn{
		conn:    conn,
		encoder: json.NewEncoder(conn),
		decoder: json.NewDecoder(conn),
	}
	f.conns[node.ID] = nc
	return nc, nil
}

func (f *Forwarder) call(node cluster.Node, req InternalRequest) (*InternalResponse, error) {
	nc, err := f.getConn(node)
	if err != nil {
		return nil, err
	}

	nc.mu.Lock()
	defer nc.mu.Unlock()

	nc.conn.SetDeadline(time.Now().Add(30 * time.Second))

	if err := nc.encoder.Encode(req); err != nil {
		f.mu.Lock()
		delete(f.conns, node.ID)
		f.mu.Unlock()
		nc.conn.Close()
		return nil, err
	}

	var resp InternalResponse
	if err := nc.decoder.Decode(&resp); err != nil {
		f.mu.Lock()
		delete(f.conns, node.ID)
		f.mu.Unlock()
		nc.conn.Close()
		return nil, err
	}

	if resp.Err != "" {
		return nil, fmt.Errorf("%s", resp.Err)
	}
	return &resp, nil
}

func (f *Forwarder) ForwardIndex(node cluster.Node, indexName, id string, data map[string]interface{}) error {
	_, err := f.call(node, InternalRequest{
		Type:      ReqIndex,
		IndexName: indexName,
		ID:        id,
		Data:      data,
	})
	return err
}

func (f *Forwarder) ForwardBatchIndex(node cluster.Node, indexName string, ids []string, data []map[string]interface{}) error {
	_, err := f.call(node, InternalRequest{
		Type:      ReqBatchIndex,
		IndexName: indexName,
		BatchIDs:  ids,
		BatchDocs: data,
	})
	return err
}

func (f *Forwarder) ForwardGet(node cluster.Node, indexName, id string) (map[string]interface{}, error) {
	resp, err := f.call(node, InternalRequest{
		Type:      ReqGet,
		IndexName: indexName,
		ID:        id,
	})
	if err != nil {
		return nil, err
	}
	return resp.Data, nil
}

func (f *Forwarder) ForwardDelete(node cluster.Node, indexName, id string) error {
	_, err := f.call(node, InternalRequest{
		Type:      ReqDelete,
		IndexName: indexName,
		ID:        id,
	})
	return err
}

func (f *Forwarder) ForwardCreateIndex(node cluster.Node, indexName string, numShards int) error {
	_, err := f.call(node, InternalRequest{
		Type:      ReqCreateIndex,
		IndexName: indexName,
		NumShards: numShards,
	})
	return err
}

func (f *Forwarder) ForwardSearch(node cluster.Node, indexName string, searchReq *bleve.SearchRequest) (*bleve.SearchResult, error) {
	resp, err := f.call(node, InternalRequest{
		Type:      ReqSearch,
		IndexName: indexName,
		SearchReq: searchReq,
	})
	if err != nil {
		return nil, err
	}
	return resp.SearchResult, nil
}
