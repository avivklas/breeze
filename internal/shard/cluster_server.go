package shard

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
)

type ClusterServer struct {
	manager *Manager
	addr    string
}

func NewClusterServer(manager *Manager, addr string) *ClusterServer {
	return &ClusterServer{
		manager: manager,
		addr:    addr,
	}
}

func (s *ClusterServer) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	fmt.Printf("Cluster server listening on %s\n", s.addr)

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				continue
			}
			go s.handleConn(conn)
		}
	}()
	return nil
}

func (s *ClusterServer) handleConn(conn net.Conn) {
	defer conn.Close()
	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)

	for {
		var req InternalRequest
		if err := decoder.Decode(&req); err != nil {
			if err != io.EOF {
				fmt.Printf("Cluster server decode error: %v\n", err)
			}
			break
		}

		resp := s.handleRequest(req)
		if err := encoder.Encode(resp); err != nil {
			fmt.Printf("Cluster server encode error: %v\n", err)
			break
		}
	}
}

func (s *ClusterServer) handleRequest(req InternalRequest) InternalResponse {
	var resp InternalResponse

	idx := s.manager.GetIndex(req.IndexName)
	if idx == nil && req.Type != ReqCreateIndex {
		var err error
		idx, err = s.manager.OpenIndex(req.IndexName)
		if err != nil {
			resp.Err = err.Error()
			return resp
		}
	}

	switch req.Type {
	case ReqIndex:
		err := idx.Index(req.ID, req.Data)
		if err != nil {
			resp.Err = err.Error()
		}
	case ReqBatchIndex:
		err := idx.BatchIndex(req.BatchIDs, req.BatchDocs)
		if err != nil {
			resp.Err = err.Error()
		}
	case ReqGet:
		data, err := idx.Get(req.ID)
		if err != nil {
			resp.Err = err.Error()
		} else {
			resp.Data = data
		}
	case ReqDelete:
		err := idx.Delete(req.ID)
		if err != nil {
			resp.Err = err.Error()
		}
	case ReqSearch:
		res, err := idx.LocalSearch(req.SearchReq)
		if err != nil {
			resp.Err = err.Error()
		} else {
			resp.SearchResult = res
		}
	case ReqCreateIndex:
		_, err := s.manager.CreateIndex(req.IndexName, req.NumShards, false)
		if err != nil {
			resp.Err = err.Error()
		}
	}

	return resp
}
