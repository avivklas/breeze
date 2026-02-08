package cluster

import (
	"fmt"
	"strings"
)

type Node struct {
	ID   string
	Addr string
}

type Cluster struct {
	SelfID string
	Nodes  []Node
}

func NewCluster(selfID string, peers []string) *Cluster {
	c := &Cluster{
		SelfID: selfID,
		Nodes:  []Node{},
	}

	for _, p := range peers {
		parts := strings.Split(p, "=")
		if len(parts) == 2 {
			c.Nodes = append(c.Nodes, Node{ID: parts[0], Addr: parts[1]})
		}
	}
	return c
}

func (c *Cluster) GetShardOwner(indexName string, shardID int, totalShards int) Node {
	// Deterministic mapping: shardID % numNodes
	nodeIdx := shardID % len(c.Nodes)
	return c.Nodes[nodeIdx]
}

func (c *Cluster) IsLocal(node Node) bool {
	return node.ID == c.SelfID
}

func (c *Cluster) GetNodeByID(id string) (Node, error) {
	for _, n := range c.Nodes {
		if n.ID == id {
			return n, nil
		}
	}
	return Node{}, fmt.Errorf("node not found: %s", id)
}
