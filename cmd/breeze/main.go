package main

import (
	"breeze/internal/api/elasticsearch"
	"breeze/internal/api/graphql"
	"breeze/internal/cluster"
	"breeze/internal/shard"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/spf13/cobra"
)

var (
	dbPath       string
	numShards    int
	port         int
	internalPort int
	publicAddr   string
	serverURL    string
	nodeID       string
	peers        []string
)

func main() {
	var rootCmd = &cobra.Command{Use: "breeze"}

	var startCmd = &cobra.Command{
		Use:   "start",
		Short: "Start the Breeze database server",
		Run: func(cmd *cobra.Command, args []string) {
			runServer()
		},
	}
	startCmd.Flags().StringVarP(&dbPath, "path", "p", "./data", "Path to database storage")
	startCmd.Flags().IntVarP(&numShards, "shards", "s", 5, "Number of shards")
	startCmd.Flags().IntVarP(&port, "port", "v", 8080, "Public API port")
	startCmd.Flags().IntVarP(&internalPort, "internal-port", "l", 9090, "Internal cluster port")
	startCmd.Flags().StringVar(&publicAddr, "public-addr", "127.0.0.1:8080", "Public address for discovery")
	startCmd.Flags().StringVarP(&nodeID, "node-id", "i", "node1", "Unique node ID")
	startCmd.Flags().StringSliceVar(&peers, "peers", []string{}, "Cluster peers (format: id=host:port)")

	var indexCmd = &cobra.Command{
		Use:   "index [id] [json]",
		Short: "Index a document",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			id := args[0]
			data := args[1]
			callIndex(id, data)
		},
	}
	indexCmd.Flags().StringVarP(&serverURL, "server", "u", "http://localhost:8080", "Server URL")

	var queryCmd = &cobra.Command{
		Use:   "query [q]",
		Short: "Query documents",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			query := args[0]
			callQuery(query)
		},
	}
	queryCmd.Flags().StringVarP(&serverURL, "server", "u", "http://localhost:8080", "Server URL")

	rootCmd.AddCommand(startCmd, indexCmd, queryCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func runServer() {
	c := cluster.NewCluster(nodeID, peers)
	manager, err := shard.NewManager(dbPath, numShards, c)
	if err != nil {
		log.Fatalf("Failed to initialize manager: %v", err)
	}
	defer manager.Close()

	// Start cluster server for internal lightweight communication
	clusterServer := shard.NewClusterServer(manager, fmt.Sprintf(":%d", internalPort))
	if err := clusterServer.Start(); err != nil {
		log.Fatalf("Failed to start cluster server: %v", err)
	}

	gqlService := graphql.NewService(manager)
	esService := elasticsearch.NewService(manager, publicAddr)

	r := gin.Default()
	r.POST("/graphql", gqlService.Handler())
	r.POST("/graphql/:index", gqlService.Handler())
	esService.RegisterHandlers(r)

	r.GET("/_metadata", func(c *gin.Context) {
		indices := manager.ListIndices()
		metadata := make(map[string]interface{})
		for _, name := range indices {
			idx := manager.GetIndex(name)
			if idx != nil {
				metadata[name] = idx.GetMetadata()
			}
		}
		c.JSON(200, metadata)
	})

	fmt.Printf("Breeze server %s starting on API port %d, Cluster port %d...\n", nodeID, port, internalPort)
	if err := r.Run(fmt.Sprintf(":%d", port)); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

func callIndex(id, data string) {
	url := fmt.Sprintf("%s/default/_doc/%s", serverURL, id)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer([]byte(data)))
	if err != nil {
		log.Fatalf("Failed to index: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	fmt.Println(string(body))
}

func callQuery(query string) {
	url := fmt.Sprintf("%s/default/_search?q=%s", serverURL, query)
	resp, err := http.Get(url)
	if err != nil {
		log.Fatalf("Failed to query: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	var prettyJSON bytes.Buffer
	if err := json.Indent(&prettyJSON, body, "", "  "); err == nil {
		fmt.Println(prettyJSON.String())
	} else {
		fmt.Println(string(body))
	}
}
