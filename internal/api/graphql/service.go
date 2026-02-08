package graphql

import (
	"breeze/internal/shard"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/gin-gonic/gin"
	"github.com/graphql-go/graphql"
)

type IndexService struct {
	index       *shard.Index
	schema      graphql.Schema
	mu          sync.RWMutex
	lastMapping int
}

type Service struct {
	manager       *shard.Manager
	indexServices map[string]*IndexService
	mu            sync.RWMutex
}

func NewService(m *shard.Manager) *Service {
	s := &Service{
		manager:       m,
		indexServices: make(map[string]*IndexService),
	}
	go s.watchIndices()
	return s
}

func (s *Service) watchIndices() {
	for {
		time.Sleep(5 * time.Second)
		names := s.manager.ListIndices()
		for _, name := range names {
			s.mu.RLock()
			_, ok := s.indexServices[name]
			s.mu.RUnlock()

			if !ok {
				idx := s.manager.GetIndex(name)
				if idx != nil {
					is := &IndexService{index: idx}
					is.rebuildSchema()
					s.mu.Lock()
					s.indexServices[name] = is
					s.mu.Unlock()
					go is.watchMapping()
				}
			}
		}
	}
}

func (is *IndexService) watchMapping() {
	for {
		time.Sleep(5 * time.Second)
		is.index.Mapping.Mu.RLock()
		currentFields := len(is.index.Mapping.Fields)
		is.index.Mapping.Mu.RUnlock()

		is.mu.RLock()
		lastFields := is.lastMapping
		is.mu.RUnlock()

		if currentFields != lastFields {
			fmt.Printf("Mapping changed for index %s, rebuilding GraphQL schema...\n", is.index.Name)
			is.rebuildSchema()
		}
	}
}

func (is *IndexService) rebuildSchema() error {
	docType := is.index.Mapping.BuildGraphQLType("Document")

	queryType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Query",
		Fields: graphql.Fields{
			"get": &graphql.Field{
				Type: docType,
				Args: graphql.FieldConfigArgument{
					"id": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.String)},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					id := p.Args["id"].(string)
					return is.index.Get(id)
				},
			},
			"search": &graphql.Field{
				Type: graphql.NewList(docType),
				Args: graphql.FieldConfigArgument{
					"query": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.String)},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					queryString := p.Args["query"].(string)
					q := bleve.NewQueryStringQuery(queryString)
					req := bleve.NewSearchRequest(q)
					req.Fields = []string{"_source"}
					res, err := is.index.Search(req)
					if err != nil {
						return nil, err
					}

					var results []map[string]interface{}
					for _, hit := range res.Hits {
						source := make(map[string]interface{})
						if s, ok := hit.Fields["_source"].(string); ok {
							json.Unmarshal([]byte(s), &source)
						}
						source["id"] = hit.ID
						results = append(results, source)
					}
					return results, nil
				},
			},
		},
	})

	mutationType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Mutation",
		Fields: graphql.Fields{
			"index": &graphql.Field{
				Type: graphql.String,
				Args: graphql.FieldConfigArgument{
					"id":   &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.String)},
					"json": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.String)},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					id := p.Args["id"].(string)
					jsonStr := p.Args["json"].(string)
					var data map[string]interface{}
					if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
						return nil, err
					}
					if err := is.index.Index(id, data); err != nil {
						return nil, err
					}
					return "ok", nil
				},
			},
			"delete": &graphql.Field{
				Type: graphql.String,
				Args: graphql.FieldConfigArgument{
					"id": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.String)},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					id := p.Args["id"].(string)
					if err := is.index.Delete(id); err != nil {
						return nil, err
					}
					return "ok", nil
				},
			},
		},
	})

	schema, err := graphql.NewSchema(graphql.SchemaConfig{
		Query:    queryType,
		Mutation: mutationType,
	})
	if err != nil {
		return err
	}

	is.mu.Lock()
	is.schema = schema
	is.index.Mapping.Mu.RLock()
	is.lastMapping = len(is.index.Mapping.Fields)
	is.index.Mapping.Mu.RUnlock()
	is.mu.Unlock()
	return nil
}

func (s *Service) Handler() gin.HandlerFunc {
	return func(c *gin.Context) {
		name := c.Param("index")
		if name == "" {
			name = "default"
		}

		s.mu.RLock()
		is, ok := s.indexServices[name]
		s.mu.RUnlock()

		if !ok {
			// Try to open it if it exists in manager but not in service
			idx := s.manager.GetIndex(name)
			if idx == nil {
				c.JSON(http.StatusNotFound, gin.H{"error": "index not found"})
				return
			}
			is = &IndexService{index: idx}
			is.rebuildSchema()
			s.mu.Lock()
			s.indexServices[name] = is
			s.mu.Unlock()
			go is.watchMapping()
		}

		var request struct {
			Query         string                 `json:"query"`
			OperationName string                 `json:"operationName"`
			Variables     map[string]interface{} `json:"variables"`
		}

		if err := c.BindJSON(&request); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		is.mu.RLock()
		schema := is.schema
		is.mu.RUnlock()

		result := graphql.Do(graphql.Params{
			Schema:         schema,
			RequestString:  request.Query,
			VariableValues: request.Variables,
			OperationName:  request.OperationName,
		})

		c.JSON(http.StatusOK, result)
	}
}
