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

type Service struct {
	manager     *shard.Manager
	schema      graphql.Schema
	mu          sync.RWMutex
	lastMapping int // Number of fields in mapping
}

func NewService(m *shard.Manager) (*Service, error) {
	s := &Service{manager: m}
	if err := s.rebuildSchema(); err != nil {
		return nil, err
	}
	// Background schema rebuilder if mapping changes
	go s.watchMapping()
	return s, nil
}

func (s *Service) watchMapping() {
	for {
		time.Sleep(5 * time.Second)
		s.manager.Mapping.Mu.RLock()
		currentFields := len(s.manager.Mapping.Fields)
		s.manager.Mapping.Mu.RUnlock()

		s.mu.RLock()
		lastFields := s.lastMapping
		s.mu.RUnlock()

		if currentFields != lastFields {
			fmt.Println("Mapping changed, rebuilding GraphQL schema...")
			s.rebuildSchema()
		}
	}
}

func (s *Service) rebuildSchema() error {
	docType := s.manager.Mapping.BuildGraphQLType("Document")

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
					return s.manager.Get(id)
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
					res, err := s.manager.Search(req)
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
					if err := s.manager.Index(id, data); err != nil {
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
					if err := s.manager.Delete(id); err != nil {
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

	s.mu.Lock()
	s.schema = schema
	s.manager.Mapping.Mu.RLock()
	s.lastMapping = len(s.manager.Mapping.Fields)
	s.manager.Mapping.Mu.RUnlock()
	s.mu.Unlock()
	return nil
}

func (s *Service) Handler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var request struct {
			Query         string                 `json:"query"`
			OperationName string                 `json:"operationName"`
			Variables     map[string]interface{} `json:"variables"`
		}

		if err := c.BindJSON(&request); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		s.mu.RLock()
		schema := s.schema
		s.mu.RUnlock()

		result := graphql.Do(graphql.Params{
			Schema:         schema,
			RequestString:  request.Query,
			VariableValues: request.Variables,
			OperationName:  request.OperationName,
		})

		c.JSON(http.StatusOK, result)
	}
}
