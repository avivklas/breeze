package mapping

import (
	"sync"

	"github.com/graphql-go/graphql"
)

type FieldType int

const (
	TypeString FieldType = iota
	TypeNumber
	TypeBoolean
	TypeObject
)

type Mapping struct {
	Fields map[string]FieldType
	Mu     sync.RWMutex
}

func NewMapping() *Mapping {
	return &Mapping{
		Fields: make(map[string]FieldType),
	}
}

func (m *Mapping) Sniff(data map[string]interface{}) bool {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	changed := false
	for k, v := range data {
		if k == "_source" {
			continue
		}
		var detected FieldType
		switch v.(type) {
		case string:
			detected = TypeString
		case float64, int, int64:
			detected = TypeNumber
		case bool:
			detected = TypeBoolean
		case map[string]interface{}:
			detected = TypeObject
		default:
			continue
		}

		if _, ok := m.Fields[k]; !ok {
			m.Fields[k] = detected
			changed = true
		}
	}
	return changed
}

func (m *Mapping) BuildGraphQLType(name string) *graphql.Object {
	m.Mu.RLock()
	defer m.Mu.RUnlock()

	fields := graphql.Fields{
		"id": &graphql.Field{Type: graphql.ID},
	}

	for k, t := range m.Fields {
		var gqlType graphql.Output
		switch t {
		case TypeString:
			gqlType = graphql.String
		case TypeNumber:
			gqlType = graphql.Float
		case TypeBoolean:
			gqlType = graphql.Boolean
		default:
			gqlType = graphql.String // Default to string for complex types for now
		}
		fields[k] = &graphql.Field{Type: gqlType}
	}

	return graphql.NewObject(graphql.ObjectConfig{
		Name:   name,
		Fields: fields,
	})
}
