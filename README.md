# Breeze

Breeze is a standalone, sharded database system built on top of the [Bleve](https://github.com/blevesearch/bleve) full-text search library.

## Features

- **Full CRUD:** Create, Read, Update, and Delete JSON documents.
- **Search:** Full-text search powered by Bleve.
- **Dynamic GraphQL:** Automatically generates a GraphQL schema by sniffing your JSON documents. Multi-index support available via `/graphql/:index`.
- **Elasticsearch Compatible:** Supports a significant subset of the Elasticsearch REST API, including Document, Search, Bulk, and Multi-Search APIs.
- **Kibana Support:** Fully compatible with Kibana (tested with v8.10.2) for data visualization and exploration.
- **ACID Compliant:** Uses a Write-Ahead Log (WAL) to ensure durability for single-document operations.
- **Sharding:** Automatically distributes data across multiple shards per index for scalability.
- **CLI Tool:** Built-in CLI for server management and data operations.

## Quick Start

### Build

```bash
go build -o breeze ./cmd/breeze/main.go
```

### Run Server

```bash
./breeze start --path ./data --port 8080 --shards 5
```

### Index a Document

Using CLI:
```bash
./breeze index 1 '{"name": "Breeze DB", "description": "Fast and light"}'
```

Using CURL (ES API):
```bash
curl -X PUT http://localhost:8080/default/_doc/2 -d '{"name": "Bleve", "type": "Library"}'
```

### Query Documents

Using CLI:
```bash
./breeze query "Breeze"
```

Using GraphQL:
```bash
curl -X POST http://localhost:8080/graphql/default -d '{"query": "query { search(query: \"Breeze\") { id name description } }"}'
```

## Kibana Connection

Breeze implements the necessary Elasticsearch handshake endpoints to allow Kibana to connect directly.

1. Install Kibana 8.10.2.
2. Update `kibana.yml`:
   ```yaml
   elasticsearch.hosts: ["http://localhost:8080"]
   ```
3. Start Kibana and navigate to `http://localhost:5601`.

## Docker & Kubernetes

### Docker

Build the image:
```bash
docker build -t breeze:latest .
```

Run the container:
```bash
docker run -p 8080:8080 -v $(pwd)/data:/root/data breeze:latest
```

### Kubernetes

Deploy Breeze as a StatefulSet:
```bash
kubectl apply -f k8s/breeze.yaml
```

## Architecture

Breeze uses a **Coordinator + Shard** architecture. Every node can act as a coordinator:
- **Multi-Index:** Each index is managed independently with its own shard set and mapping.
- **Writes:** Documents are hashed by ID (CRC32) and routed to the corresponding shard within the index.
- **Reads:** Requests for specific IDs are routed to the owner shard.
- **Searches:** Queries are fanned out to all shards of the target index and the results are merged.

Durability is ensured by writing every operation to a **Write-Ahead Log (WAL)** before it is committed to the underlying Bleve index.
