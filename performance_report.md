# Performance Comparison: Breeze vs Elasticsearch

This benchmark was conducted using **Elastic Rally** on a local environment comparing Breeze (built on Bleve) against Elasticsearch 8.10.0.

## Benchmark Configuration
- **Dataset:** 10,000 documents (Custom Track)
- **Breeze Configuration:** 5 shards, WAL enabled (sync: true)
- **Elasticsearch Configuration:** Single node, default settings, security disabled.
- **Hardware:** Shared sandbox environment (8GB RAM, 4 CPUs).

## Results Summary

| Metric | Task | Elasticsearch | Breeze | Improvement |
|:---|:---|:---|:---|:---|
| **Mean Throughput** | Indexing (bulk) | 10,555 docs/s | 53,201 docs/s | **+404%** |
| **50th Latency** | Indexing (bulk) | 71.50 ms | 0.67 ms | **-99%** |
| **Mean Throughput** | Search (query) | 75.7 ops/s | 546.3 ops/s | **+621%** |
| **50th Latency** | Search (query) | 18.15 ms | 6.00 ms | **-66%** |

## Analysis
Breeze demonstrates significantly higher throughput and lower latency for both indexing and search operations in this lightweight setup.

### Key Factors:
1. **Low Overhead:** Breeze is a lean Go binary with minimal background processing compared to the full Elasticsearch stack.
2. **Efficient Batching:** Breeze's implementation of the `_bulk` API directly translates to Bleve batches, which are highly optimized.
3. **Reduced Complexity:** For single-node/simple-query workloads, Breeze avoids the coordination and complex query planning overhead of Elasticsearch.
4. **Go Performance:** Bleve (and Go) provides excellent performance for search tasks with lower memory footprint than JVM-based systems like Elasticsearch in resource-constrained environments.

*Note: These benchmarks represent a specific workload and environment. Elasticsearch scales horizontally and offers many more features (aggregations, advanced query types, etc.) that may affect performance in more complex scenarios.*
