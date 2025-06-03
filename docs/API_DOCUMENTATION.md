# Raft Distributed Database - API Documentation

This document provides comprehensive API documentation for the Raft Distributed Database system, which includes both a standalone Key-Value Store API and a distributed Raft-based API.

## Table of Contents

1. [Overview](#overview)
2. [Authentication](#authentication)
3. [Standalone Key-Value Store API](#standalone-key-value-store-api)
4. [Raft Distributed Database API](#raft-distributed-database-api)
5. [Error Handling](#error-handling)
6. [Examples](#examples)
7. [Client Libraries](#client-libraries)

## Overview

The system provides two main APIs:

1. **Standalone Key-Value Store API** (`src/kv_api.py`) - A simple, single-node key-value store with PostgreSQL backend
2. **Raft Distributed Database API** (`src/raft/server.py`) - A distributed, fault-tolerant key-value store using the Raft consensus algorithm

Both APIs are built with FastAPI and provide automatic OpenAPI documentation at `/docs` and `/redoc` endpoints.

## Authentication

### API Key Authentication (Standalone KV Store)

The standalone API uses Bearer token authentication:

```http
Authorization: Bearer <api_key>
```

Default development API key: `development-api-key-change-in-production`

### No Authentication (Raft API)

The Raft API currently doesn't require authentication (suitable for internal cluster communication).

---

## Standalone Key-Value Store API

**Base URL**: `http://localhost:8000`
**FastAPI Docs**: `http://localhost:8000/docs`

### Health & Monitoring

#### GET /health
Get service health status.

**Response**:
```json
{
  "status": "healthy",
  "database": {
    "status": "healthy",
    "connection_pool": {
      "size": 10,
      "checked_out": 2
    }
  },
  "api_version": "1.0.0",
  "timestamp": "2023-12-07T10:30:00Z"
}
```

#### GET /metrics
Prometheus metrics endpoint (returns plain text metrics).

### Key-Value Operations

#### GET /get/{key}
Retrieve a value by key.

**Parameters**:
- `key` (path) - The key to retrieve

**Response** (200):
```json
{
  "key": "user:123",
  "value": {"name": "John Doe", "email": "john@example.com"},
  "created_at": "2023-12-07T10:00:00Z",
  "updated_at": "2023-12-07T10:30:00Z",
  "version": 3
}
```

**Error** (404):
```json
{
  "error": "key_not_found",
  "message": "Key 'user:123' not found",
  "timestamp": "2023-12-07T10:30:00Z"
}
```

#### PUT /put/{key}
Store or create a new key-value pair.

**Parameters**:
- `key` (path) - The key to store

**Request Body**:
```json
{
  "value": {"name": "John Doe", "email": "john@example.com"}
}
```

**Response** (200):
```json
{
  "key": "user:123",
  "value": {"name": "John Doe", "email": "john@example.com"},
  "created_at": "2023-12-07T10:30:00Z",
  "updated_at": "2023-12-07T10:30:00Z",
  "version": 1
}
```

#### POST /update/{key}
Update an existing key-value pair with optimistic locking.

**Parameters**:
- `key` (path) - The key to update

**Request Body**:
```json
{
  "value": {"name": "Jane Doe", "email": "jane@example.com"},
  "version": 3
}
```

**Response** (200):
```json
{
  "key": "user:123",
  "value": {"name": "Jane Doe", "email": "jane@example.com"},
  "created_at": "2023-12-07T10:00:00Z",
  "updated_at": "2023-12-07T10:35:00Z",
  "version": 4
}
```

**Error** (409 - Version Conflict):
```json
{
  "error": "version_conflict",
  "message": "Expected version 3, but current version is 4",
  "timestamp": "2023-12-07T10:35:00Z"
}
```

#### DELETE /delete/{key}
Delete a key-value pair.

**Parameters**:
- `key` (path) - The key to delete

**Response** (200):
```json
{
  "key": "user:123",
  "value": {"name": "Jane Doe", "email": "jane@example.com"},
  "created_at": "2023-12-07T10:00:00Z",
  "updated_at": "2023-12-07T10:35:00Z",
  "version": 4
}
```

#### GET /exists/{key}
Check if a key exists without retrieving the value.

**Parameters**:
- `key` (path) - The key to check

**Response** (200):
```json
{
  "exists": true
}
```

#### GET /keys
List keys with pagination.

**Query Parameters**:
- `limit` (int, optional) - Maximum number of keys to return (default: 100, max: 1000)
- `offset` (int, optional) - Number of keys to skip (default: 0)

**Response** (200):
```json
{
  "keys": ["user:123", "user:124", "config:app"],
  "count": 3,
  "limit": 100,
  "offset": 0
}
```

### Administration

#### GET /config
Get current configuration (requires authentication).

**Response** (200):
```json
{
  "database": {
    "host": "localhost",
    "port": 5432,
    "database": "kvstore"
  },
  "api_version": "1.0.0"
}
```

---

## Raft Distributed Database API

**Base URL**: `http://localhost:8001` (node1), `http://localhost:8002` (node2), `http://localhost:8003` (node3)
**FastAPI Docs**: `http://localhost:8001/docs`

### Cluster Status & Monitoring

#### GET /raft/status
Get Raft cluster status.

**Response**:
```json
{
  "node_id": "node1",
  "state": "leader",
  "current_term": 5,
  "current_leader": "node1",
  "log_length": 42,
  "commit_index": 41,
  "last_applied": 41,
  "cluster_size": 3,
  "wal_stats": {
    "total_entries": 42,
    "last_index": 42,
    "size_bytes": 15360
  }
}
```

**Node States**:
- `leader` - Node is the current cluster leader
- `follower` - Node is following the current leader  
- `candidate` - Node is running for election

#### GET /raft/debug
Get detailed debug information (includes log entries, indices, etc.).

**Response**:
```json
{
  "node_id": "node1",
  "state": "leader",
  "current_term": 5,
  "voted_for": "node1",
  "current_leader": "node1",
  "log_length": 42,
  "last_log_index": 42,
  "last_log_term": 5,
  "commit_index": 41,
  "last_applied": 41,
  "next_index": {"node2": 43, "node3": 43},
  "match_index": {"node2": 42, "node3": 42},
  "log_entries": [
    {
      "index": 40,
      "term": 5,
      "command_type": "put"
    },
    {
      "index": 41,
      "term": 5,
      "command_type": "update"
    },
    {
      "index": 42,
      "term": 5,
      "command_type": "delete"
    }
  ]
}
```

### Distributed Key-Value Operations

#### GET /kv/{key}
Get a value from the distributed store.

**Parameters**:
- `key` (path) - The key to retrieve
- `linearizable` (query, optional) - Whether to ensure linearizable reads (default: true)

**Response** (200):
```json
{
  "key": "user:123",
  "value": {"name": "John Doe", "email": "john@example.com"},
  "version": 3,
  "created_at": "2023-12-07T10:00:00Z",
  "updated_at": "2023-12-07T10:30:00Z"
}
```

**Error** (404):
```json
{
  "detail": "Key 'user:123' not found"
}
```

#### PUT /kv/{key}
Put a value into the distributed store.

**Parameters**:
- `key` (path) - The key to store

**Request Body**:
```json
{
  "value": {"name": "John Doe", "email": "john@example.com"}
}
```

**Response** (200):
```json
{
  "key": "user:123",
  "value": {"name": "John Doe", "email": "john@example.com"},
  "version": 1,
  "created_at": "2023-12-07T10:30:00Z",
  "updated_at": "2023-12-07T10:30:00Z"
}
```

#### POST /kv/{key}
Update a value with version check (optimistic locking).

**Parameters**:
- `key` (path) - The key to update

**Request Body**:
```json
{
  "value": {"name": "Jane Doe", "email": "jane@example.com"},
  "version": 3
}
```

**Response** (200):
```json
{
  "key": "user:123",
  "value": {"name": "Jane Doe", "email": "jane@example.com"},
  "version": 4,
  "created_at": "2023-12-07T10:00:00Z",
  "updated_at": "2023-12-07T10:35:00Z"
}
```

#### DELETE /kv/{key}
Delete a key from the distributed store.

**Parameters**:
- `key` (path) - The key to delete

**Response** (200):
```json
{
  "key": "user:123",
  "value": {"name": "Jane Doe", "email": "jane@example.com"},
  "version": 4,
  "created_at": "2023-12-07T10:00:00Z",
  "updated_at": "2023-12-07T10:35:00Z"
}
```

**Response** (200 - Key not found):
```json
{
  "deleted": true
}
```

### Raft Internal Endpoints

These endpoints are used for internal Raft protocol communication:

#### POST /raft/request_vote
Handle RequestVote RPC (internal use).

#### POST /raft/append_entries  
Handle AppendEntries RPC (internal use).

---

## Error Handling

### HTTP Status Codes

- **200 OK** - Successful operation
- **400 Bad Request** - Invalid request parameters
- **401 Unauthorized** - Invalid or missing API key (standalone API)
- **404 Not Found** - Key not found
- **409 Conflict** - Version conflict during update
- **500 Internal Server Error** - Server error
- **503 Service Unavailable** - Service health check failed

### Error Response Format

```json
{
  "error": "error_type",
  "message": "Human-readable error message",
  "details": {
    "additional": "context"
  },
  "timestamp": "2023-12-07T10:30:00Z"
}
```

### Common Error Types

- `key_not_found` - Requested key does not exist
- `version_conflict` - Version mismatch during optimistic locking
- `invalid_request` - Malformed request body or parameters
- `service_unavailable` - Database or service temporarily unavailable

---

## Examples

### Using curl with Standalone API

```bash
# Set API key
API_KEY="development-api-key-change-in-production"

# Store a value
curl -X PUT "http://localhost:8000/put/user:123" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"value": {"name": "John Doe", "email": "john@example.com"}}'

# Retrieve a value
curl -X GET "http://localhost:8000/get/user:123" \
  -H "Authorization: Bearer $API_KEY"

# Update with version check
curl -X POST "http://localhost:8000/update/user:123" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"value": {"name": "Jane Doe", "email": "jane@example.com"}, "version": 1}'

# List keys
curl -X GET "http://localhost:8000/keys?limit=10&offset=0" \
  -H "Authorization: Bearer $API_KEY"

# Delete a key
curl -X DELETE "http://localhost:8000/delete/user:123" \
  -H "Authorization: Bearer $API_KEY"
```

### Using curl with Raft API

```bash
# Store a value (will be replicated across cluster)
curl -X PUT "http://localhost:8001/kv/user:123" \
  -H "Content-Type: application/json" \
  -d '{"value": {"name": "John Doe", "email": "john@example.com"}}'

# Retrieve a value
curl -X GET "http://localhost:8001/kv/user:123"

# Update with version check
curl -X POST "http://localhost:8001/kv/user:123" \
  -H "Content-Type: application/json" \
  -d '{"value": {"name": "Jane Doe", "email": "jane@example.com"}, "version": 1}'

# Check cluster status
curl -X GET "http://localhost:8001/raft/status"

# Get debug information
curl -X GET "http://localhost:8001/raft/debug"
```

### Using Python requests

```python
import requests

# Standalone API
API_KEY = "development-api-key-change-in-production"
headers = {"Authorization": f"Bearer {API_KEY}"}

# Store a value
response = requests.put(
    "http://localhost:8000/put/user:123",
    headers=headers,
    json={"value": {"name": "John Doe", "email": "john@example.com"}}
)
print(response.json())

# Raft API
# Store a value in distributed store
response = requests.put(
    "http://localhost:8001/kv/user:123",
    json={"value": {"name": "John Doe", "email": "john@example.com"}}
)
print(response.json())

# Check cluster status
response = requests.get("http://localhost:8001/raft/status")
print(response.json())
```

---

## Client Libraries

### Python Client for Raft Cluster

The project includes a Python client library (`src/raft/client.py`) for easy interaction with the Raft cluster:

```python
from src.raft.client import RaftClient

# Create client
client = RaftClient(["localhost:8001", "localhost:8002", "localhost:8003"])

# Store a value
await client.put("user:123", {"name": "John Doe", "email": "john@example.com"})

# Retrieve a value
value = await client.get("user:123")
print(value)

# Update with version check
await client.update("user:123", {"name": "Jane Doe"}, version=1)

# Delete a key
await client.delete("user:123")

# Close client
await client.close()
```

### Key Features of the Client:
- **Automatic leader discovery** - Finds and connects to the current leader
- **Transparent failover** - Automatically retries on different nodes if leader fails
- **Connection pooling** - Efficient HTTP connection management
- **Idempotency** - Built-in request deduplication for safety

---

## Running the APIs

### Standalone Key-Value Store

```bash
# Start PostgreSQL
docker compose up postgres -d

# Install dependencies
pip install -r requirements.txt

# Run the API
python -m src.kv_api --host 0.0.0.0 --port 8000
```

### Raft Distributed Database

```bash
# Start the entire Raft cluster
docker compose -f docker-compose-raft.yml up -d

# The APIs will be available at:
# - Node 1: http://localhost:8001
# - Node 2: http://localhost:8002  
# - Node 3: http://localhost:8003

# View logs
docker compose -f docker-compose-raft.yml logs -f
```

### Demo Slides

```bash
# Run individual demos
python demo_slides.py slide9   # Leader Election Demo
python demo_slides.py slide10  # Fault Tolerance Demo
python demo_slides.py slide11  # Challenge Demo

# Run all demos
python demo_slides.py all
```

---

## Interactive Documentation

Both APIs provide interactive Swagger UI documentation:

- **Standalone API**: http://localhost:8000/docs
- **Raft API**: http://localhost:8001/docs (for node1)

These interfaces allow you to:
- Browse all available endpoints
- View request/response schemas  
- Test API calls directly in the browser
- Download OpenAPI specifications

For ReDoc-style documentation:
- **Standalone API**: http://localhost:8000/redoc
- **Raft API**: http://localhost:8001/redoc 