# Distributed Key-Value Store

A production-ready distributed key-value store built with Python, PostgreSQL, and FastAPI.

## Features

- **PostgreSQL Backend**: Reliable persistent storage with JSONB support
- **RESTful API**: Fast, async API built with FastAPI
- **Optimistic Locking**: Version-based conflict resolution
- **Connection Pooling**: Efficient database connection management
- **Authentication**: API key-based authentication
- **Monitoring**: Prometheus metrics and health checks
- **Docker Support**: Easy deployment with Docker Compose

## Quick Start

### Using Docker Compose (Recommended)

```bash
# Clone the repository
git clone <repository-url>
cd distributed-kv-store

# Start all services
docker-compose up -d

# Wait for services to be ready
sleep 10

# Check health
curl http://localhost:8000/health

# Run examples
chmod +x examples/api_examples.sh
./examples/api_examples.sh
```

### Manual Setup

1. **Install PostgreSQL**
   ```bash
   # Ubuntu/Debian
   sudo apt-get install postgresql postgresql-contrib
   
   # macOS
   brew install postgresql
   ```

2. **Create Python virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up database**
   ```bash
   python create_database.py
   ```

5. **Run the API server**
   ```bash
   python -m src.kv_api --api-key your-secure-key
   ```

## API Usage

### Authentication
All API endpoints (except `/health`) require authentication via Bearer token:
```bash
-H "Authorization: Bearer your-api-key"
```

### Endpoints

#### Store a value
```bash
curl -X PUT http://localhost:8000/put/mykey \
  -H "Authorization: Bearer development-api-key-change-in-production" \
  -H "Content-Type: application/json" \
  -d '{"value": {"name": "John", "age": 30}}'
```

#### Retrieve a value
```bash
curl -X GET http://localhost:8000/get/mykey \
  -H "Authorization: Bearer development-api-key-change-in-production"
```

#### Update with version control
```bash
curl -X POST http://localhost:8000/update/mykey \
  -H "Authorization: Bearer development-api-key-change-in-production" \
  -H "Content-Type: application/json" \
  -d '{"value": {"name": "Jane", "age": 31}, "version": 1}'
```

#### Delete a key
```bash
curl -X DELETE http://localhost:8000/delete/mykey \
  -H "Authorization: Bearer development-api-key-change-in-production"
```

#### Check if key exists
```bash
curl -X GET http://localhost:8000/exists/mykey \
  -H "Authorization: Bearer development-api-key-change-in-production"
```

#### List keys (paginated)
```bash
curl -X GET "http://localhost:8000/keys?limit=10&offset=0" \
  -H "Authorization: Bearer development-api-key-change-in-production"
```

## API Documentation

Once running, you can access:
- **Interactive API docs**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **OpenAPI schema**: http://localhost:8000/openapi.json
- **Prometheus metrics**: http://localhost:8000/metrics
- **Health check**: http://localhost:8000/health

## Project Structure

```
distributed-kv-store/
├── src/                    # Source code
│   ├── __init__.py
│   ├── kv_store.py        # PostgreSQL backend
│   └── kv_api.py          # FastAPI REST server
├── examples/              # Usage examples
├── config/                # Configuration files
├── docker/                # Docker files
├── tests/                 # Test suite
├── requirements.txt       # Python dependencies
├── docker-compose.yml     # Docker Compose config
└── README.md             # This file
```

## Configuration

Configuration can be done via:
- Command-line arguments
- Environment variables
- `.env` file (copy from `.env.example`)

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DB_HOST` | PostgreSQL host | localhost |
| `DB_PORT` | PostgreSQL port | 5432 |
| `DB_NAME` | Database name | kvstore |
| `DB_USER` | Database user | siren |
| `DB_PASSWORD` | Database password | (empty) |
| `API_KEY` | API authentication key | (generated) |

## Development

### Running Tests
```bash
pytest tests/ -v
```

### Monitoring

Access Prometheus metrics at http://localhost:9090 when using Docker Compose.

### Troubleshooting

If `docker-compose up` fails:

1. **Check Docker is running**
   ```bash
   docker info
   ```

2. **Check ports are available**
   ```bash
   lsof -i :5432  # PostgreSQL
   lsof -i :8000  # API
   ```

3. **View logs**
   ```bash
   docker-compose logs -f
   ```

4. **Reset everything**
   ```bash
   docker-compose down -v
   docker-compose up --build
   ```

## License

MIT License
