distributed-kv-store/
│
├── src/
│   ├── __init__.py
│   ├── kv_store.py          # PostgreSQL backend module (KeyValueStore class)
│   └── kv_api.py            # FastAPI REST server
│
├── examples/
│   ├── api_examples.sh      # Curl examples for API usage
│   └── kv_client_example.py # Python client example
│
├── config/
│   └── prometheus.yml       # Prometheus configuration
│
├── docker/
│   └── Dockerfile          # Docker image for the API server
│
├── tests/
│   ├── __init__.py
│   ├── test_kv_store.py    # Unit tests for KeyValueStore
│   └── test_api.py         # API integration tests
│
├── requirements.txt         # Python dependencies
├── docker-compose.yml      # Docker Compose for local development
├── README.md              # Project documentation
├── .gitignore            # Git ignore file
└── .env.example          # Environment variables example