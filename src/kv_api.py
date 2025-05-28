"""
Distributed Key-Value Store - RESTful API Server
FastAPI-based REST API with authentication, metrics, and comprehensive error handling
"""

import time
import secrets
from datetime import datetime
from typing import Optional, Dict, Any, List
from contextlib import asynccontextmanager
import logging
from functools import wraps

from fastapi import FastAPI, HTTPException, Depends, Header, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator
import uvicorn
from prometheus_client import Counter, Histogram, Gauge, generate_latest
from prometheus_client.core import CollectorRegistry
from starlette.responses import PlainTextResponse

# Import our KeyValueStore (from previous module)
from src.kv_store import KeyValueStore, DatabaseConfig, KeyNotFoundError, VersionConflictError, DatabaseError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Metrics setup
registry = CollectorRegistry()
request_count = Counter(
    'kvstore_requests_total',
    'Total number of requests',
    ['method', 'endpoint', 'status'],
    registry=registry
)
request_duration = Histogram(
    'kvstore_request_duration_seconds',
    'Request duration in seconds',
    ['method', 'endpoint'],
    registry=registry
)
active_connections = Gauge(
    'kvstore_active_connections',
    'Number of active connections',
    registry=registry
)
error_count = Counter(
    'kvstore_errors_total',
    'Total number of errors',
    ['error_type'],
    registry=registry
)


# Pydantic models
class KeyValueRequest(BaseModel):
    """Request model for key-value operations"""
    value: Any = Field(..., description="The value to store (JSON-serializable)")
    version: Optional[int] = Field(None, description="Version for optimistic locking (update only)")

    @validator('value')
    def validate_json_serializable(cls, v):
        """Ensure value is JSON serializable"""
        import json
        try:
            json.dumps(v)
            return v
        except (TypeError, ValueError) as e:
            raise ValueError(f"Value must be JSON serializable: {e}")


class KeyValueResponse(BaseModel):
    """Response model for key-value operations"""
    key: str = Field(..., description="The key")
    value: Any = Field(..., description="The stored value")
    created_at: str = Field(..., description="Creation timestamp (ISO format)")
    updated_at: str = Field(..., description="Last update timestamp (ISO format)")
    version: int = Field(..., description="Version number for optimistic locking")


class ErrorResponse(BaseModel):
    """Standard error response"""
    error: str = Field(..., description="Error type")
    message: str = Field(..., description="Error message")
    details: Optional[Dict[str, Any]] = Field(None, description="Additional error details")
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())


class HealthResponse(BaseModel):
    """Health check response"""
    status: str = Field(..., description="Service status")
    database: Dict[str, Any] = Field(..., description="Database health information")
    api_version: str = Field(default="1.0.0", description="API version")
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())


class ListKeysResponse(BaseModel):
    """Response for listing keys"""
    keys: List[str] = Field(..., description="List of keys")
    count: int = Field(..., description="Number of keys returned")
    limit: int = Field(..., description="Limit used")
    offset: int = Field(..., description="Offset used")


# Authentication
class APIKeyAuth:
    """Simple API key authentication"""

    def __init__(self, api_keys: Optional[List[str]] = None):
        self.api_keys = set(api_keys) if api_keys else set()
        if not self.api_keys:
            # Generate a default API key for demo purposes
            default_key = secrets.token_urlsafe(32)
            self.api_keys.add(default_key)
            logger.warning(f"No API keys provided. Generated default key: {default_key}")

    async def __call__(self, authorization: Optional[str] = Header(None)) -> str:
        """Validate API key from Authorization header"""
        if not authorization:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing Authorization header"
            )

        # Expected format: "Bearer <api_key>"
        parts = authorization.split()
        if len(parts) != 2 or parts[0].lower() != "bearer":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid Authorization header format"
            )

        api_key = parts[1]
        if api_key not in self.api_keys:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid API key"
            )

        return api_key


# Timing decorator for metrics
def track_request_duration(endpoint: str):
    """Decorator to track request duration"""

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                return result
            finally:
                duration = time.time() - start_time
                request_duration.labels(method=func.__name__, endpoint=endpoint).observe(duration)

        return wrapper

    return decorator


# Create FastAPI app with lifespan manager
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle"""
    # Startup
    logger.info("Starting KV Store API server...")

    # Initialize database
    config = DatabaseConfig(
        host=app.state.db_host,
        port=app.state.db_port,
        database=app.state.db_name,
        user=app.state.db_user,
        password=app.state.db_password
    )

    app.state.kv_store = KeyValueStore(config)
    await app.state.kv_store.initialize()
    logger.info("Database initialized successfully")

    yield

    # Shutdown
    logger.info("Shutting down KV Store API server...")
    await app.state.kv_store.close()
    logger.info("Database connections closed")


# Create FastAPI application
app = FastAPI(
    title="Distributed Key-Value Store API",
    description="A RESTful API for distributed key-value storage with PostgreSQL backend",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configure app state with defaults (can be overridden)
app.state.db_host = "localhost"
app.state.db_port = 5432
app.state.db_name = "kvstore"
app.state.db_user = "siren"
app.state.db_password = "pass"
app.state.api_keys = []  # Will generate default if empty

# Initialize auth
auth = APIKeyAuth(app.state.api_keys)


# Exception handlers
@app.exception_handler(KeyNotFoundError)
async def key_not_found_handler(request: Request, exc: KeyNotFoundError):
    """Handle key not found errors"""
    error_count.labels(error_type="key_not_found").inc()
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content=ErrorResponse(
            error="key_not_found",
            message=str(exc)
        ).dict()
    )


@app.exception_handler(VersionConflictError)
async def version_conflict_handler(request: Request, exc: VersionConflictError):
    """Handle version conflict errors"""
    error_count.labels(error_type="version_conflict").inc()
    return JSONResponse(
        status_code=status.HTTP_409_CONFLICT,
        content=ErrorResponse(
            error="version_conflict",
            message=str(exc)
        ).dict()
    )


@app.exception_handler(DatabaseError)
async def database_error_handler(request: Request, exc: DatabaseError):
    """Handle database errors"""
    error_count.labels(error_type="database_error").inc()
    logger.error(f"Database error: {exc}")
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content=ErrorResponse(
            error="database_error",
            message="Database operation failed",
            details={"original_error": str(exc)}
        ).dict()
    )


@app.exception_handler(ValueError)
async def value_error_handler(request: Request, exc: ValueError):
    """Handle validation errors"""
    error_count.labels(error_type="validation_error").inc()
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content=ErrorResponse(
            error="validation_error",
            message=str(exc)
        ).dict()
    )


# Middleware for request tracking
@app.middleware("http")
async def track_requests(request: Request, call_next):
    """Track request metrics"""
    active_connections.inc()
    start_time = time.time()

    try:
        response = await call_next(request)
        duration = time.time() - start_time

        # Log request
        logger.info(
            f"{request.method} {request.url.path} - "
            f"Status: {response.status_code} - "
            f"Duration: {duration:.3f}s"
        )

        # Track metrics
        request_count.labels(
            method=request.method,
            endpoint=request.url.path,
            status=response.status_code
        ).inc()

        return response
    finally:
        active_connections.dec()


# API Endpoints
@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """Check service health status"""
    try:
        db_health = await app.state.kv_store.health_check()
        return HealthResponse(
            status="healthy" if db_health["status"] == "healthy" else "degraded",
            database=db_health
        )
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service unavailable"
        )


@app.get("/metrics", tags=["Metrics"])
async def metrics():
    """Prometheus metrics endpoint"""
    return PlainTextResponse(generate_latest(registry))


@app.get("/get/{key}", response_model=KeyValueResponse, tags=["Key-Value Operations"])
@track_request_duration("/get/{key}")
async def get_value(
        key: str,
        api_key: str = Depends(auth)
):
    """
    Retrieve a value by key

    - **key**: The key to retrieve
    - Returns the value and metadata
    - Returns 404 if key not found
    """
    logger.debug(f"GET request for key: {key}")
    result = await app.state.kv_store.get(key)
    return KeyValueResponse(**result)


@app.put("/put/{key}", response_model=KeyValueResponse, tags=["Key-Value Operations"])
@track_request_duration("/put/{key}")
async def put_value(
        key: str,
        request: KeyValueRequest,
        api_key: str = Depends(auth)
):
    """
    Store or create a new key-value pair

    - **key**: The key to store
    - **value**: The value to store (must be JSON-serializable)
    - Returns the stored data with metadata
    - This operation is idempotent - repeated calls with same key won't create duplicates
    """
    logger.debug(f"PUT request for key: {key}")
    result = await app.state.kv_store.put(key, request.value)
    return KeyValueResponse(**result)


@app.post("/update/{key}", response_model=KeyValueResponse, tags=["Key-Value Operations"])
@track_request_duration("/update/{key}")
async def update_value(
        key: str,
        request: KeyValueRequest,
        api_key: str = Depends(auth)
):
    """
    Update an existing key-value pair with optimistic locking

    - **key**: The key to update
    - **value**: The new value
    - **version**: The expected version (required for update)
    - Returns 404 if key not found
    - Returns 409 if version conflict
    """
    if request.version is None:
        raise ValueError("Version is required for update operations")

    logger.debug(f"UPDATE request for key: {key} with version {request.version}")
    result = await app.state.kv_store.update(key, request.value, request.version)
    return KeyValueResponse(**result)


@app.delete("/delete/{key}", response_model=Optional[KeyValueResponse], tags=["Key-Value Operations"])
@track_request_duration("/delete/{key}")
async def delete_value(
        key: str,
        api_key: str = Depends(auth)
):
    """
    Delete a key-value pair

    - **key**: The key to delete
    - Returns the deleted data if key existed
    - Returns 404 if key not found
    """
    logger.debug(f"DELETE request for key: {key}")
    result = await app.state.kv_store.delete(key)

    if result is None:
        raise KeyNotFoundError(f"Key '{key}' not found")

    return KeyValueResponse(**result)


@app.get("/exists/{key}", response_model=Dict[str, bool], tags=["Key-Value Operations"])
@track_request_duration("/exists/{key}")
async def check_exists(
        key: str,
        api_key: str = Depends(auth)
):
    """
    Check if a key exists without retrieving the value

    - **key**: The key to check
    - Returns {"exists": true/false}
    """
    logger.debug(f"EXISTS request for key: {key}")
    try:
        await app.state.kv_store.get(key)
        return {"exists": True}
    except KeyNotFoundError:
        return {"exists": False}


@app.get("/keys", response_model=ListKeysResponse, tags=["Key-Value Operations"])
@track_request_duration("/keys")
async def list_keys(
        limit: int = 100,
        offset: int = 0,
        api_key: str = Depends(auth)
):
    """
    List keys with pagination

    - **limit**: Maximum number of keys to return (default: 100, max: 1000)
    - **offset**: Number of keys to skip (default: 0)
    """
    # Validate parameters
    if limit > 1000:
        raise ValueError("Limit cannot exceed 1000")
    if limit < 1:
        raise ValueError("Limit must be positive")
    if offset < 0:
        raise ValueError("Offset cannot be negative")

    logger.debug(f"LIST_KEYS request with limit={limit}, offset={offset}")
    keys = await app.state.kv_store.list_keys(limit=limit, offset=offset)

    return ListKeysResponse(
        keys=keys,
        count=len(keys),
        limit=limit,
        offset=offset
    )


# Configuration endpoint (for debugging)
@app.get("/config", tags=["Administration"])
async def get_config(api_key: str = Depends(auth)):
    """Get current configuration (admin only)"""
    return {
        "database": {
            "host": app.state.db_host,
            "port": app.state.db_port,
            "database": app.state.db_name
        },
        "api_version": "1.0.0"
    }


# CLI entry point
def create_app(
        db_host: str = "localhost",
        db_port: int = 5432,
        db_name: str = "kvstore",
        db_user: str = "siren",
        db_password: str = "pass",
        api_keys: Optional[List[str]] = None
) -> FastAPI:
    """Create and configure the FastAPI application"""
    app.state.db_host = db_host
    app.state.db_port = db_port
    app.state.db_name = db_name
    app.state.db_user = db_user
    app.state.db_password = db_password
    app.state.api_keys = api_keys or []

    # Reinitialize auth with new keys
    global auth
    auth = APIKeyAuth(app.state.api_keys)

    return app


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="KV Store API Server")
    parser.add_argument("--host", default="0.0.0.0", help="Server host")
    parser.add_argument("--port", type=int, default=8000, help="Server port")
    parser.add_argument("--db-host", default="localhost", help="Database host")
    parser.add_argument("--db-port", type=int, default=5432, help="Database port")
    parser.add_argument("--db-name", default="kvstore", help="Database name")
    parser.add_argument("--db-user", default="siren", help="Database user")
    parser.add_argument("--db-password", default="pass", help="Database password")
    parser.add_argument("--api-key", action="append", help="API keys (can specify multiple)")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload")

    args = parser.parse_args()

    # Configure app
    app = create_app(
        db_host=args.db_host,
        db_port=args.db_port,
        db_name=args.db_name,
        db_user=args.db_user,
        db_password=args.db_password,
        api_keys=args.api_key
    )

    # Run server
    uvicorn.run(
        "kv_api:app" if args.reload else app,
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level="info"
    )