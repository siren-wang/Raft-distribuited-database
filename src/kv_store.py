"""
Distributed Key-Value Store - PostgreSQL Backend Module
Implements connection pooling, schema management, and basic KV operations
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
import asyncpg
from asyncpg.pool import Pool
import backoff

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class DatabaseConfig:
    """Configuration for database connection"""
    host: str = "localhost"
    port: int = 5432
    database: str = "kvstore"
    user: str = "siren"
    password: str = ""

    # Connection pool settings
    min_pool_size: int = 10
    max_pool_size: int = 20

    # Retry settings
    max_retries: int = 3
    retry_base_delay: float = 1.0
    retry_max_delay: float = 60.0

    # Connection settings
    command_timeout: float = 60.0
    connection_timeout: float = 10.0

    @property
    def dsn(self) -> str:
        """Generate PostgreSQL DSN string"""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


class DatabaseError(Exception):
    """Base exception for database operations"""
    pass


class KeyNotFoundError(DatabaseError):
    """Raised when a key is not found in the store"""
    pass


class VersionConflictError(DatabaseError):
    """Raised when there's a version conflict during update"""
    pass


class KeyValueStore:
    """
    PostgreSQL-backed key-value store with connection pooling and versioning
    """

    # SQL queries
    CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS kv_store (
        key VARCHAR(255) PRIMARY KEY,
        value JSONB NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        version INTEGER NOT NULL DEFAULT 1
    );

    CREATE INDEX IF NOT EXISTS idx_kv_store_updated_at ON kv_store(updated_at);
    """

    INSERT_SQL = """
    INSERT INTO kv_store (key, value, created_at, updated_at, version)
    VALUES ($1, $2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1)
    ON CONFLICT (key) DO NOTHING
    RETURNING key, value, created_at, updated_at, version;
    """

    GET_SQL = """
    SELECT key, value, created_at, updated_at, version
    FROM kv_store
    WHERE key = $1;
    """

    UPDATE_SQL = """
    UPDATE kv_store
    SET value = $2, 
        updated_at = CURRENT_TIMESTAMP,
        version = version + 1
    WHERE key = $1 AND version = $3
    RETURNING key, value, created_at, updated_at, version;
    """

    DELETE_SQL = """
    DELETE FROM kv_store
    WHERE key = $1
    RETURNING key, value, created_at, updated_at, version;
    """

    LIST_KEYS_SQL = """
    SELECT key
    FROM kv_store
    ORDER BY key
    LIMIT $1 OFFSET $2;
    """

    def __init__(self, config: Optional[DatabaseConfig] = None):
        """Initialize the KeyValueStore with configuration"""
        self.config = config or DatabaseConfig()
        self._pool: Optional[Pool] = None
        self._initialized = False

        logger.info(f"Initialized KeyValueStore with config: host={self.config.host}, "
                    f"port={self.config.port}, database={self.config.database}")

    @backoff.on_exception(
        backoff.expo,
        (asyncpg.PostgresError, OSError),
        max_tries=3,
        max_time=60
    )
    async def _create_pool(self) -> Pool:
        """Create connection pool with retry logic"""
        logger.info("Creating connection pool...")

        try:
            pool = await asyncpg.create_pool(
                self.config.dsn,
                min_size=self.config.min_pool_size,
                max_size=self.config.max_pool_size,
                command_timeout=self.config.command_timeout,
                timeout=self.config.connection_timeout
            )
            logger.info(f"Connection pool created successfully "
                        f"(min_size={self.config.min_pool_size}, "
                        f"max_size={self.config.max_pool_size})")
            return pool
        except Exception as e:
            logger.error(f"Failed to create connection pool: {e}")
            raise DatabaseError(f"Failed to create connection pool: {e}")

    async def initialize(self):
        """Initialize the database connection and schema"""
        if self._initialized:
            logger.warning("KeyValueStore already initialized")
            return

        logger.info("Initializing KeyValueStore...")

        # Create connection pool
        self._pool = await self._create_pool()

        # Initialize schema
        await self._initialize_schema()

        self._initialized = True
        logger.info("KeyValueStore initialization complete")

    async def _initialize_schema(self):
        """Create database tables and indexes"""
        logger.info("Initializing database schema...")

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(self.CREATE_TABLE_SQL)

        logger.info("Database schema initialized successfully")

    async def close(self):
        """Close the connection pool"""
        if self._pool:
            await self._pool.close()
            self._pool = None
            self._initialized = False
            logger.info("Connection pool closed")

    @asynccontextmanager
    async def acquire_connection(self):
        """Acquire a connection from the pool with proper error handling"""
        if not self._initialized:
            raise DatabaseError("KeyValueStore not initialized. Call initialize() first.")

        try:
            async with self._pool.acquire() as conn:
                yield conn
        except asyncpg.PostgresError as e:
            logger.error(f"Database error: {e}")
            raise DatabaseError(f"Database operation failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise

    @backoff.on_exception(
        backoff.expo,
        DatabaseError,
        max_tries=3,
        max_time=10
    )
    async def get(self, key: str) -> Dict[str, Any]:
        """
        Get a value by key

        Args:
            key: The key to retrieve

        Returns:
            Dictionary with key, value, created_at, updated_at, and version

        Raises:
            KeyNotFoundError: If the key doesn't exist
        """
        logger.debug(f"Getting key: {key}")

        async with self.acquire_connection() as conn:
            row = await conn.fetchrow(self.GET_SQL, key)

            if not row:
                logger.warning(f"Key not found: {key}")
                raise KeyNotFoundError(f"Key '{key}' not found")

            result = {
                "key": row["key"],
                "value": row["value"],
                "created_at": row["created_at"].isoformat(),
                "updated_at": row["updated_at"].isoformat(),
                "version": row["version"]
            }

            logger.debug(f"Retrieved key '{key}' with version {result['version']}")
            return result

    @backoff.on_exception(
        backoff.expo,
        DatabaseError,
        max_tries=3,
        max_time=10
    )
    async def put(self, key: str, value: Any) -> Dict[str, Any]:
        """
        Insert or create a new key-value pair

        Args:
            key: The key to store
            value: The value to store (will be JSON serialized)

        Returns:
            Dictionary with stored data including metadata
        """
        logger.debug(f"Putting key: {key}")

        # Ensure value is JSON serializable
        try:
            json_value = json.dumps(value)
            json.loads(json_value)  # Validate
        except (TypeError, ValueError) as e:
            raise ValueError(f"Value must be JSON serializable: {e}")

        async with self.acquire_connection() as conn:
            row = await conn.fetchrow(self.INSERT_SQL, key, json_value)

            if not row:
                # Key already exists, return current value
                logger.info(f"Key '{key}' already exists, returning current value")
                return await self.get(key)

            result = {
                "key": row["key"],
                "value": row["value"],
                "created_at": row["created_at"].isoformat(),
                "updated_at": row["updated_at"].isoformat(),
                "version": row["version"]
            }

            logger.info(f"Successfully created key '{key}'")
            return result

    @backoff.on_exception(
        backoff.expo,
        DatabaseError,
        max_tries=3,
        max_time=10
    )
    async def update(self, key: str, value: Any, version: int) -> Dict[str, Any]:
        """
        Update an existing key-value pair with optimistic locking

        Args:
            key: The key to update
            value: The new value
            version: The expected version for optimistic locking

        Returns:
            Dictionary with updated data

        Raises:
            KeyNotFoundError: If the key doesn't exist
            VersionConflictError: If the version doesn't match
        """
        logger.debug(f"Updating key: {key} with version {version}")

        # Ensure value is JSON serializable
        try:
            json_value = json.dumps(value)
            json.loads(json_value)  # Validate
        except (TypeError, ValueError) as e:
            raise ValueError(f"Value must be JSON serializable: {e}")

        async with self.acquire_connection() as conn:
            row = await conn.fetchrow(self.UPDATE_SQL, key, json_value, version)

            if not row:
                # Check if key exists
                existing = await conn.fetchrow(self.GET_SQL, key)
                if not existing:
                    raise KeyNotFoundError(f"Key '{key}' not found")
                else:
                    raise VersionConflictError(
                        f"Version conflict for key '{key}': "
                        f"expected {version}, actual {existing['version']}"
                    )

            result = {
                "key": row["key"],
                "value": row["value"],
                "created_at": row["created_at"].isoformat(),
                "updated_at": row["updated_at"].isoformat(),
                "version": row["version"]
            }

            logger.info(f"Successfully updated key '{key}' to version {result['version']}")
            return result

    @backoff.on_exception(
        backoff.expo,
        DatabaseError,
        max_tries=3,
        max_time=10
    )
    async def delete(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Delete a key-value pair

        Args:
            key: The key to delete

        Returns:
            The deleted data if the key existed, None otherwise
        """
        logger.debug(f"Deleting key: {key}")

        async with self.acquire_connection() as conn:
            row = await conn.fetchrow(self.DELETE_SQL, key)

            if not row:
                logger.warning(f"Key '{key}' not found for deletion")
                return None

            result = {
                "key": row["key"],
                "value": row["value"],
                "created_at": row["created_at"].isoformat(),
                "updated_at": row["updated_at"].isoformat(),
                "version": row["version"]
            }

            logger.info(f"Successfully deleted key '{key}'")
            return result

    async def list_keys(self, limit: int = 100, offset: int = 0) -> List[str]:
        """
        List keys with pagination

        Args:
            limit: Maximum number of keys to return
            offset: Number of keys to skip

        Returns:
            List of keys
        """
        logger.debug(f"Listing keys: limit={limit}, offset={offset}")

        async with self.acquire_connection() as conn:
            rows = await conn.fetch(self.LIST_KEYS_SQL, limit, offset)
            keys = [row["key"] for row in rows]

            logger.debug(f"Listed {len(keys)} keys")
            return keys

    async def health_check(self) -> Dict[str, Any]:
        """
        Perform a health check on the database connection

        Returns:
            Dictionary with health status information
        """
        try:
            async with self.acquire_connection() as conn:
                # Simple query to test connection
                result = await conn.fetchval("SELECT 1")

                # Get pool stats
                pool_stats = {
                    "size": self._pool.get_size() if self._pool else 0,
                    "free_connections": self._pool.get_idle_size() if self._pool else 0,
                    "used_connections": self._pool.get_size() - self._pool.get_idle_size()
                    if self._pool else 0
                }

                return {
                    "status": "healthy",
                    "database": self.config.database,
                    "pool_stats": pool_stats,
                    "timestamp": datetime.utcnow().isoformat()
                }
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }


async def main():
    """Example usage of the KeyValueStore"""
    # Create configuration
    config = DatabaseConfig(
        host="localhost",
        port=5432,
        database="kvstore",
        user="siren",
        password=""  # Leave blank if no password is required
    )

    # Create and initialize store
    store = KeyValueStore(config)
    await store.initialize()

    try:
        print("=== KeyValueStore Example ===\n")

        # 1. Create a new key
        key = "user:alice"
        # value = {"name": "Alice", "age": 28}
        # print(f"1. Creating key '{key}'...")
        # created = await store.put(key, value)
        # print(f"Created: {created}\n")
        #
        # # 2. Retrieve the key
        # print(f"2. Retrieving key '{key}'...")
        # retrieved = await store.get(key)
        # print(f"Retrieved: {retrieved}\n")
        #
        # # 3. Update with correct version
        # print("3. Updating with correct version...")
        # updated_value = {"name": "Alice", "age": 29}
        # updated = await store.update(
        #     key,
        #     updated_value,
        #     version=retrieved["version"]
        # )
        # print(f"Updated: {updated}\n")
        #
        # # 4. Try update with wrong version
        # print("4. Attempting update with incorrect version (should fail)...")
        # try:
        #     await store.update(
        #         key,
        #         {"name": "Alice", "age": 30},
        #         version=1  # Likely outdated
        #     )
        # except VersionConflictError as e:
        #     print(f"Expected version conflict: {e}\n")
        #
        # # 5. List all keys
        # print("5. Listing keys...")
        # keys = await store.list_keys(limit=10)
        # print(f"Keys: {keys}\n")
        #
        # # 6. Perform a health check
        # print("6. Performing health check...")
        # health = await store.health_check()
        # print(f"Health: {health}\n")

        # 7. Delete the key
        print(f"7. Deleting key '{key}'...")
        deleted = await store.delete(key)
        print(f"Deleted: {deleted}\n")

    finally:
        await store.close()


if __name__ == "__main__":
    # Run the example
    asyncio.run(main())
