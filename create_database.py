import asyncpg
import asyncio
import os
from dotenv import load_dotenv

# Load .env file if present
load_dotenv()

# Configuration
DB_ADMIN_USER = os.getenv("DB_ADMIN_USER", "siren")
DB_ADMIN_PASS = os.getenv("DB_ADMIN_PASS", "")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "kvstore")

KV_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS kv_store (
    key VARCHAR(255) PRIMARY KEY,
    value JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    version INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_kv_store_updated_at ON kv_store(updated_at);
"""

TRIGGER_SQL = """
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS set_updated_at ON kv_store;

CREATE TRIGGER set_updated_at
BEFORE UPDATE ON kv_store
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();
"""

async def create_database_if_not_exists():
    """Connect to default DB and create kvstore if it doesn't exist"""
    try:
        conn = await asyncpg.connect(
            user=DB_ADMIN_USER,
            password=DB_ADMIN_PASS,
            database="postgres",  # Connect to default DB
            host=DB_HOST,
            port=DB_PORT
        )

        exists = await conn.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1", DB_NAME
        )

        if not exists:
            print(f"Creating database '{DB_NAME}'...")
            await conn.execute(f'CREATE DATABASE "{DB_NAME}"')
        else:
            print(f"Database '{DB_NAME}' already exists.")

        await conn.close()
    except Exception as e:
        print(f"Failed to create database: {e}")
        raise


async def create_schema():
    """Connect to kvstore and create table and triggers"""
    try:
        conn = await asyncpg.connect(
            user=DB_ADMIN_USER,
            password=DB_ADMIN_PASS,
            database=DB_NAME,
            host=DB_HOST,
            port=DB_PORT
        )

        await conn.execute(KV_TABLE_SQL)
        await conn.execute(TRIGGER_SQL)
        await conn.close()

        print(f"Schema for '{DB_NAME}' initialized successfully.")
    except Exception as e:
        print(f"Failed to initialize schema: {e}")
        raise


async def main():
    await create_database_if_not_exists()
    await create_schema()

if __name__ == "__main__":
    asyncio.run(main())
