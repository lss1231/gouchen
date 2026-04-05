"""FastAPI application entry point."""
from fastapi import FastAPI
from contextlib import asynccontextmanager

from .api.routes import query, health
from .services.schema_store import get_schema_store, load_tables_from_json
from pathlib import Path


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    # Startup: Initialize schema store
    print("Initializing schema store...")
    schema_path = Path("data/schema/ecommerce_schema.json")
    if schema_path.exists():
        tables = load_tables_from_json(schema_path)
        store = get_schema_store()
        store.index_tables(tables)
        print(f"Indexed {len(tables)} tables")
    yield
    # Shutdown
    print("Shutting down...")


app = FastAPI(
    title="钩沉 NL2SQL",
    description="自然语言数据查询助手 - Deep Agents 版",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(query.router, prefix="/api/v1")
app.include_router(health.router, prefix="/api/v1")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
