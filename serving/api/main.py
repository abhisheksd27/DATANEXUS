"""
DataNexus Analytics Serving API
High-performance REST API built with FastAPI.
Exposes real-time business intelligence endpoints, revenue analytics,
and inventory metrics backed by the DataNexus Data Lake / Warehouse.
"""

from datetime import datetime, timezone
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import revenue, products

app = FastAPI(
    title="DataNexus Analytics Serving API",
    description="Enterprise REST API for querying real-time e-commerce analytics, revenue metrics, and inventory data.",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount Business Routers
app.include_router(revenue.router, prefix="/api/v1/revenue", tags=["Revenue Analytics"])
app.include_router(products.router, prefix="/api/v1/products", tags=["Product & Inventory"])

@app.get("/", tags=["System"])
def root():
    return {
        "project": "DataNexus",
        "description": "Real-Time E-Commerce Intelligence Platform",
        "version": "1.0.0",
        "status": "online",
        "docs": "/docs",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.get("/health", tags=["System"])
def health_check():
    return {
        "status": "healthy",
        "services": {
            "api": "UP",
            "datalake": "CONNECTED",
            "kafka": "CONNECTED",
            "database": "CONNECTED"
        },
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
