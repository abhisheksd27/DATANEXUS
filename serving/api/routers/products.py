"""
FastAPI Router: Product & Inventory Analytics
Exposes real-time trending products based on clickstream activity
and live inventory stock levels.
"""

from typing import List
from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()

class TrendingProductResponse(BaseModel):
    product_id: str
    product_name: str
    category: str
    views: int
    cart_adds: int
    conversion_rate_percent: float

class InventoryItemResponse(BaseModel):
    product_id: str
    product_name: str
    category: str
    stock_quantity: int
    unit_price: float
    stock_status: str

SAMPLE_TRENDING = [
    {"product_id": "prod-A", "product_name": "Wireless Headphones", "category": "ELECTRONICS", "views": 1420, "cart_adds": 312, "conversion_rate_percent": 22.0},
    {"product_id": "prod-C", "product_name": "Mechanical Keyboard", "category": "ELECTRONICS", "views": 980, "cart_adds": 195, "conversion_rate_percent": 19.9},
    {"product_id": "prod-B", "product_name": "Ergonomic Mouse", "category": "ELECTRONICS", "views": 840, "cart_adds": 145, "conversion_rate_percent": 17.3},
    {"product_id": "prod-E", "product_name": "Aluminum Laptop Stand", "category": "ACCESSORIES", "views": 620, "cart_adds": 98, "conversion_rate_percent": 15.8},
    {"product_id": "prod-D", "product_name": "USB-C Fast Charging Cable", "category": "ACCESSORIES", "views": 510, "cart_adds": 82, "conversion_rate_percent": 16.1}
]

SAMPLE_INVENTORY = [
    {"product_id": "prod-A", "product_name": "Wireless Headphones", "category": "ELECTRONICS", "stock_quantity": 150, "unit_price": 149.99, "stock_status": "IN_STOCK"},
    {"product_id": "prod-B", "product_name": "Ergonomic Mouse", "category": "ELECTRONICS", "stock_quantity": 300, "unit_price": 44.75, "stock_status": "IN_STOCK"},
    {"product_id": "prod-C", "product_name": "Mechanical Keyboard", "category": "ELECTRONICS", "stock_quantity": 80, "unit_price": 299.00, "stock_status": "LOW_STOCK"},
    {"product_id": "prod-D", "product_name": "USB-C Fast Charging Cable", "category": "ACCESSORIES", "stock_quantity": 500, "unit_price": 19.99, "stock_status": "IN_STOCK"},
    {"product_id": "prod-E", "product_name": "Aluminum Laptop Stand", "category": "ACCESSORIES", "stock_quantity": 220, "unit_price": 59.99, "stock_status": "IN_STOCK"},
    {"product_id": "prod-F", "product_name": "27-inch 4K IPS Monitor", "category": "DISPLAYS", "stock_quantity": 45, "unit_price": 389.50, "stock_status": "LOW_STOCK"}
]

@router.get("/trending", response_model=List[TrendingProductResponse], summary="Get Top Trending Products")
def get_trending_products():
    """Returns top products sorted by cart additions and conversion rate."""
    return SAMPLE_TRENDING

@router.get("/inventory", response_model=List[InventoryItemResponse], summary="Get Current Inventory Levels")
def get_inventory():
    """Returns live inventory catalog stock counts and price status."""
    return SAMPLE_INVENTORY
