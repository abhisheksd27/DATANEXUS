"""
FastAPI Router: Revenue Analytics
Exposes daily, city-level, and summarized financial metrics.
"""

from typing import List
from fastapi import APIRouter
from pydantic import BaseModel, Field

router = APIRouter()

class DailyRevenueResponse(BaseModel):
    order_date: str
    city: str
    gross_revenue: float
    avg_order_value: float
    total_orders: int
    completed_orders: int

class CityRevenueSummary(BaseModel):
    city: str
    total_revenue: float
    market_share_percent: float

class PlatformSummary(BaseModel):
    total_gross_revenue: float
    total_orders_placed: int
    overall_avg_order_value: float
    top_performing_city: str
    currency: str = "INR"

# Baseline analytical data matching Gold zone models
SAMPLE_DAILY_DATA = [
    {"order_date": "2026-08-18", "city": "MUMBAI", "gross_revenue": 669.99, "avg_order_value": 223.33, "total_orders": 3, "completed_orders": 3},
    {"order_date": "2026-08-18", "city": "BENGALURU", "gross_revenue": 89.50, "avg_order_value": 89.50, "total_orders": 1, "completed_orders": 0},
    {"order_date": "2026-08-18", "city": "DELHI", "gross_revenue": 299.00, "avg_order_value": 299.00, "total_orders": 1, "completed_orders": 1},
    {"order_date": "2026-08-18", "city": "HYDERABAD", "gross_revenue": 0.00, "avg_order_value": 0.00, "total_orders": 1, "completed_orders": 0},
    {"order_date": "2026-08-18", "city": "PUNE", "gross_revenue": 79.99, "avg_order_value": 79.99, "total_orders": 1, "completed_orders": 1},
    {"order_date": "2026-08-18", "city": "CHENNAI", "gross_revenue": 199.50, "avg_order_value": 199.50, "total_orders": 1, "completed_orders": 1}
]

@router.get("/daily", response_model=List[DailyRevenueResponse], summary="Get Daily Revenue by City")
def get_daily_revenue():
    """Returns daily revenue breakdown across delivery hubs."""
    return SAMPLE_DAILY_DATA

@router.get("/by-city", response_model=List[CityRevenueSummary], summary="Get Aggregated Revenue by City")
def get_revenue_by_city():
    """Returns total revenue and market share breakdown by city."""
    total = sum(d["gross_revenue"] for d in SAMPLE_DAILY_DATA)
    city_totals = {}
    for d in SAMPLE_DAILY_DATA:
        city = d["city"]
        city_totals[city] = city_totals.get(city, 0.0) + d["gross_revenue"]

    return [
        {
            "city": city,
            "total_revenue": round(rev, 2),
            "market_share_percent": round((rev / total * 100), 1) if total > 0 else 0.0
        }
        for city, rev in sorted(city_totals.items(), key=lambda x: x[1], reverse=True)
    ]

@router.get("/summary", response_model=PlatformSummary, summary="Get Platform Financial Overview")
def get_platform_summary():
    """Returns high-level KPI summary for executive dashboards."""
    total_rev = sum(d["gross_revenue"] for d in SAMPLE_DAILY_DATA)
    total_orders = sum(d["total_orders"] for d in SAMPLE_DAILY_DATA)
    aov = round(total_rev / total_orders, 2) if total_orders > 0 else 0.0

    return {
        "total_gross_revenue": round(total_rev, 2),
        "total_orders_placed": total_orders,
        "overall_avg_order_value": aov,
        "top_performing_city": "MUMBAI",
        "currency": "INR"
    }
