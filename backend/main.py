from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
from spatial import query_pois_within_radius, compute_metrics

app = FastAPI(title="Morocco Spatial Dashboard")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_PATH = "morocco_pois.db"

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

@app.get("/health")
def health():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) as total FROM pois")
    row = cur.fetchone()
    conn.close()
    return {"status": "ok", "total_pois": row["total"]}

@app.get("/categories")
def get_categories():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT super_category FROM pois WHERE super_category IS NOT NULL ORDER BY super_category")
    cats = [r["super_category"] for r in cur.fetchall()]
    cur.execute("SELECT DISTINCT fclass FROM pois WHERE fclass IS NOT NULL ORDER BY fclass")
    fclasses = [r["fclass"] for r in cur.fetchall()]
    conn.close()
    return {"super_categories": cats, "fclasses": fclasses}

from spatial import query_pois_within_radius, compute_metrics, get_map_pois

@app.get("/analyze")
def analyze(
    lat: float = Query(..., description="Latitude"),
    lon: float = Query(..., description="Longitude"),
):
    conn = get_db()
    metrics  = compute_metrics(lat, lon, conn)
    map_pois = get_map_pois(lat, lon, conn)
    conn.close()

    return {
        "center": {"lat": lat, "lon": lon},
        "metrics": metrics,
        "pois": map_pois,
    }
