import sqlite3
import math
from collections import Counter

DB_PATH = "morocco_pois.db"

# ─── Filter Configuration ─────────────────────────────────────────────────────

# fclasses completely excluded from ALL metrics AND map markers
FCLASS_BLACKLIST = {
    # Sports clutter
    "swimming_pool", "pitch", "track",
    # Micro-infrastructure
    "bench", "toilet", "waste_basket", "post_box", "telephone",
    "drinking_water", "vending_any", "vending_parking",
    "recycling", "waste_basket", "camera_surveillance",
    # Parking
    "parking", "parking_bicycle", "parking_multistorey", "parking_underground",
    # Geographic noise
    "locality", "suburb", "hamlet", "village", "town", "city",
    "region", "national_capital",
    # Natural features
    "peak", "cliff", "glacier", "spring", "waterfall", "volcano",
    "cave_entrance", "island", "beach",
    # Minor infrastructure
    "pylon", "gate", "lock_gate", "slipway", "weir", "dam",
}

# super_categories excluded ONLY from map markers (kept in metrics)
CATEGORY_MARKER_BLACKLIST = {
    "Natural Features",
    "Settlements",
    "Cemetery & Memorial",
}

# fclasses excluded ONLY from density/diversity metrics (too numerous, skew counts)
FCLASS_METRICS_BLACKLIST = FCLASS_BLACKLIST | {
    "picnic_site", "viewpoint", "wayside_cross", "wayside_shrine",
    "hunting_stand", "shelter",
}


# ─── Haversine Distance ────────────────────────────────────────────────────────

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


# ─── Bounding Box ─────────────────────────────────────────────────────────────

def bounding_box(lat: float, lon: float, radius_km: float):
    delta_lat = radius_km / 111.0
    delta_lon = radius_km / (111.0 * math.cos(math.radians(lat)))
    return (
        lat - delta_lat,
        lat + delta_lat,
        lon - delta_lon,
        lon + delta_lon,
    )


# ─── Core Spatial Query ───────────────────────────────────────────────────────

def query_pois_within_radius(
    lat: float,
    lon: float,
    radius_km: float,
    conn: sqlite3.Connection,
    apply_metrics_filter: bool = False,
) -> list[dict]:
    """
    RTree bounding box pre-filter → exact Haversine filter.
    apply_metrics_filter=True  → strips noisy fclasses for metric computation.
    apply_metrics_filter=False → returns raw results (for map markers, filtered separately).
    """
    min_lat, max_lat, min_lon, max_lon = bounding_box(lat, lon, radius_km)

    cur = conn.cursor()
    cur.execute("""
        SELECT p.id, p.name, p.fclass, p.super_category, p.latitude, p.longitude
        FROM pois p
        JOIN pois_rtree r ON p.id = r.id
        WHERE r.min_lat >= :min_lat
          AND r.max_lat <= :max_lat
          AND r.min_lon >= :min_lon
          AND r.max_lon <= :max_lon
    """, {
        "min_lat": min_lat, "max_lat": max_lat,
        "min_lon": min_lon, "max_lon": max_lon,
    })

    candidates = cur.fetchall()
    cols = [d[0] for d in cur.description]

    blacklist = FCLASS_METRICS_BLACKLIST if apply_metrics_filter else FCLASS_BLACKLIST

    results = []
    for row in candidates:
        poi = dict(zip(cols, row))
        if poi["latitude"] is None or poi["longitude"] is None:
            continue
        if poi["fclass"] in blacklist:
            continue
        dist = haversine_km(lat, lon, poi["latitude"], poi["longitude"])
        if dist <= radius_km:
            poi["distance_km"] = round(dist, 4)
            results.append(poi)

    return results


# ─── Metrics Computation ──────────────────────────────────────────────────────

def compute_metrics(lat: float, lon: float, conn: sqlite3.Connection) -> dict:
    # Use metrics-filtered POIs for all indicator computation
    pois_1km  = query_pois_within_radius(lat, lon, 1.0, conn, apply_metrics_filter=True)
    pois_400m = query_pois_within_radius(lat, lon, 0.4, conn, apply_metrics_filter=True)

    metrics = {}

    # A) POI Density
    metrics["poi_density_1km"] = len(pois_1km)

    # B) POI Diversity
    fclasses   = [p["fclass"] for p in pois_1km if p["fclass"]]
    categories = [p["super_category"] for p in pois_1km if p["super_category"]]

    metrics["n_poi_types"]      = len(set(fclasses))
    metrics["n_poi_categories"] = len(set(categories))
    metrics["entropy_fclass"]   = round(_shannon_entropy(fclasses), 4)
    metrics["entropy_category"] = round(_shannon_entropy(categories), 4)

    # C) Category Breakdown (1 km)
    cat_counts: dict[str, int] = {}
    for p in pois_1km:
        cat = p["super_category"]
        if cat:
            cat_counts[cat] = cat_counts.get(cat, 0) + 1

    for cat, count in cat_counts.items():
        safe_key = _safe_key(cat)
        metrics[f"density_{safe_key}"] = count

    # D) Accessibility Binary (400 m)
    ACC_TARGETS = [
        "bus_stop", "tram_stop", "pharmacy", "school", "park",
        "mall", "bank", "hospital", "supermarket", "restaurant",
        "cafe", "atm", "kindergarten", "clinic", "doctors",
        "fast_food", "fuel", "post_office", "police", "railway_station",
    ]
    fclasses_400m = {p["fclass"] for p in pois_400m if p["fclass"]}
    for target in ACC_TARGETS:
        metrics[f"ACC_{target}"] = 1 if target in fclasses_400m else 0

    # E) Nearest Distance per Category
    nearest: dict[str, float] = {}
    for p in pois_1km:
        cat = p["super_category"]
        if cat:
            d = p["distance_km"]
            if cat not in nearest or d < nearest[cat]:
                nearest[cat] = d

    for cat, dist in nearest.items():
        metrics[f"nearest_{_safe_key(cat)}_km"] = dist

    return metrics


# ─── Map POIs ─────────────────────────────────────────────────────────────────

def get_map_pois(lat: float, lon: float, conn: sqlite3.Connection, limit: int = 200) -> list[dict]:
    """
    Returns clean POIs for map rendering.
    Applies fclass blacklist + category marker blacklist.
    """
    pois = query_pois_within_radius(lat, lon, 1.0, conn, apply_metrics_filter=False)

    filtered = [
        p for p in pois
        if p.get("super_category") not in CATEGORY_MARKER_BLACKLIST
    ]

    filtered.sort(key=lambda x: x["distance_km"])

    return [
        {
            "name": p["name"] or p["fclass"],
            "lat": p["latitude"],
            "lon": p["longitude"],
            "fclass": p["fclass"],
            "category": p["super_category"],
            "distance_km": p["distance_km"],
        }
        for p in filtered[:limit]
    ]


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _safe_key(cat: str) -> str:
    return cat.replace(" ", "_").replace("&", "and")


def _shannon_entropy(values: list[str]) -> float:
    if not values:
        return 0.0
    total = len(values)
    counts = Counter(values)
    entropy = 0.0
    for count in counts.values():
        p = count / total
        if p > 0:
            entropy -= p * math.log2(p)
    return entropy