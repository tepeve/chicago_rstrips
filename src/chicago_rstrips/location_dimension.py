import hashlib
import json
import re
import pandas as pd
import ast


POINT_WKT_RE = re.compile(r"POINT\s*\(\s*([-\d\.]+)\s+([-\d\.]+)\s*\)", re.IGNORECASE)
PAIR_RE = re.compile(r"\(\s*([-\d\.]+)\s*,\s*([-\d\.]+)\s*\)")



def _parse_lon_lat(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None, None
    s = str(value).strip()
    if not s:
        return None, None
    try:
        obj = json.loads(s)
        if isinstance(obj, dict):
            lon = obj.get("longitude") or obj.get("lon")
            lat = obj.get("latitude") or obj.get("lat")
            if lon is not None and lat is not None:
                return float(lon), float(lat)
            # NUEVO: Si es GeoJSON
            if obj.get("type") == "Point" and isinstance(obj.get("coordinates"), list):
                lon, lat = obj["coordinates"][:2]
                return float(lon), float(lat)
    except Exception:
        # Intentar con ast.literal_eval si json falla
        try:
            obj = ast.literal_eval(s)
            if isinstance(obj, dict):
                lon = obj.get("longitude") or obj.get("lon")
                lat = obj.get("latitude") or obj.get("lat")
                if lon is not None and lat is not None:
                    return float(lon), float(lat)
                # Si es GeoJSON
                if obj.get("type") == "Point" and isinstance(obj.get("coordinates"), list):
                    lon, lat = obj["coordinates"][:2]
                    return float(lon), float(lat)
        except Exception:
            pass
    m = POINT_WKT_RE.search(s)
    if m:
        return float(m.group(1)), float(m.group(2))
    m = PAIR_RE.search(s)
    if m:
        return float(m.group(1)), float(m.group(2))
    parts = re.split(r"[\s,]+", s.replace("POINT", "").replace("(", "").replace(")", "").strip())
    if len(parts) >= 2:
        try:
            return float(parts[0]), float(parts[1])
        except Exception:
            return None, None
    return None, None

def _make_id(text: str, id_len: int = 16) -> str:
    if text is None:
        return None
    h = hashlib.sha1(str(text).encode("utf-8")).hexdigest()
    return h[:id_len]



def build_location_dimension(df: pd.DataFrame,
                             pickup_col: str = "pickup_centroid_location",
                             dropoff_col: str = "dropoff_centroid_location",
                             id_len: int = 16):
    vals = pd.concat([
        df.get(pickup_col, pd.Series(dtype="string")),
        df.get(dropoff_col, pd.Series(dtype="string")),
    ], ignore_index=True).dropna().astype("string").drop_duplicates()

    rows = []
    for txt in vals:
        lon, lat = _parse_lon_lat(txt)
        rows.append({
            "location_id": _make_id(txt, id_len=id_len),
            "original_text": str(txt),
            "longitude": lon,
            "latitude": lat,
            "source_type": "census_centroid"
        })
    dim_df = pd.DataFrame(rows).drop_duplicates(subset=["location_id"]).reset_index(drop=True)
    mapping = dict(zip(dim_df["original_text"], dim_df["location_id"]))
    return dim_df, mapping



def map_location_keys(df: pd.DataFrame,
                      mapping: dict,
                      pickup_col: str = "pickup_centroid_location",
                      dropoff_col: str = "dropoff_centroid_location"):
    trips_df = df.copy()
    if pickup_col in trips_df.columns:
        trips_df["pickup_location_id"] = trips_df[pickup_col].map(mapping).astype("string")
    else:
        trips_df["pickup_location_id"] = pd.Series([None]*len(trips_df), dtype="string")
    if dropoff_col in trips_df.columns:
        trips_df["dropoff_location_id"] = trips_df[dropoff_col].map(mapping).astype("string")
    else:
        trips_df["dropoff_location_id"] = pd.Series([None]*len(trips_df), dtype="string")
    for col in (pickup_col, dropoff_col):
        if col in trips_df.columns:
            trips_df = trips_df.drop(columns=[col])
    return trips_df



def update_location_dimension(existing_dim: pd.DataFrame,
                              new_df: pd.DataFrame,
                              pickup_col: str = "pickup_centroid_location",
                              dropoff_col: str = "dropoff_centroid_location",
                              id_len: int = 16):
    # Extraer textos nuevos
    incoming = pd.concat([
        new_df.get(pickup_col, pd.Series(dtype="string")),
        new_df.get(dropoff_col, pd.Series(dtype="string")),
    ], ignore_index=True).dropna().astype("string").drop_duplicates()

    if existing_dim is None or existing_dim.empty:
        return build_location_dimension(new_df, pickup_col, dropoff_col, id_len)

    existing_texts = set(existing_dim["original_text"].astype(str))
    to_add = [t for t in incoming if t not in existing_texts]

    if not to_add:
        # No hay novedades
        mapping = dict(zip(existing_dim["original_text"], existing_dim["location_id"]))
        return existing_dim, mapping

    rows = []
    for txt in to_add:
        lon, lat = _parse_lon_lat(txt)
        rows.append({
            "location_id": _make_id(txt, id_len=id_len),
            "original_text": str(txt),
            "longitude": lon,
            "latitude": lat,
            "source_type": "census_centroid"
        })
    add_df = pd.DataFrame(rows)
    updated = pd.concat([existing_dim, add_df], ignore_index=True)
    updated = updated.drop_duplicates(subset=["location_id"]).reset_index(drop=True)
    mapping = dict(zip(updated["original_text"], updated["location_id"]))
    return updated, mapping