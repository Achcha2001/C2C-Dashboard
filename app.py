from flask import Flask, render_template, request, redirect, url_for, abort, flash, jsonify
import os, re, time, threading, hashlib
from io import BytesIO
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from werkzeug.utils import secure_filename

# NEW: analytics builder for the /analytics page
from analytics_data import build_analytics

app = Flask(__name__)
app.secret_key = "mmbl-c2c"

# ---------------- Storage ----------------
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
UPLOAD_ROOT = os.path.join(BASE_DIR, "uploads")
BRANDS = ("wu", "ria", "mg")
ALLOWED_EXT = {".xlsx", ".xls", ".csv"}
os.makedirs(UPLOAD_ROOT, exist_ok=True)
for b in BRANDS:
    os.makedirs(os.path.join(UPLOAD_ROOT, b), exist_ok=True)

# =================== PERFORMANCE: CACHING LAYER ===================
# - File DF cache:   path -> (mtime, df)
# - Brand/period DF: (brand, period_key|"All") -> (signature, df)
# - Upload lists:    brand -> (expires_at_ts, files_list, signature)

_DF_FILE_CACHE: dict[str, tuple[float, pd.DataFrame]] = {}
_BRAND_PERIOD_CACHE: dict[tuple[str, str], tuple[str, pd.DataFrame]] = {}
_UPLOADS_CACHE: dict[str, tuple[float, list, str]] = {}
_LOCK = threading.Lock()

# Tunables
UPLOAD_LIST_TTL_SEC = 8            # avoid re-scanning directories many times per page
MAX_FILE_CACHE = 120               # cap number of parsed files kept in memory

def _dir_signature(files):
    """Return a small signature string for a files listing (names+mtimes+sizes)."""
    h = hashlib.sha1()
    for f in files:
        # tuple of essential props (name, mtime int, size)
        h.update(f["name"].encode("utf-8", "ignore"))
        h.update(str(int(f["mtime"].timestamp())).encode())
        h.update(str(int(f["size"])).encode())
    return h.hexdigest()

def _prune_file_cache_if_needed():
    # very simple LRU-ish prune: drop ~25% oldest when over cap
    if len(_DF_FILE_CACHE) <= MAX_FILE_CACHE:
        return
    items = sorted(_DF_FILE_CACHE.items(), key=lambda kv: kv[1][0])  # by mtime asc
    drop = max(1, len(items) // 4)
    for k, _ in items[:drop]:
        _DF_FILE_CACHE.pop(k, None)

def _invalidate_brand_caches(brand: str | None = None):
    """Clear caches after upload/delete to keep things consistent."""
    with _LOCK:
        if brand:
            # purge uploads cache for brand
            _UPLOADS_CACHE.pop(brand, None)
            # purge brand-period cache entries for this brand
            for key in list(_BRAND_PERIOD_CACHE.keys()):
                if key[0] == brand:
                    _BRAND_PERIOD_CACHE.pop(key, None)
            # purge file cache entries under brand dir
            brand_dir = os.path.join(UPLOAD_ROOT, brand) + os.sep
            for path in list(_DF_FILE_CACHE.keys()):
                if path.startswith(brand_dir):
                    _DF_FILE_CACHE.pop(path, None)
        else:
            _UPLOADS_CACHE.clear()
            _BRAND_PERIOD_CACHE.clear()
            _DF_FILE_CACHE.clear()

# --------------- ZONES & BDOs (authoritative order/mapping) ---------------
ZONES_DEF = [
    {
        "zone_key": "Z1",
        "territory": "ZONE 1",
        "title": "Z1 - WESTERN / NORTH WESTERN / NORTH CENTRAL",
        "zm": "AJANTHA ROSHAN",
        "bdos": {
            "Z1B1": "CHINTHAKA",
            "Z1B2": "AJANTHA",
            "Z1B3": "AJANTHA",
            "Z3B4": "AJANTHA",
            "Z3B5": "AJANTHA",
            "Z3B6": "AJANTHA",
        },
    },
    {
        "zone_key": "Z2",
        "territory": "ZONE 2",
        "title": "Z2 - NORTH , EAST , & CENTRAL",
        "zm": "SRIDHAR",
        "bdos": {
            "Z2B1": "JENOJAN",
            "Z2B2": "JENOJAN",
            "Z2B4": "LAREEF",
            "Z2B3": "NUWAIS",
            "Z2B5": "NUWAIS",
            "Z3B2": "SRIDHAR",
            "Z3B3": "SRIDHAR",
            "Z3B1": "SRIDHAR",
        },
    },
    {
        "zone_key": "Z3",
        "territory": "ZONE 3",
        "title": "Z3 - UVA / SOUTHERN / SABARAGAMUWA / WESTERN",
        "zm": "CHATHURANGA",
        "bdos": {
            "Z3B7": "CHATHURANGA",
            "Z3B8": "CHATHURANGA",
            "Z4B1": "CHATHURANGA",
            "Z4B5": "DANANJAYA",
            "Z4B6": "DANANJAYA",
            "Z4B3": "LAKMAL",
            "Z4B4": "LAKMAL",
            "Z4B2": "MADURANGA",
        },
    },
]
BDO_TO_ZONE, BDO_NAME_CANON = {}, {}
for z in ZONES_DEF:
    for code, nm in z["bdos"].items():
        BDO_TO_ZONE[code] = z
        if nm:
            BDO_NAME_CANON[code] = nm

# ---------- Avatar URL helper ----------
def _avatar_url_for(name: str | None) -> str:
    from flask import url_for  # local import to avoid circular on startup
    nm = (name or "").strip().lower()
    slug = re.sub(r"[^a-z0-9]+", "", nm)
    avatars_dir = os.path.join(BASE_DIR, "static", "avatars")
    if slug:
        for ext in ("png", "jpg", "jpeg"):
            p = os.path.join(avatars_dir, f"{slug}.{ext}")
            if os.path.exists(p):
                return url_for("static", filename=f"avatars/{slug}.{ext}")
    return url_for("static", filename="avatars/default.png")

@app.context_processor
def inject_avatar_src():
    return {"avatar_src": _avatar_url_for}

# --------------- Month helpers ---------------
MONTH_FULL = ["January","February","March","April","May","June","July","August","September","October","November","December"]
MONTH_ABBR  = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
MONTH_LOOKUP = {m.lower(): i+1 for i, m in enumerate(MONTH_FULL)}
MONTH_LOOKUP.update({m.lower(): i+1 for i, m in enumerate(MONTH_ABBR)})

def parse_period_from_filename(name: str):
    base = os.path.basename(name).lower()
    for key, mnum in MONTH_LOOKUP.items():
        if key in base:
            y = re.search(r"(20\d{2})", base)
            if y: return int(y.group(1)), mnum
    m = re.search(r"(20\d{2})[-_ ](1[0-2]|0?[1-9])", base)
    if m: return int(m.group(1)), int(m.group(2))
    m = re.search(r"(1[0-2]|0?[1-9])[-_ ](20\d{2})", base)
    if m: return int(m.group(2)), int(m.group(1))
    return None, None

def period_key(y, m):   return f"{y}-{m:02d}"
def period_label(y, m): return f"{MONTH_FULL[m-1]} {y}"

def parse_period_key(key: str):
    try:
        y, m = key.split("-")
        return int(y), int(m)
    except Exception:
        return None, None

# --------------- File readers ---------------
HEADER_HINTS = [
    "MTCN","Name of BDO","BDO","Name of Sales Rep","Sales Rep",
    "City","State","Province","District","Amount","Transaction Amount","Total"
]

def read_any_table_bytes(raw: bytes, filename: str) -> pd.DataFrame:
    fn = (filename or "").lower()
    if not raw: return pd.DataFrame()
    try:
        if fn.endswith(".csv"):
            preview = pd.read_csv(BytesIO(raw), header=None, dtype=str, nrows=40)
        else:
            preview = pd.read_excel(BytesIO(raw), header=None, dtype=str, nrows=40, engine="openpyxl")
    except Exception:
        return pd.DataFrame()

    header_row = 0
    for i, row in preview.iterrows():
        vals = [str(x).strip() for x in row.to_list()]
        if any(h in vals for h in HEADER_HINTS):
            header_row = i
            break

    try:
        if fn.endswith(".csv"):
            df = pd.read_csv(BytesIO(raw), header=header_row, dtype=str)
        else:
            df = pd.read_excel(BytesIO(raw), header=header_row, dtype=str, engine="openpyxl")
    except Exception:
        return pd.DataFrame()

    df = df.loc[:, ~df.columns.astype(str).str.contains(r"^Unnamed", na=False, case=False)]
    df.columns = [str(c).strip() for c in df.columns]
    return df

def _read_any_table_path_cached(path: str) -> pd.DataFrame:
    """Cache parsed files by path+mtime."""
    try:
        mtime = os.path.getmtime(path)
    except FileNotFoundError:
        return pd.DataFrame()
    with _LOCK:
        cached = _DF_FILE_CACHE.get(path)
        if cached and cached[0] == mtime:
            return cached[1]
    # miss
    with open(path, "rb") as f:
        raw = f.read()
    df = read_any_table_bytes(raw, path)
    with _LOCK:
        _DF_FILE_CACHE[path] = (mtime, df)
        _prune_file_cache_if_needed()
    return df

def read_any_table_path(path: str) -> pd.DataFrame:
    # keep name for backwards compatibility; now cached
    return _read_any_table_path_cached(path)

def find_col(df, candidates):
    cols = {c.lower().replace(" ", ""): c for c in df.columns}
    for cand in candidates:
        key = cand.lower().replace(" ", "")
        if key in cols: return cols[key]
    for c in df.columns:
        if any(x.lower().replace(" ", "") in c.lower().replace(" ", "") for x in candidates):
            return c
    return None

def value_series(df):
    amt  = find_col(df, ["Amount","Transaction Amount","Total Amount","Value","Total"])
    if amt:
        # strip commas fast
        s = df[amt].astype(str).str.replace(",", "", regex=False)
        return pd.to_numeric(s, errors="coerce").fillna(0.0)
    return pd.Series(1.0, index=df.index)

# --------------- Upload inventory (with TTL cache) ---------------
def _list_uploads_uncached(brand: str):
    bdir = os.path.join(UPLOAD_ROOT, brand)
    out = []
    for name in os.listdir(bdir):
        path = os.path.join(bdir, name)
        if not os.path.isfile(path): 
            continue
        try:
            size  = os.path.getsize(path)
            mtime = datetime.fromtimestamp(os.path.getmtime(path))
        except OSError:
            continue
        y, m  = parse_period_from_filename(name)
        out.append({"name": name, "path": path, "size": size, "mtime": mtime, "year": y, "month": m})
    out.sort(key=lambda x: x["mtime"], reverse=True)
    return out

def list_uploads(brand: str):
    now = time.time()
    with _LOCK:
        cached = _UPLOADS_CACHE.get(brand)
        if cached and cached[0] > now:
            return cached[1]
    files = _list_uploads_uncached(brand)
    sig = _dir_signature(files)
    with _LOCK:
        _UPLOADS_CACHE[brand] = (now + UPLOAD_LIST_TTL_SEC, files, sig)
    return files

def _uploads_signature(brand: str) -> str:
    # Ensure we sync the cache and return the current signature
    files = list_uploads(brand)
    with _LOCK:
        return _UPLOADS_CACHE.get(brand, (0, files, _dir_signature(files)))[2]

def available_periods():
    seen, items = set(), []
    for b in BRANDS:
        for f in list_uploads(b):
            if f["year"] and f["month"]:
                k = period_key(f["year"], f["month"])
                if k not in seen:
                    seen.add(k)
                    items.append({"key": k, "year": f["year"], "month": f["month"], "label": period_label(f["year"], f["month"])})
    items.sort(key=lambda x: (x["year"], x["month"]), reverse=True)
    return items

# ---------- Period collectors (cached) ----------
def _collect_brand_df_for_period_cached(brand: str, period_key_str: str) -> pd.DataFrame:
    """
    period_key_str: "YYYY-MM" or "All"
    """
    sig = _uploads_signature(brand)
    cache_key = (brand, period_key_str)
    with _LOCK:
        hit = _BRAND_PERIOD_CACHE.get(cache_key)
        if hit and hit[0] == sig:
            return hit[1]

    files = list_uploads(brand)
    frames = []
    if period_key_str == "All":
        for f in files:
            df = _read_any_table_path_cached(f["path"])
            if not df.empty:
                frames.append(df)
    else:
        y, m = parse_period_key(period_key_str)
        for f in files:
            if f["year"] == y and f["month"] == m:
                df = _read_any_table_path_cached(f["path"])
                if not df.empty:
                    frames.append(df)
    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    with _LOCK:
        _BRAND_PERIOD_CACHE[cache_key] = (sig, out)
    return out

def collect_brand_df_for_period(brand: str, y: int, m: int) -> pd.DataFrame:
    return _collect_brand_df_for_period_cached(brand, period_key(y, m))

def collect_all_brand_df(brand: str) -> pd.DataFrame:
    return _collect_brand_df_for_period_cached(brand, "All")

def collect_df_for_period_key(brand: str, p: str) -> pd.DataFrame:
    if not p or str(p).strip().lower() == "all":
        return collect_all_brand_df(brand)
    y, m = parse_period_key(p)
    if y and m:
        return collect_brand_df_for_period(brand, y, m)
    return pd.DataFrame()

# ---------- ZONE DISTRICT RULES ----------
# - Z1 forbids MATARA entirely
# - BDO Z1B2 is only counted for district GAMPAHA
# - Z2B2 rows for VAVUNIYA/MANNAR -> reassign to Z3B1 (Sridhar)
# - Hide VAVUNIYA/MANNAR from Z2B2, and GALLE from Z4B2
ZONE_RULES = {
    "Z1": {
        "forbid_districts": {"MATARA"},
        "bdo_allow_districts": {"Z1B2": {"GAMPAHA"}},
    },
    "Z2": {
        "bdo_reassign": [
            {"from": "Z2B2", "to": "Z3B1", "districts": {"VAVUNIYA", "MANNAR"}},
        ],
        "bdo_forbid_districts": {"Z2B2": {"VAVUNIYA", "MANNAR"}},
    },
    "Z3": {
        "bdo_forbid_districts": {"Z4B2": {"GALLE"}},
    },
}

def _norm_place(s: str) -> str:
    s = (s or "").upper().strip()
    s = re.sub(r"\bDISTRICT\b", "", s)
    s = re.sub(r"[^A-Z]", "", s)
    return s

def _apply_reassignments(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    dcol = find_col(df, ["State","Province","District","Region"])
    bcol = find_col(df, ["BDO","Name of BDO"])
    if not dcol or not bcol:
        return df
    out = df.copy()
    for rules in ZONE_RULES.values():
        for r in rules.get("bdo_reassign", []):
            frm = str(r.get("from","")).upper()
            to  = str(r.get("to","")).upper()
            dset = { _norm_place(x) for x in r.get("districts", set()) }
            if not (frm and to and dset):
                continue
            sel = out[bcol].astype(str).str.upper().eq(frm) & out[dcol].fillna("").map(_norm_place).isin(dset)
            out.loc[sel, bcol] = to
    return out

def _apply_zone_constraints(df: pd.DataFrame, zone_key: str|None) -> pd.DataFrame:
    if df is None or df.empty or not zone_key or zone_key not in ZONE_RULES:
        return df
    dcol = find_col(df, ["State","Province","District","Region"])
    bcol = find_col(df, ["BDO","Name of BDO"])
    if not dcol:
        return df

    rules = ZONE_RULES[zone_key]
    out = df.copy()

    forbid = { _norm_place(x) for x in rules.get("forbid_districts", set()) }
    if forbid:
        out = out[out[dcol].fillna("").map(_norm_place).apply(lambda x: x not in forbid)]

    allow_map = { code: { _norm_place(x) for x in vals }
                  for code, vals in rules.get("bdo_allow_districts", {}).items() }
    if bcol and allow_map:
        mask = pd.Series(True, index=out.index)
        for code, allow_set in allow_map.items():
            sel = out[bcol].astype(str).str.upper().eq(code.upper())
            if allow_set:
                mask &= ~sel | out[dcol].fillna("").map(_norm_place).isin(allow_set)
            else:
                mask &= ~sel
        out = out[mask]

    forbid_map = { code: { _norm_place(x) for x in vals }
                   for code, vals in rules.get("bdo_forbid_districts", {}).items() }
    if bcol and forbid_map:
        mask = pd.Series(True, index=out.index)
        for code, forbids in forbid_map.items():
            sel = out[bcol].astype(str).str.upper().eq(code.upper())
            mask &= ~sel | (~out[dcol].fillna("").map(_norm_place).isin(forbids))
        out = out[mask]

    return out

def _pair_allowed(zone_key: str|None, bdo_code: str, district: str) -> bool:
    if not zone_key or zone_key not in ZONE_RULES:
        return True
    rules = ZONE_RULES[zone_key]
    nd = _norm_place(district)
    if nd in { _norm_place(x) for x in rules.get("forbid_districts", set()) }:
        return False
    allow_map = { k.upper(): { _norm_place(x) for x in v }
                  for k, v in rules.get("bdo_allow_districts", {}).items() }
    if bdo_code.upper() in allow_map:
        return nd in allow_map[bdo_code.upper()]
    forbid_map = { k.upper(): { _norm_place(x) for x in v }
                   for k, v in rules.get("bdo_forbid_districts", {}).items() }
    if bdo_code.upper() in forbid_map and nd in forbid_map[bdo_code.upper()]:
        return False
    return True

# ---------- Aggregation per BDO + DISTRICT ----------
def agg_bdo_district(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["BDO","District","value"])
    df = _apply_reassignments(df)
    bdo = find_col(df, ["BDO","Name of BDO"])
    district = find_col(df, ["State","Province","District","Region"])
    tmp = pd.DataFrame({
        "BDO": df[bdo] if bdo else "(—)",
        "District": df[district] if district else "",
        "value": value_series(df)
    })
    return tmp.groupby(["BDO","District"], dropna=False)["value"].sum().reset_index()

def merge_two_periods_geo(d1: pd.DataFrame, d2: pd.DataFrame) -> pd.DataFrame:
    a = d1.rename(columns={"value":"v1"})
    b = d2.rename(columns={"value":"v2"})
    return pd.merge(a, b, on=["BDO","District"], how="outer").fillna(0.0)

def growth(v1, v2):
    v1 = float(v1 or 0.0); v2 = float(v2 or 0.0)
    if v2 == 0:
        return "0%" if v1 == 0 else "#DIV/0!"
    pct = ((v1 - v2) / v2) * 100.0
    return f"{int(round(pct))}%"

# ---------- Build row-spanned blocks (dashboard) ----------
def build_zone_blocks(periodA, periodB):
    y1, m1 = periodA; y2, m2 = periodB

    brand_geo = {}
    for lbl, key in [("WU","wu"),("RIA","ria"),("MG","mg")]:
        d1 = agg_bdo_district(collect_brand_df_for_period(key, y1, m1))
        d2 = agg_bdo_district(collect_brand_df_for_period(key, y2, m2))
        brand_geo[lbl] = merge_two_periods_geo(d1, d2)

    def get_vals(lbl, bdo, district):
        g = brand_geo[lbl]
        hit = g[(g["BDO"].astype(str)==str(bdo)) & (g["District"].astype(str)==str(district))]
        if hit.empty:
            return 0.0, 0.0
        r = hit.iloc[0]
        return float(r["v1"]), float(r["v2"])

    rows_by_zone = []
    grand_acc = {"WU1":0,"WU2":0,"RIA1":0,"RIA2":0,"MG1":0,"MG2":0}

    for z in ZONES_DEF:
        zone_rows = []
        for bdo, rep_name in z["bdos"].items():
            states = set()
            for lbl in ("WU","RIA","MG"):
                g = brand_geo[lbl]
                states.update(g[g["BDO"].astype(str)==str(bdo)]["District"].astype(str).tolist())
            if not states:
                states = {"—"}

            filtered_states = [s for s in states if _pair_allowed(z["zone_key"], bdo, s)]
            if not filtered_states:
                continue

            ordered_states = [s for s in filtered_states if s and s != "—"] + (["—"] if "—" in filtered_states else [])

            for st in ordered_states:
                wu1, wu2 = get_vals("WU", bdo, st)
                ria1, ria2 = get_vals("RIA", bdo, st)
                mg1, mg2  = get_vals("MG", bdo, st)

                zone_rows.append({
                    "territory": z["territory"],
                    "bdo": bdo,
                    "name": BDO_NAME_CANON.get(bdo, rep_name or ""),
                    "district": st,
                    "WU":   {"a": int(round(wu1)), "b": int(round(wu2)), "g": growth(wu1, wu2)},
                    "RIA":  {"a": int(round(ria1)), "b": int(round(ria2)), "g": growth(ria1, ria2)},
                    "MG":   {"a": int(round(mg1)),  "b": int(round(mg2)),  "g": growth(mg1,  mg2)},
                    "TOTAL":{"a": int(round(wu1+ria1+mg1)), "b": int(round(wu2+ria2+mg2)), "g": growth(wu1+ria1+mg1, wu2+ria2+mg2)},
                })

                grand_acc["WU1"] += wu1; grand_acc["WU2"] += wu2
                grand_acc["RIA1"] += ria1; grand_acc["RIA2"] += ria2
                grand_acc["MG1"]  += mg1;  grand_acc["MG2"]  += mg2

        if zone_rows:
            zone_rows[0]["show_territory"] = True
            zone_rows[0]["territory_span"] = len(zone_rows)
        for i in range(1, len(zone_rows)):
            zone_rows[i]["show_territory"] = False

        i = 0
        while i < len(zone_rows):
            j = i + 1
            while j < len(zone_rows) and zone_rows[j]["name"] == zone_rows[i]["name"]:
                j += 1
            span = j - i
            zone_rows[i]["show_name"] = True
            zone_rows[i]["name_span"] = span
            for k in range(i+1, j):
                zone_rows[k]["show_name"] = False
            i = j

        sub = {"WU_a":0,"WU_b":0,"RIA_a":0,"RIA_b":0,"MG_a":0,"MG_b":0,"T_a":0,"T_b":0}
        for r in zone_rows:
            sub["WU_a"] += r["WU"]["a"]; sub["WU_b"] += r["WU"]["b"]
            sub["RIA_a"]+= r["RIA"]["a"]; sub["RIA_b"]+= r["RIA"]["b"]
            sub["MG_a"] += r["MG"]["a"];  sub["MG_b"] += r["MG"]["b"]
            sub["T_a"]  += r["TOTAL"]["a"]; sub["T_b"] += r["TOTAL"]["b"]
        sub["WU_g"]=growth(sub["WU_a"],sub["WU_b"])
        sub["RIA_g"]=growth(sub["RIA_a"],sub["RIA_b"])
        sub["MG_g"]=growth(sub["MG_a"],sub["MG_b"])
        sub["T_g"]=growth(sub["T_a"],sub["T_b"])

        rows_by_zone.append({"meta": z, "rows": zone_rows, "subtotal": sub})

    grand = {
        "WU_a": int(round(grand_acc["WU1"])), "WU_b": int(round(grand_acc["WU2"])),
        "WU_g": growth(grand_acc["WU1"], grand_acc["WU2"]),
        "RIA_a": int(round(grand_acc["RIA1"])), "RIA_b": int(round(grand_acc["RIA2"])),
        "RIA_g": growth(grand_acc["RIA1"], grand_acc["RIA2"]),
        "MG_a":  int(round(grand_acc["MG1"])),  "MG_b":  int(round(grand_acc["MG2"])),
        "MG_g": growth(grand_acc["MG1"], grand_acc["MG2"]),
        "T_a": int(round(grand_acc["WU1"]+grand_acc["RIA1"]+grand_acc["MG1"])),
        "T_b": int(round(grand_acc["WU2"]+grand_acc["RIA2"]+grand_acc["MG2"])),
        "T_g": growth(grand_acc["WU1"]+grand_acc["RIA1"]+grand_acc["MG1"],
                      grand_acc["WU2"]+grand_acc["RIA2"]+grand_acc["MG2"]),
    }

    return rows_by_zone, grand

# ======== Generic comparison helpers (ZM / BDO / Agent) ========
def _get_zone_by_any(key: str):
    if not key: return None
    k = str(key).strip().lower()
    for z in ZONES_DEF:
        if z["zone_key"].lower() == k or z["zm"].strip().lower() == k:
            return z
    return None

def _collect_scope_df(brand_key: str, y: int, m: int, scope: str, key: str, bdo_filter: str|None=None) -> pd.DataFrame:
    df = collect_brand_df_for_period(brand_key, y, m)
    if df.empty:
        return df

    df = _apply_reassignments(df)

    bcol = find_col(df, ["BDO","Name of BDO"])
    rcol = find_col(df, ["Name of Sales Rep","Sales Rep","Agent"])

    if scope == "zm":
        z = _get_zone_by_any(key)
        if not z or not bcol: 
            return df.iloc[0:0]
        allowed = set(z["bdos"].keys())
        df = df[df[bcol].astype(str).isin(allowed)]
        df = _apply_zone_constraints(df, z["zone_key"])
    elif scope == "bdo":
        if not bcol: 
            return df.iloc[0:0]
        df = df[df[bcol].astype(str) == str(key)]
    elif scope == "agent":
        if bdo_filter and bcol:
            df = df[df[bcol].astype(str) == str(bdo_filter)]
    else:
        return df.iloc[0:0]

    return df

def _group_for_scope(df: pd.DataFrame, scope: str) -> tuple[pd.DataFrame, str]:
    if df.empty:
        return pd.DataFrame(columns=["label","value"]), "label"
    bcol = find_col(df, ["BDO","Name of BDO"])
    rcol = find_col(df, ["Name of Sales Rep","Sales Rep","Agent"])

    if scope == "zm":
        grp = bcol or "BDO"
        label = df[grp]
    elif scope in ("bdo", "agent"):
        grp = rcol or (bcol or "BDO")
        label = df[grp] if grp in df.columns else pd.Series([""]*len(df))
    else:
        grp = "x"
        label = pd.Series([""]*len(df))

    out = pd.DataFrame({"label": label, "value": value_series(df)})
    out = out.groupby("label", dropna=False)["value"].sum().reset_index()
    out["label"] = out["label"].fillna("").astype(str)
    return out, "label"

def _merge_compare(a: pd.DataFrame, b: pd.DataFrame) -> pd.DataFrame:
    a = a.rename(columns={"value":"v1"})
    b = b.rename(columns={"value":"v2"})
    m = pd.merge(a, b, on="label", how="outer").fillna(0.0)
    m["p1"] = m["v1"].astype(float).round().astype(int)
    m["p2"] = m["v2"].astype(float).round().astype(int)
    m["growth"] = [growth(x, y) for x, y in zip(m["v1"], m["v2"])]
    m = m.drop(columns=["v1","v2"])
    m = m.sort_values(by="p1", ascending=False, kind="mergesort").reset_index(drop=True)
    return m

def _brand_totals_for_scope(y: int, m: int, scope: str, key: str, bdo_filter: str|None=None):
    totals = {}
    overall = 0.0
    for code, brand_key in (("WU","wu"),("RIA","ria"),("MG","mg")):
        df = _collect_scope_df(brand_key, y, m, scope, key, bdo_filter)
        v = float(value_series(df).sum()) if not df.empty else 0.0
        totals[code] = int(round(v))
        overall += v
    totals["TOTAL"] = int(round(overall))
    return totals

# ---------- Helpers for name/agent handling ----------
def _looks_numeric_series(s: pd.Series) -> bool:
    if s is None or s.empty: return False
    ss = s.astype(str).str.strip()
    ss = ss[ss.astype(bool)]
    if ss.empty: return False
    return (ss.str.fullmatch(r"\d+")).mean() >= 0.7

def _name_series(df: pd.DataFrame) -> pd.Series:
    cands = ["Name of BDO","BDO Name","Agent Name","Name of Agent",
             "Sales Rep","Name of Sales Rep","Representative","Agent"]
    col = find_col(df, cands)
    if not col:
        return pd.Series([""]*len(df))
    s = df[col].fillna("").astype(str)
    if _looks_numeric_series(s):
        alt = find_col(df, ["Name of BDO","BDO Name"])
        if alt and alt != col:
            s = df[alt].fillna("").astype(str)
    return s

# ---------------- ROUTES ----------------
@app.route("/")
def dashboard():
    periods = available_periods()
    if not periods:
        analytics = type("A", (), {})()
        analytics.brand_totals = type("B", (), {"WU":0,"RIA":0,"MG":0})
        analytics.grand_total = 0
        return render_template("dashboard.html", active="dashboard", year=datetime.now().year,
                               analytics=analytics, periods=[], p1=None, p2=None,
                               blocks=[], grand=None, y1=None, y2=None)

    p1_key = request.args.get("p1") or periods[0]["key"]
    p2_key = request.args.get("p2") or (periods[1]["key"] if len(periods) > 1 else periods[0]["key"])
    p1 = next(x for x in periods if x["key"] == p1_key)
    p2 = next(x for x in periods if x["key"] == p2_key)

    blocks, grand = build_zone_blocks((p1["year"], p1["month"]), (p2["year"], p2["month"]))

    wu_a, ria_a, mg_a = grand["WU_a"], grand["RIA_a"], grand["MG_a"]
    analytics = type("A", (), {})()
    analytics.brand_totals = type("B", (), {"WU": wu_a, "RIA": ria_a, "MG": mg_a})
    analytics.grand_total = wu_a + ria_a + mg_a

    return render_template("dashboard.html",
                           active="dashboard",
                           year=datetime.now().year,
                           analytics=analytics,
                           periods=periods,
                           p1=p1, p2=p2,
                           y1=p1["year"], y2=p2["year"],
                           blocks=blocks, grand=grand)

# ---------------- Analytics ----------------
@app.route("/analytics")
def analytics():
    result = build_analytics(UPLOAD_ROOT)
    tables = result["tables"]
    charts_json = result["charts_json"]

    totals = charts_json.get("brandBar", {}).get("values", [0,0,0])
    brand_totals = {"WU": float(totals[0] if len(totals) > 0 else 0.0),
                    "RIA": float(totals[1] if len(totals) > 1 else 0.0),
                    "MG": float(totals[2] if len(totals) > 2 else 0.0)}
    analytics_obj = type("A", (), {})()
    analytics_obj.brand_totals = type("B", (), brand_totals)
    analytics_obj.grand_total = sum(brand_totals.values())

    return render_template(
        "analytics.html",
        active="analytics",
        year=datetime.now().year,
        tables=tables,
        charts_json=charts_json,
        analytics=analytics_obj
    )

# ---------------- Uploads ----------------
@app.route("/uploads/<brand>", methods=["GET","POST"])
def uploads_brand(brand):
    brand = brand.lower()
    if brand not in BRANDS: abort(404)
    if request.method == "POST":
        f = request.files.get("file")
        if not f or f.filename == "":
            flash("Please choose a file.","warning")
            return redirect(url_for("uploads_brand", brand=brand))
        ext = os.path.splitext(f.filename)[1].lower()
        if ext not in ALLOWED_EXT:
            flash("Only .xlsx, .xls, .csv files are allowed.","danger")
            return redirect(url_for("uploads_brand", brand=brand))
        fn = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}__{secure_filename(f.filename)}"
        f.save(os.path.join(UPLOAD_ROOT, brand, fn))
        _invalidate_brand_caches(brand)                        # <-- clear caches
        flash(f"Uploaded {fn}","success")
        return redirect(url_for("uploads_brand", brand=brand))
    files = list_uploads(brand)
    return render_template("uploads.html", active="uploads", year=datetime.now().year, brand=brand, files=files)

@app.post("/uploads/<brand>/delete/<path:filename>")
def delete_upload(brand, filename):
    brand = brand.lower()
    bdir = os.path.join(UPLOAD_ROOT, brand)
    path = os.path.normpath(os.path.join(bdir, secure_filename(filename)))
    if not path.startswith(bdir): abort(400)
    if os.path.exists(path):
        os.remove(path)
        _invalidate_brand_caches(brand)                        # <-- clear caches
        flash(f"Deleted {filename}","success")
    else:
        flash("File not found.","warning")
    return redirect(url_for("uploads_brand", brand=brand))

# ---------------- Existing BDO performance (district mix) ----------------
@app.get("/api/performance")
def api_performance():
    bdo = (request.args.get("bdo") or "").strip()
    p1  = request.args.get("p1")
    p2  = request.args.get("p2")
    if not (bdo and p1 and p2):
        return jsonify({"error":"missing params"}), 400

    y1, m1 = map(int, p1.split("-"))
    y2, m2 = map(int, p2.split("-"))

    def agg_brand_for(bkey, y, m):
        df = collect_brand_df_for_period(bkey, y, m)
        df = _apply_reassignments(df)
        if df.empty:
            return pd.DataFrame(columns=["District","value"])
        bcol = find_col(df, ["BDO","Name of BDO"])
        dcol = find_col(df, ["State","Province","District","Region"])
        if bcol is None:
            return pd.DataFrame(columns=["District","value"])
        df = df[df[bcol].astype(str).str.strip() == bdo]
        tmp = pd.DataFrame({
            "District": df[dcol] if dcol else "",
            "value": value_series(df)
        })
        return tmp.groupby("District", dropna=False)["value"].sum().reset_index()

    wu1, wu2 = agg_brand_for("wu", y1, m1), agg_brand_for("wu", y2, m2)
    ria1, ria2 = agg_brand_for("ria", y1, m1), agg_brand_for("ria", y2, m2)
    mg1, mg2   = agg_brand_for("mg", y1, m1), agg_brand_for("mg", y2, m2)

    dists = set(wu1["District"])|set(wu2["District"])|set(ria1["District"])|set(ria2["District"])|set(mg1["District"])|set(mg2["District"])
    dists = [d for d in dists if str(d)]
    dists.sort()

    def gv(df, d):
        s = df[df["District"]==d]
        return float(s["value"].iloc[0]) if not s.empty else 0.0

    rows = []
    p1_tot = p2_tot = 0.0
    for d in dists:
        v1 = gv(wu1,d)+gv(ria1,d)+gv(mg1,d)
        v2 = gv(wu2,d)+gv(ria2,d)+gv(mg2,d)
        rows.append({"district": d, "p1_total": int(round(v1)), "p2_total": int(round(v2))})
        p1_tot += v1; p2_tot += v2

    name = BDO_NAME_CANON.get(bdo, bdo)

    return jsonify({
        "bdo": bdo, "name": name,
        "labels": ["Western Union","RIA","MoneyGram","Total"],
        "p1_label": period_label(y1, m1),
        "p2_label": period_label(y2, m2),
        "p1_values": [ int(round(wu1["value"].sum())), int(round(ria1["value"].sum())), int(round(mg1["value"].sum())), int(round(p1_tot)) ],
        "p2_values": [ int(round(wu2["value"].sum())), int(round(ria2["value"].sum())), int(round(mg2["value"].sum())), int(round(p2_tot)) ],
        "rows": rows
    })

# ---------------- Periods & Comparison APIs ----------------
@app.get("/api/periods")
def api_periods():
    periods = available_periods()
    periods = [{"key":"All","year":None,"month":None,"label":"All (lifetime)"}] + periods
    zones = [
        {
            "zone_key": z["zone_key"],
            "title": z["title"],
            "territory": z["territory"],
            "zm": z["zm"],
            "bdos": list(z["bdos"].keys())
        }
        for z in ZONES_DEF
    ]
    bdos = [{"code": code, "name": BDO_NAME_CANON.get(code, "")} for code in BDO_TO_ZONE.keys()]
    return jsonify({"periods": periods, "zones": zones, "bdos": bdos})

@app.get("/api/compare")
def api_compare():
    scope = (request.args.get("scope") or "zm").lower()
    key   = (request.args.get("key") or "").strip()
    brand = (request.args.get("brand") or "all").lower()
    bdo_filter = request.args.get("bdo")

    p1 = request.args.get("p1")
    p2 = request.args.get("p2")
    if not (p1 and p2):
        return jsonify({"error":"missing p1/p2 (YYYY-MM)"}), 400
    y1, m1 = parse_period_key(p1)
    y2, m2 = parse_period_key(p2)
    if not (y1 and m1 and y2 and m2):
        return jsonify({"error":"bad period format"}), 400

    def rows_for(y, m):
        dfs = []
        for brand_key in ("wu","ria","mg"):
            df = _collect_scope_df(brand_key, y, m, scope, key, bdo_filter)
            if not df.empty: dfs.append(df)
        if not dfs:
            return pd.DataFrame(columns=["label","value"])
        df_all = pd.concat(dfs, ignore_index=True)
        grouped, _ = _group_for_scope(df_all, scope)
        return grouped

    g1 = rows_for(y1, m1)
    g2 = rows_for(y2, m2)
    merged = _merge_compare(g1, g2)

    if scope == "agent" and not bdo_filter and key:
        bdo_filter = key
    totals_p1 = _brand_totals_for_scope(y1, m1, scope, key, bdo_filter)
    totals_p2 = _brand_totals_for_scope(y2, m2, scope, key, bdo_filter)
    by_brand = {}
    for code in ("WU","RIA","MG"):
        by_brand[code] = {
            "p1": totals_p1.get(code,0),
            "p2": totals_p2.get(code,0),
            "growth": growth(totals_p1.get(code,0), totals_p2.get(code,0))
        }
    grand = {
        "p1": totals_p1.get("TOTAL",0),
        "p2": totals_p2.get("TOTAL",0),
        "growth": growth(totals_p1.get("TOTAL",0), totals_p2.get("TOTAL",0))
    }

    meta = {"scope": scope}
    if scope == "zm":
        z = _get_zone_by_any(key)
        if z:
            meta.update({"zone_key": z["zone_key"], "zm": z["zm"], "title": z["title"]})
    elif scope in ("bdo","agent"):
        meta.update({"bdo": key or bdo_filter, "bdo_name": BDO_NAME_CANON.get(key or bdo_filter, "")})

    focus = brand if brand in ("wu","ria","mg") else "all"
    if focus in ("wu","ria","mg"):
        code = {"wu":"WU","ria":"RIA","mg":"MG"}[focus]
        meta["focus_brand"] = code
        meta["focus_totals"] = by_brand.get(code, {})

    return jsonify({
        "p1_key": p1, "p2_key": p2,
        "p1_label": period_label(y1, m1),
        "p2_label": period_label(y2, m2),
        "meta": meta,
        "rows": merged.to_dict(orient="records"),
        "totals": grand,
        "by_brand": by_brand
    })

# ---------------- SUMMARY & AGENT/LEADER APIs (period-aware, support p=All) ----------------
@app.get("/api/summary")
def api_summary():
    p = request.args.get("p")
    if not p:
        return jsonify({"error":"missing p"}), 400

    brand_vals = []
    for key in ("wu","ria","mg"):
        df = collect_df_for_period_key(key, p)
        df = _apply_reassignments(df)
        v = float(value_series(df).sum()) if not df.empty else 0.0
        brand_vals.append(v)

    frames = []
    for key in ("wu","ria","mg"):
        df = collect_df_for_period_key(key, p)
        df = _apply_reassignments(df)
        if df.empty:
            continue
        names = _name_series(df)
        frames.append(pd.DataFrame({"NAME": names, "VAL": value_series(df)}))
    if frames:
        allf = pd.concat(frames, ignore_index=True)
        allf["KEY"] = allf["NAME"].fillna("").astype(str).str.strip().str.upper()
        agg = (allf.groupby("KEY")["VAL"].sum()
                 .sort_values(ascending=False).head(10).reset_index())
        disp = (allf.groupby("KEY")["NAME"]
                   .agg(lambda s: s[s.astype(bool)].mode().iloc[0] if (s.astype(bool)).any() else ""))
        labels = [disp.get(k, k.title()) for k in agg["KEY"]]
        values = agg["VAL"].astype(float).round(0).tolist()
    else:
        labels, values = [], []

    return jsonify({
        "brandBar": {"labels":["Western Union","RIA","MoneyGram"], "values": brand_vals},
        "agentsPie": {"labels": labels, "values": values}
    })

@app.get("/api/zone-leaders")
def api_zone_leaders():
    p = request.args.get("p")
    zone_key = (request.args.get("zone") or "All").upper()
    if not p:
        return jsonify({"error":"missing p"}), 400

    zone = next((z for z in ZONES_DEF if z["zone_key"].upper() == zone_key), None)
    allowed_bdos = set(BDO_TO_ZONE.keys()) if not zone else set(zone["bdos"].keys())

    def collect_brand(brand_key):
        df = collect_df_for_period_key(brand_key, p)
        df = _apply_reassignments(df)
        if df.empty: 
            return pd.DataFrame(columns=["LEADER","VALUE"])
        bcol = find_col(df, ["BDO","Name of BDO"])
        if bcol:
            df = df[df[bcol].astype(str).isin(allowed_bdos)]
            df = _apply_zone_constraints(df, zone["zone_key"] if zone else None)
        leader = df[bcol].astype(str).map(lambda x: BDO_NAME_CANON.get(str(x), ""))
        return pd.DataFrame({"LEADER": leader, "VALUE": value_series(df)})

    parts = {}
    frames = []
    for code, key in (("WU","wu"),("RIA","ria"),("MG","mg")):
        d = collect_brand(key)
        parts[code] = d
        frames.append(d)

    all_leaders = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["LEADER","VALUE"])
    all_leaders = (all_leaders.groupby("LEADER")["VALUE"].sum()
                     .sort_values(ascending=False).reset_index())
    labels = all_leaders["LEADER"].fillna("").astype(str).tolist()

    def series_for(code):
        d = parts[code]
        if d.empty:
            return [0]*len(labels)
        g = d.groupby("LEADER")["VALUE"].sum()
        return [int(round(float(g.get(n, 0)))) for n in labels]

    wu = series_for("WU"); ria = series_for("RIA"); mg = series_for("MG")
    avatars = [_avatar_url_for(n) for n in labels]

    zm_name = zone["zm"] if zone else "All Zones"
    zm_avatar = _avatar_url_for(zm_name)

    return jsonify({
        "labels": labels, "wu": wu, "ria": ria, "mg": mg,
        "avatars": avatars,
        "meta": {"zone": zone_key if zone else "All", "zm": zm_name, "zm_avatar": zm_avatar}
    })

@app.get("/api/zone-leader-detail")
def api_zone_leader_detail():
    p = request.args.get("p")
    zone_key = (request.args.get("zone") or "All").upper()
    leader   = (request.args.get("leader") or "").strip()
    if not p or not leader:
        return jsonify({"rows": []})

    zone = next((z for z in ZONES_DEF if z["zone_key"].upper() == zone_key), None)
    allowed_bdos = set(BDO_TO_ZONE.keys()) if not zone else set(zone["bdos"].keys())
    codes = [c for c in allowed_bdos if BDO_NAME_CANON.get(c, "").strip().upper() == leader.strip().upper()]
    if not codes:
        codes = list(allowed_bdos)

    def totals_for(brand_key):
        df = collect_df_for_period_key(brand_key, p)
        df = _apply_reassignments(df)
        if df.empty: 
            return {}
        bcol = find_col(df, ["BDO","Name of BDO"])
        if not bcol:
            return {}
        df = df[df[bcol].astype(str).isin(codes)]
        df = _apply_zone_constraints(df, zone_key if zone else None)
        g = pd.DataFrame({"BDO": df[bcol].astype(str), "V": value_series(df)})
        return g.groupby("BDO")["V"].sum().to_dict()

    wu = totals_for("wu"); ria = totals_for("ria"); mg = totals_for("mg")
    all_codes = sorted(set(wu.keys())|set(ria.keys())|set(mg.keys()))
    rows = []
    for code in all_codes:
        vw = float(wu.get(code,0)); vr = float(ria.get(code,0)); vm = float(mg.get(code,0))
        rows.append({"bdo": code,
                     "wu": int(round(vw)),
                     "ria": int(round(vr)),
                     "mg": int(round(vm)),
                     "total": int(round(vw+vr+vm))})
    rows.sort(key=lambda r: r["total"], reverse=True)
    return jsonify({"rows": rows})

@app.get("/api/agents-overview")
def api_agents_overview():
    p = request.args.get("p")
    zone_key = (request.args.get("zone") or "All").upper()
    if not p: 
        return jsonify({"error":"missing p"}), 400

    zone = next((z for z in ZONES_DEF if z["zone_key"].upper()==zone_key), None)
    allowed_bdos = set(BDO_TO_ZONE.keys()) if not zone else set(zone["bdos"].keys())

    def brand_part(brand_key):
        df = collect_df_for_period_key(brand_key, p)
        df = _apply_reassignments(df)
        if df.empty:
            return pd.DataFrame(columns=["NAME","VAL"])
        bcol = find_col(df, ["BDO","Name of BDO"])
        if bcol:
            df = df[df[bcol].astype(str).isin(allowed_bdos)]
            df = _apply_zone_constraints(df, zone_key if zone else None)
        names = _name_series(df)
        return pd.DataFrame({"NAME": names, "VAL": value_series(df)})

    parts = {}
    frames = []
    for code, key in (("WU","wu"),("RIA","ria"),("MG","mg")):
        d = brand_part(key)
        parts[code] = d
        frames.append(d)

    allf = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["NAME","VAL"])
    allf["NAME"] = allf["NAME"].fillna("").astype(str)
    totals = (allf.groupby("NAME")["VAL"].sum()
                .sort_values(ascending=False).reset_index())
    labels = totals["NAME"].tolist()

    def series_for(code):
        d = parts[code]
        if d.empty: return [0]*len(labels)
        g = d.groupby("NAME")["VAL"].sum()
        return [int(round(float(g.get(n, 0)))) for n in labels]

    wu = series_for("WU"); ria = series_for("RIA"); mg = series_for("MG")
    avatars = [_avatar_url_for(n) for n in labels]

    return jsonify({"labels": labels, "wu": wu, "ria": ria, "mg": mg, "avatars": avatars})

@app.get("/api/agent-detail")
def api_agent_detail():
    p = request.args.get("p")
    name = (request.args.get("name") or "").strip()
    if not p or not name:
        return jsonify({"error":"missing params"}), 400

    out = {"name": name, "brands": {}}
    total = 0
    for code, key in (("WU","wu"),("RIA","ria"),("MG","mg")):
        df = collect_df_for_period_key(key, p)
        df = _apply_reassignments(df)
        if df.empty:
            out["brands"][code] = 0
            continue
        names = _name_series(df)
        v = value_series(df)[names.fillna("").astype(str)==name].sum()
        iv = int(round(float(v)))
        out["brands"][code] = iv
        total += iv
    out["total"] = total
    return jsonify(out)

# ---------------- Legacy all-time ----------------
@app.get("/api/zone-leaders-legacy")
def api_zone_leaders_legacy():
    zone_key = (request.args.get("zone") or "All").upper()
    zone = next((z for z in ZONES_DEF if z["zone_key"].upper() == zone_key), None)

    codes_scope = list(BDO_TO_ZONE.keys()) if not zone else list(zone["bdos"].keys())

    def _brand_sum_by_bdo(df: pd.DataFrame):
        if df.empty:
            return pd.Series(dtype=float)
        df = _apply_reassignments(df)
        bcol = find_col(df, ["BDO","Name of BDO"])
        if not bcol:
            return pd.Series(dtype=float)
        tmp = pd.DataFrame({"BDO": df[bcol].astype(str), "value": value_series(df)})
        return tmp.groupby("BDO")["value"].sum()

    sums = {}
    for label, b in (("WU","wu"),("RIA","ria"),("MG","mg")):
        sums[label] = _brand_sum_by_bdo(collect_all_brand_df(b))

    leaders = {}
    for code in codes_scope:
        leader = BDO_NAME_CANON.get(code, code)
        if leader not in leaders:
            leaders[leader] = {"WU":0.0,"RIA":0.0,"MG":0.0}
        for lab in ("WU","RIA","MG"):
            leaders[leader][lab] += float(sums.get(lab, pd.Series()).get(code, 0.0))

    order = sorted(leaders.keys(),
                   key=lambda k: leaders[k]["WU"]+leaders[k]["RIA"]+leaders[k]["MG"],
                   reverse=True)

    labels  = order
    wu_vals = [int(round(leaders[k]["WU"]))  for k in order]
    ria_vals= [int(round(leaders[k]["RIA"])) for k in order]
    mg_vals = [int(round(leaders[k]["MG"]))  for k in order]
    avatars = [_avatar_url_for(k) for k in order]

    meta = {
        "zone": zone_key if zone else "All",
        "zm": zone["zm"] if zone else "All Zones",
        "zm_avatar": _avatar_url_for(zone["zm"] if zone else "default")
    }
    return jsonify({
        "labels": labels,
        "wu": wu_vals, "ria": ria_vals, "mg": mg_vals,
        "avatars": avatars,
        "meta": meta
    })

if __name__ == "__main__":
    # use $PORT if the platform sets it (Render, HF Spaces, Railway, etc.)
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)

