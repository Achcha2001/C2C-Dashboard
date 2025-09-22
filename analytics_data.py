# Builds charts_json + (optional) HTML tables for the Analytics page.

import os, re
from io import BytesIO
import pandas as pd
import numpy as np

BRANDS = ("wu", "ria", "mg")
HEADER_HINTS = [
    "MTCN","Name of BDO","BDO","Name of Sales Rep","Sales Rep",
    "City","State","Province","District","Amount","Transaction Amount","Total"
]

def _read_any_table_bytes(raw: bytes, filename: str) -> pd.DataFrame:
    if not raw:
        return pd.DataFrame()
    fn = (filename or "").lower()
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
            header_row = i; break
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

def _read_any_table_path(path: str) -> pd.DataFrame:
    with open(path, "rb") as f:
        raw = f.read()
    return _read_any_table_bytes(raw, path)

def _list_uploads(upload_root: str, brand: str):
    bdir = os.path.join(upload_root, brand)
    if not os.path.isdir(bdir):
        return []
    out = []
    for name in os.listdir(bdir):
        path = os.path.join(bdir, name)
        if os.path.isfile(path):
            out.append({"name": name, "path": path})
    return out

def _concat_brand_df(upload_root: str, brand: str) -> pd.DataFrame:
    frames = []
    for f in _list_uploads(upload_root, brand):
        df = _read_any_table_path(f["path"])
        if not df.empty:
            frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def _find_col(df, candidates):
    cols = {c.lower().replace(" ", ""): c for c in df.columns}
    for cand in candidates:
        key = cand.lower().replace(" ", "")
        if key in cols:
            return cols[key]
    for c in df.columns:
        if any(x.lower().replace(" ", "") in c.lower().replace(" ", "") for x in candidates):
            return c
    return None

def _value_series(df: pd.DataFrame) -> pd.Series:
    amt  = _find_col(df, ["Amount","Transaction Amount","Total Amount","Value","Total"])
    if amt:
        return pd.to_numeric(df[amt].astype(str).str.replace(",", ""), errors="coerce").fillna(0.0)
    return pd.Series(1.0, index=df.index)

def _fmt_int(n):
    try:
        return f"{int(round(float(n))):,}"
    except Exception:
        return "0"

def _build_city_table_html(df: pd.DataFrame):
    if df.empty:
        return None
    state = _find_col(df, ["State","Province","Region"])
    prov  = _find_col(df, ["Province"]) if state and "state" not in state.lower() else None
    city  = _find_col(df, ["City","Town"])

    group_cols = []
    if state: group_cols.append(state)
    if prov and prov not in group_cols: group_cols.append(prov)
    if city:  group_cols.append(city)
    if not group_cols:
        return None

    tmp = pd.DataFrame({"value": _value_series(df)})
    for c in group_cols:
        tmp[c] = df[c].fillna("").astype(str)

    agg = (tmp.groupby(group_cols, dropna=False)["value"]
              .sum().reset_index().sort_values("value", ascending=False))

    rename_map = {c: c.upper() for c in group_cols}
    agg = agg.rename(columns=rename_map | {"value": "TOTAL"})
    agg["TOTAL"] = agg["TOTAL"].map(_fmt_int)
    return agg.to_html(index=False, classes="table table-hover table-sm mb-0")

def build_analytics(upload_root: str):
    wu  = _concat_brand_df(upload_root, "wu")
    ria = _concat_brand_df(upload_root, "ria")
    mg  = _concat_brand_df(upload_root, "mg")

    brand_totals = {
        "WU": float(_value_series(wu).sum()) if not wu.empty else 0.0,
        "RIA": float(_value_series(ria).sum()) if not ria.empty else 0.0,
        "MG": float(_value_series(mg).sum()) if not mg.empty else 0.0,
    }

    charts_json = {
        "brandBar": {
            "labels": ["Western Union", "RIA", "MoneyGram"],
            "values": [brand_totals["WU"], brand_totals["RIA"], brand_totals["MG"]],
        },
        "agentsPie": {"labels": [], "values": []},  # populated via live API by period
    }

    tables = {
        "wu_city":  _build_city_table_html(wu),
        "ria_city": _build_city_table_html(ria),
        "mg_city":  _build_city_table_html(mg),
        "bdo_sales": "<div class='text-muted'>BDO/Sales Rep table available in live views.</div>"
    }
    return {"tables": tables, "charts_json": charts_json}
