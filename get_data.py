from __future__ import annotations
import time, sys
from io import BytesIO
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict

import pandas as pd
import requests
from requests.exceptions import HTTPError

BASE = "https://api.openf1.org/v1"
SESSION_SCOPE = "RACE_SPRINT"   # "RACE" | "RACE_SPRINT" | "ALL"
INCLUDE_MEETINGS = True
DOWNLOAD_LAPS = False
TIMEOUT = 60
RETRIES = 4
BACKOFF = 2.0
OUT_DIR = Path("data/openf1_full")
OVERWRITE = True

def mkdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def fetch_csv(endpoint: str, **params) -> pd.DataFrame:
    url = f"{BASE}/{endpoint}"
    params = {**params, "csv": "true"}
    err: Optional[Exception] = None
    for a in range(RETRIES):
        try:
            r = requests.get(url, params=params, timeout=TIMEOUT)
            if r.status_code in (204, 400):
                return pd.DataFrame()
            r.raise_for_status()
            if not r.content or r.content.strip() == b"":
                return pd.DataFrame()
            return pd.read_csv(BytesIO(r.content))
        except HTTPError as e:
            if getattr(e.response, "status_code", None) in (400, 404):
                return pd.DataFrame()
            err = e
        except Exception as e:
            err = e
        time.sleep(BACKOFF ** a)
    raise RuntimeError(f"Request failed: {endpoint} {params} -> {err}")

def available_years(start_from: int = 2018, end_to: Optional[int] = None) -> list[int]:
    end_to = end_to or datetime.utcnow().year
    yrs = []
    for y in range(start_from, end_to + 1):
        try:
            df = fetch_csv("sessions", year=y)
            if not df.empty:
                yrs.append(y)
        except Exception:
            pass
    return yrs

def filter_sessions(df: pd.DataFrame, scope: str) -> pd.DataFrame:
    if df.empty or scope == "ALL":
        return df
    cols = {c.lower(): c for c in df.columns}
    sn = df[cols["session_name"]].astype(str).str.lower() if "session_name" in cols else None
    st = df[cols["session_type"]].astype(str).str.upper() if "session_type" in cols else None
    m = pd.Series(False, index=df.index)
    if scope in ("RACE", "RACE_SPRINT"):
        if sn is not None: m |= sn.eq("race")
        if st is not None: m |= st.eq("R")
    if scope == "RACE_SPRINT":
        if sn is not None: m |= sn.str.contains("sprint", na=False)
        if st is not None: m |= st.isin({"S","SPRINT"})
    return df.loc[m] if m.any() else df

class CSVAgg:
    def __init__(self, path: Path):
        self.path = path
        if path.exists() and OVERWRITE:
            path.unlink()
        mkdir(path.parent)
    def append(self, df: pd.DataFrame) -> None:
        if df is None or df.empty: return
        self.path.write_text("") if not self.path.exists() else None
        df.to_csv(self.path, index=False, mode="a", header=not self._has_header())
    def _has_header(self) -> bool:
        try:
            with self.path.open("rb") as f:
                return bool(f.read(1))
        except FileNotFoundError:
            return False

def main() -> None:
    mkdir(OUT_DIR)
    years = available_years(2018, datetime.utcnow().year)
    if not years:
        print("No available years detected.", file=sys.stderr); sys.exit(1)
    print(f"Detected years: {years}")

    aggs: Dict[str, Optional[CSVAgg]] = {
        "sessions_all":  CSVAgg(OUT_DIR / "sessions_all.csv"),
        "meetings_all":  CSVAgg(OUT_DIR / "meetings_all.csv") if INCLUDE_MEETINGS else None,
        "stints":        CSVAgg(OUT_DIR / "stints_all.csv"),
        "pit":           CSVAgg(OUT_DIR / "pit_all.csv"),
        "weather":       CSVAgg(OUT_DIR / "weather_all.csv"),
        "starting_grid": CSVAgg(OUT_DIR / "starting_grid_all.csv"),
        "session_result":CSVAgg(OUT_DIR / "session_result_all.csv"),
        "race_control":  CSVAgg(OUT_DIR / "race_control_all.csv"),
        "laps":          CSVAgg(OUT_DIR / "laps_all.csv") if DOWNLOAD_LAPS else None,
    }

    total_sessions = total_targets = 0

    for y in years:
        try:
            sessions = fetch_csv("sessions", year=y)
        except Exception as e:
            print(f"[{y}] sessions error: {e}", file=sys.stderr); continue
        aggs["sessions_all"].append(sessions)
        if INCLUDE_MEETINGS and aggs["meetings_all"] is not None:
            try:
                m = fetch_csv("meetings", year=y)
                aggs["meetings_all"].append(m)
            except Exception:
                pass
        total_sessions += len(sessions)
        tgt = filter_sessions(sessions, SESSION_SCOPE)
        total_targets += len(tgt)
        cols = {c.lower(): c for c in tgt.columns}
        sk = cols.get("session_key","session_key")
        print(f"[{y}] sessions={len(sessions)} targets={len(tgt)}")
        for _, r in tgt.iterrows():
            try:
                key = int(r[sk])
            except Exception:
                continue
            for ep, name in [("stints","stints"),("pit","pit"),("weather","weather"),
                             ("starting_grid","starting_grid"),("session_result","session_result"),
                             ("race_control","race_control")]:
                try:
                    df = fetch_csv(ep, session_key=key)
                    aggs[name].append(df)
                except Exception as e:
                    print(f"  - [{y} SK={key}] {ep} warn: {e}", file=sys.stderr)
            if DOWNLOAD_LAPS and aggs["laps"] is not None:
                try:
                    df = fetch_csv("laps", session_key=key)
                    aggs["laps"].append(df)
                except Exception as e:
                    print(f"  - [{y} SK={key}] laps warn: {e}", file=sys.stderr)

    print(f"Done. Years={years} total_sessions={total_sessions} targets_processed={total_targets}")
    print(f"Output: {OUT_DIR.resolve()}")
    files = ["sessions_all.csv","meetings_all.csv" if INCLUDE_MEETINGS else None,
             "stints_all.csv","pit_all.csv","weather_all.csv",
             "starting_grid_all.csv","session_result_all.csv",
             "race_control_all.csv","laps_all.csv" if DOWNLOAD_LAPS else None]
    for f in filter(None, files):
        print(f" - {f}")

if __name__ == "__main__":
    main()
