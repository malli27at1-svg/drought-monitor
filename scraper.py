"""
USDM Full Drought Scraper
Fetches all area types from the USDM REST API and saves CSVs + manifest.json
Designed to run weekly via GitHub Actions.
"""

import requests, pandas as pd, time, json, os, io
from datetime import datetime
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
BASE       = "https://usdmdataservices.unl.edu/api"
HEADERS    = {"Accept": "application/json"}
START_DATE = "1/1/2000"
END_DATE   = datetime.today().strftime("%m/%d/%Y")
DATA_DIR   = Path("data")
SLEEP      = 0.3   # seconds between requests

# ── AOI reference tables ──────────────────────────────────────────────────────
STATE_FIPS = {
    "AL":"01","AK":"02","AZ":"04","AR":"05","CA":"06","CO":"08","CT":"09",
    "DE":"10","FL":"12","GA":"13","HI":"15","ID":"16","IL":"17","IN":"18",
    "IA":"19","KS":"20","KY":"21","LA":"22","ME":"23","MD":"24","MA":"25",
    "MI":"26","MN":"27","MS":"28","MO":"29","MT":"30","NE":"31","NV":"32",
    "NH":"33","NJ":"34","NM":"35","NY":"36","NC":"37","ND":"38","OH":"39",
    "OK":"40","OR":"41","PA":"42","RI":"44","SC":"45","SD":"46","TN":"47",
    "TX":"48","UT":"49","VT":"50","VA":"51","WA":"53","WV":"54","WI":"55","WY":"56",
}
FIPS_TO_ABBR = {v: k for k, v in STATE_FIPS.items()}

NWS_REGIONS = {
    "CR":"Central Region","ER":"Eastern Region","PR":"Pacific Region",
    "SR":"Southern Region","WR":"Western Region","AR":"Alaska Region","BR":"Boulder Region",
}
RFC_CODES = {
    "ABRFC":"Arkansas-Red Basin","CBRFC":"Colorado Basin","CNRFC":"California-Nevada",
    "LMRFC":"Lower Mississippi","MBRFC":"Missouri Basin","MARFC":"Middle Atlantic",
    "NCRFC":"North Central","NERFC":"Northeast","NWRFC":"Northwest",
    "OHRFC":"Ohio","SERFC":"Southeast","WGRFC":"West Gulf",
}
HUC2_IDS = {
    "01":"New England","02":"Mid Atlantic","03":"South Atlantic-Gulf","04":"Great Lakes",
    "05":"Ohio","06":"Tennessee","07":"Upper Mississippi","08":"Lower Mississippi",
    "09":"Souris-Red-Rainy","10":"Missouri","11":"Arkansas-White-Red",
    "12":"Texas-Gulf","13":"Rio Grande","14":"Upper Colorado","15":"Lower Colorado",
    "16":"Great Basin","17":"Pacific Northwest","18":"California",
}
USACE_DISTRICTS = {
    "SWF":"Fort Worth","SWT":"Tulsa","SPK":"Sacramento","SPA":"Albuquerque",
    "SPL":"Los Angeles","SPN":"San Francisco","NAB":"Baltimore","NAE":"New England",
    "NAN":"New York","NAO":"Norfolk","NAP":"Philadelphia","LRB":"Buffalo",
    "LRC":"Chicago","LRD":"Detroit","LRE":"Detroit","LRH":"Huntington",
    "LRL":"Louisville","LRN":"Nashville","LRP":"Pittsburgh","MVK":"Vicksburg",
    "MVM":"Memphis","MVN":"New Orleans","MVR":"Rock Island","MVS":"St. Louis",
    "NWD":"Portland","NWK":"Kansas City","NWO":"Omaha","NWP":"Portland OR",
    "NWS":"Seattle","NWW":"Walla Walla","POA":"Alaska","SAD":"Savannah",
    "SAJ":"Jacksonville","SAM":"Mobile","SAW":"Wilmington","SWG":"Galveston",
    "SWL":"Little Rock",
}
USACE_DIVISIONS = {
    "LRD":"Great Lakes & Ohio","MVD":"Mississippi Valley","NAD":"North Atlantic",
    "NWD":"Northwestern","POD":"Pacific Ocean","SAD":"South Atlantic","SWD":"Southwestern",
}
CLIMATE_HUBS = {
    "1":"Northeast","2":"Southeast","3":"Midwest",
    "4":"Southern Plains & Gulf Coast","5":"Northern Plains","6":"Southwest","7":"Northwest",
}
RDEWS = {
    "1":"Missouri Basin","2":"Upper Colorado River Basin","3":"California-Nevada",
    "4":"North Central","5":"Southern Plains","6":"Southeast",
    "7":"Intermountain West","8":"Peninsula Florida","9":"Northeast","10":"Pacific Islands",
}
RCC_IDS = {
    "1":"High Plains","2":"Midwest","3":"Northeast",
    "4":"Southern","5":"Southeast","6":"Western",
}
FEMA_REGIONS = {str(i): f"Region {i}" for i in range(1, 11)}

# ── Area type definitions ────────────────────────────────────────────────────
# Each entry: (api_endpoint, huc_level_or_None, {aoi: label}, is_multi_area_response)
AREA_TYPES = {
    "national": {
        "api": "USStatistics", "huc": None,
        "jobs": {"us": "United States", "conus": "CONUS"},
        "multi": False,
    },
    "state": {
        "api": "StateStatistics", "huc": None,
        "jobs": STATE_FIPS,   # {abbr: fips} — we use fips as aoi, abbr as label
        "multi": False,
    },
    "county": {
        "api": "CountyStatistics", "huc": None,
        "jobs": {abbr: abbr for abbr in STATE_FIPS},  # aoi = state abbrev, returns all counties
        "multi": True,   # response has multiple rows per date (one per county)
    },
    "climate_div": {
        "api": "ClimateDivisionStatistics", "huc": None,
        "jobs": {"us": "All Climate Divisions"},
        "multi": True,
    },
    "fema": {
        "api": "FEMARegionStatistics", "huc": None,
        "jobs": FEMA_REGIONS,
        "multi": False,
    },
    "nws_region": {
        "api": "NWSRegionStatistics", "huc": None,
        "jobs": NWS_REGIONS,
        "multi": False,
    },
    "rfc": {
        "api": "RiverForecastCenterStatistics", "huc": None,
        "jobs": RFC_CODES,
        "multi": False,
    },
    "huc2": {
        "api": "HUCStatistics", "huc": 2,
        "jobs": HUC2_IDS,
        "multi": False,
    },
    "usace_district": {
        "api": "USACEDistrictStatistics", "huc": None,
        "jobs": USACE_DISTRICTS,
        "multi": False,
    },
    "usace_division": {
        "api": "USACEDivisionStatistics", "huc": None,
        "jobs": USACE_DIVISIONS,
        "multi": False,
    },
    "climate_hub": {
        "api": "ClimateHubStatistics", "huc": None,
        "jobs": CLIMATE_HUBS,
        "multi": False,
    },
    "rdews": {
        "api": "RegionalDroughtEarlyWarningSystemStatistics", "huc": None,
        "jobs": RDEWS,
        "multi": False,
    },
    "rcc": {
        "api": "RegionalClimateCenterStatistics", "huc": None,
        "jobs": RCC_IDS,
        "multi": False,
    },
    "tribal": {
        "api": "TribalStatistics", "huc": None,
        "jobs": {"us": "All Tribal Areas"},
        "multi": True,
    },
    "urban": {
        "api": "UrbanAreaStatistics", "huc": None,
        "jobs": {"us": "All Urban Areas"},
        "multi": True,
    },
}

# ── Fetch helpers ─────────────────────────────────────────────────────────────
def make_url(api_code, aoi, huc=None):
    url = (f"{BASE}/{api_code}/GetDroughtSeverityStatisticsByAreaPercent"
           f"?aoi={aoi}&startdate={START_DATE}&enddate={END_DATE}&statisticsType=1")
    if huc:
        url += f"&hucLevel={huc}"
    return url

def fetch(url):
    r = requests.get(url, headers=HEADERS, timeout=45)
    r.raise_for_status()
    ct = r.headers.get("content-type", "")
    if "json" in ct:
        return r.json()
    return pd.read_csv(io.StringIO(r.text)).to_dict("records")

def parse_single(raw):
    """Parse a single-area time series into a clean DataFrame."""
    if not raw:
        return pd.DataFrame()
    df = pd.DataFrame(raw)
    date_col = next((c for c in df.columns if c.lower() in ("mapdate","week","date")), None)
    if not date_col:
        return pd.DataFrame()
    df["_d"] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=["_d"]).sort_values("_d").reset_index(drop=True)

    lc = {c.lower(): c for c in df.columns}
    def col(n):
        c = lc.get(n)
        return pd.to_numeric(df[c], errors="coerce").round(2) if c else None

    none = col("none")
    d0, d1, d2, d3, d4 = col("d0"), col("d1"), col("d2"), col("d3"), col("d4")
    if d0 is None:
        return pd.DataFrame()

    out = pd.DataFrame({"Week": df["_d"].dt.strftime("%Y-%m-%d")})
    if none is not None: out["None"]  = none.values
    # statisticsType=1: d0=D0-D4, d1=D1-D4 (cumulative already)
    out["D0-D4"] = d0.values
    if d1 is not None: out["D1-D4"] = d1.values
    if d2 is not None: out["D2-D4"] = d2.values
    if d3 is not None: out["D3-D4"] = d3.values
    if d4 is not None: out["D4"]    = d4.values
    return out

def parse_multi(raw):
    """Parse a multi-area response (e.g. all counties in a state)."""
    if not raw:
        return pd.DataFrame()
    df = pd.DataFrame(raw)
    date_col = next((c for c in df.columns if c.lower() in ("mapdate","week","date")), None)
    if not date_col:
        return pd.DataFrame()
    df["Week"] = pd.to_datetime(df[date_col], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df.dropna(subset=["Week"]).sort_values(["Week"]).reset_index(drop=True)

    # Identify the area name column
    lc = {c.lower(): c for c in df.columns}
    area_col = (lc.get("name") or lc.get("areaofinterest") or
                lc.get("county") or lc.get("stateabbreviation"))

    out = pd.DataFrame({"Week": df["Week"]})
    if area_col:
        out.insert(1, "Area", df[area_col].values)

    def col(n):
        c = lc.get(n)
        return pd.to_numeric(df[c], errors="coerce").round(2) if c else None

    none = col("none")
    d0, d1, d2, d3, d4 = col("d0"), col("d1"), col("d2"), col("d3"), col("d4")
    if d0 is None:
        return pd.DataFrame()

    if none is not None: out["None"]  = none.values
    out["D0-D4"] = d0.values
    if d1 is not None: out["D1-D4"] = d1.values
    if d2 is not None: out["D2-D4"] = d2.values
    if d3 is not None: out["D3-D4"] = d3.values
    if d4 is not None: out["D4"]    = d4.values
    return out

# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    DATA_DIR.mkdir(exist_ok=True)
    manifest = {}   # area_type → {aoi_key: {label, file, multi}}
    total_ok = 0
    total_err = 0

    print(f"\n{'─'*60}")
    print(f"USDM FULL SCRAPER  —  {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"Date range: {START_DATE} → {END_DATE}")
    print(f"{'─'*60}\n")

    for area_type, cfg in AREA_TYPES.items():
        api_code = cfg["api"]
        huc      = cfg["huc"]
        is_multi = cfg["multi"]
        jobs     = cfg["jobs"]

        # For state, jobs = {abbr: fips} — aoi is fips, label is abbr
        # For others, jobs = {aoi_key: label}
        subdir = DATA_DIR / area_type
        subdir.mkdir(exist_ok=True)
        manifest[area_type] = {}

        print(f"[{area_type.upper()}]  ({len(jobs)} request{'s' if len(jobs)>1 else ''})")

        for key, val in jobs.items():
            # state: key=abbr, val=fips  →  aoi=fips, label=abbr, filename=abbr
            if area_type == "state":
                aoi, label, filename = val, key, key
            else:
                aoi, label, filename = key, val, key

            url = make_url(api_code, aoi, huc)
            safe_name = str(filename).replace("/", "-").replace(" ", "_")
            csv_path  = subdir / f"{safe_name}.csv"

            try:
                raw = fetch(url)
                df  = parse_multi(raw) if is_multi else parse_single(raw)

                if df.empty:
                    print(f"  ⚠  {label}: empty / parse failed")
                    total_err += 1
                    continue

                # For updates: merge with existing CSV (append new weeks only)
                if csv_path.exists():
                    existing = pd.read_csv(csv_path, dtype=str)
                    if "Area" in df.columns and "Area" in existing.columns:
                        combined = pd.concat([existing, df.astype(str)]).drop_duplicates(
                            subset=["Week","Area"], keep="last")
                    elif "Week" in existing.columns:
                        combined = pd.concat([existing, df.astype(str)]).drop_duplicates(
                            subset=["Week"], keep="last")
                    else:
                        combined = df
                    combined = combined.sort_values("Week").reset_index(drop=True)
                    combined.to_csv(csv_path, index=False)
                else:
                    df.to_csv(csv_path, index=False)

                n_rows = len(df)
                print(f"  ✅  {label}: {n_rows} rows → {csv_path}")

                manifest[area_type][str(filename)] = {
                    "label":  str(label),
                    "file":   f"{area_type}/{safe_name}.csv",
                    "multi":  is_multi,
                    "rows":   n_rows,
                }
                total_ok += 1

            except Exception as e:
                print(f"  ❌  {label}: {e}")
                total_err += 1

            time.sleep(SLEEP)

        print()

    # Write manifest
    manifest_data = {
        "generated": datetime.utcnow().isoformat(),
        "start_date": START_DATE,
        "end_date": END_DATE,
        "areas": manifest,
    }
    with open(DATA_DIR / "manifest.json", "w") as f:
        json.dump(manifest_data, f, indent=2)
    print(f"Manifest → {DATA_DIR}/manifest.json")
    print(f"\n{'─'*60}")
    print(f"COMPLETE  —  {total_ok} succeeded, {total_err} failed")
    print(f"{'─'*60}\n")

if __name__ == "__main__":
    main()
