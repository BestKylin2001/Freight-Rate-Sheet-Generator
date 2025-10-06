
"""
Streamlit App — Stable, Cloud‑Ready Rewrite
------------------------------------------
Key improvements:
- No hardcoded credentials. Uses Streamlit Secrets or ENV for GCP.
- Defensive imports for local utilities (bigquery_utils_v2 OR bigquery_utils).
- Clear separation of config, data, and UI.
- Caching, error boundaries, and structured feedback.
- Works as a web app (Streamlit Cloud / Cloud Run) — no .exe packaging needed.

Setup
=====
1) requirements.txt (minimum):
   streamlit
   pandas
   google-cloud-bigquery
   google-auth

2) Secrets (preferred)
   In .streamlit/secrets.toml (Streamlit Cloud) or environment variables:
   [gcp]
   project_id = "YOUR_PROJECT_ID"
   # Put the *full JSON* as one line string for service_account_json, or store the key file in Secret Manager.
   service_account_json = "{" ... }"

   OR set env vars:
   GCP_PROJECT_ID=...
   GCP_SERVICE_ACCOUNT_JSON='{"...": "..."}'

3) Run locally:
   streamlit run streamlit_app_rewrite.py

Author: rewritten by ChatGPT (stability-focused)
"""

import os
import json
from functools import lru_cache
from typing import Optional, Dict

import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# ---------------------------
# Config & Credentials
# ---------------------------

def _get_secret(key: str, default: Optional[str] = None) -> Optional[str]:
    # Prefer Streamlit secrets, fallback to ENV
    try:
        return st.secrets.get(key, default)  # type: ignore[attr-defined]
    except Exception:
        return os.environ.get(key.upper(), default)

@lru_cache(maxsize=1)
def get_gcp_config() -> Dict[str, str]:
    # Try secrets first
    project_id = None
    service_account_json = None

    # Nested "gcp" dict in st.secrets is common
    try:
        gcp_secrets = st.secrets.get("gcp", None)  # type: ignore[attr-defined]
        if gcp_secrets:
            project_id = gcp_secrets.get("project_id")
            service_account_json = gcp_secrets.get("service_account_json")
    except Exception:
        pass

    # Fallback flat keys or ENV
    project_id = project_id or _get_secret("gcp_project_id") or _get_secret("project_id")
    service_account_json = service_account_json or _get_secret("gcp_service_account_json")

    if not project_id:
        raise RuntimeError("Missing GCP project_id. Set in st.secrets['gcp']['project_id'] or env PROJECT_ID.")
    if not service_account_json:
        raise RuntimeError("Missing service account JSON. Set in st.secrets['gcp']['service_account_json'] or env GCP_SERVICE_ACCOUNT_JSON.")

    return {"project_id": project_id, "service_account_json": service_account_json}

@lru_cache(maxsize=1)
def get_bq_client() -> bigquery.Client:
    cfg = get_gcp_config()
    try:
        info = json.loads(cfg["service_account_json"])
    except json.JSONDecodeError:
        # If provided as a python-ish dict string, attempt eval as last resort
        info = eval(cfg["service_account_json"])  # noqa: S307 - trusted runtime environment

    creds = service_account.Credentials.from_service_account_info(info)
    return bigquery.Client(project=cfg["project_id"], credentials=creds)

# ---------------------------
# Optional Local Utilities
# ---------------------------
# Support both filenames: bigquery_utils_v2.py and bigquery_utils.py

def _import_utils():
    try:
        import bigquery_utils_v2 as utils  # type: ignore
        return utils
    except Exception:
        try:
            import bigquery_utils as utils  # type: ignore
            return utils
        except Exception:
            return None

utils = _import_utils()

# ---------------------------
# Query Helpers
# ---------------------------

@st.cache_data(show_spinner=False)
def run_query(sql: str, params: Optional[dict] = None) -> pd.DataFrame:
    client = get_bq_client()
    job_config = None
    if params:
        # Build query parameters (supports STRING/NUMERIC)
        qps = []
        for k, v in params.items():
            typ = "STRING" if isinstance(v, str) else "FLOAT64"
            qps.append(bigquery.ScalarQueryParameter(k, typ, v))
        job_config = bigquery.QueryJobConfig(query_parameters=qps)
    query_job = client.query(sql, job_config=job_config)
    return query_job.to_dataframe()

# ---------------------------
# UI
# ---------------------------

st.set_page_config(page_title="Freight Quote (Stable)", layout="wide")

st.title("Freight Quote Generator — Stable Web App")
st.caption("Runs in the browser. No local Python installation for end‑users.")

with st.expander("Configuration", expanded=False):
    cfg_ok = True
    try:
        cfg = get_gcp_config()
        st.success(f"GCP Project: {cfg['project_id']}")
    except Exception as e:
        cfg_ok = False
        st.error(f"Credentials not configured: {e}")
        st.info("Set secrets in `.streamlit/secrets.toml` or environment variables. See header docstring.")

left, right = st.columns([1, 2])

with left:
    st.subheader("Route Inputs")
    origin = st.text_input("Origin", "Los Angeles, CA")
    destination = st.text_input("Destination", "New York, NY")
    weight = st.number_input("Weight (lbs)", min_value=0.0, value=100.0, step=10.0)
    get_quote = st.button("Get Quote")

with right:
    st.subheader("Results")

# Example: Either call a utility function, or use inline SQL
# Prefer: A parameterized query against a rate table
RATE_SQL = """
SELECT origin, destination, base_rate, per_lb_rate
FROM `{{PROJECT}}.{{DATASET}}.rate_sheet`
WHERE origin = @origin AND destination = @destination
LIMIT 1
"""

with st.expander("Advanced: SQL / Dataset Settings", expanded=False):
    dataset = st.text_input("BigQuery Dataset", "shipping")
    table_hint = st.text_input("Table (informational)", "rate_sheet")
    st.caption("The app queries a `rate_sheet` with columns: origin, destination, base_rate, per_lb_rate.")

if get_quote and cfg_ok:
    try:
        sql = RATE_SQL.replace("{{PROJECT}}", get_gcp_config()["project_id"]).replace("{{DATASET}}", dataset)
        params = {"origin": origin, "destination": destination}
        df = run_query(sql, params=params)

        if df.empty:
            st.warning("No rate found for this route. Check dataset/table content.")
        else:
            row = df.iloc[0]
            base = float(row["base_rate"])
            per_lb = float(row["per_lb_rate"])
            total = base + per_lb * float(weight)

            st.success(f"Estimated Quote: **${total:,.2f}**")
            st.dataframe(df, use_container_width=True)

            # Export
            out = pd.DataFrame([{
                "origin": origin,
                "destination": destination,
                "weight_lbs": weight,
                "base_rate": base,
                "per_lb_rate": per_lb,
                "estimated_total": total
            }])
            st.download_button("Download Quote (CSV)",
                               data=out.to_csv(index=False).encode("utf-8"),
                               file_name="quote.csv",
                               mime="text/csv")
    except Exception as e:
        st.error("Query failed.")
        st.exception(e)

st.markdown("---")
st.subheader("Batch Quotes (Optional)")
st.caption("Upload a CSV with columns: origin, destination, weight_lbs")

batch_file = st.file_uploader("Upload CSV", type=["csv"], accept_multiple_files=False)
if batch_file and cfg_ok:
    try:
        batch_df = pd.read_csv(batch_file)
        required_cols = {"origin", "destination", "weight_lbs"}
        missing = required_cols - set(c.lower() for c in batch_df.columns)
        if missing:
            st.error(f"Missing required columns: {', '.join(sorted(missing))}")
        else:
            # Normalize column names
            batch_df.columns = [c.lower() for c in batch_df.columns]

            results = []
            for _, r in batch_df.iterrows():
                params = {"origin": str(r["origin"]), "destination": str(r["destination"])}
                sql = RATE_SQL.replace("{{PROJECT}}", get_gcp_config()["project_id"]).replace("{{DATASET}}", dataset)
                df = run_query(sql, params=params)
                if df.empty:
                    results.append({
                        "origin": r["origin"],
                        "destination": r["destination"],
                        "weight_lbs": r["weight_lbs"],
                        "status": "NO_RATE"
                    })
                else:
                    row = df.iloc[0]
                    base = float(row["base_rate"])
                    per_lb = float(row["per_lb_rate"])
                    total = base + per_lb * float(r["weight_lbs"])
                    results.append({
                        "origin": r["origin"],
                        "destination": r["destination"],
                        "weight_lbs": r["weight_lbs"],
                        "base_rate": base,
                        "per_lb_rate": per_lb,
                        "estimated_total": total,
                        "status": "OK"
                    })

            res_df = pd.DataFrame(results)
            st.dataframe(res_df, use_container_width=True)
            st.download_button("Download Results (CSV)",
                               data=res_df.to_csv(index=False).encode("utf-8"),
                               file_name="batch_quotes.csv",
                               mime="text/csv")
    except Exception as e:
        st.error("Batch processing failed.")
        st.exception(e)

st.markdown("---")
st.caption("Tip: deploy to Streamlit Cloud (set secrets) or Cloud Run (containerize). No desktop EXE needed.")
