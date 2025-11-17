import os
import json
from functools import lru_cache
from typing import Optional, Dict
from itertools import product

import streamlit as st
import numpy as np
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# ---------------------------
# Config & Credentials
# ---------------------------

def _get_secret(key: str, default: Optional[str] = None) -> Optional[str]:
    try:
        value = st.secrets.get(key, default)
        # if value is not None:
            # st.write(f"Loaded secret: {key} from st.secrets")
        return value
    except Exception as e:
        # st.write(f"Failed to load {key} from st.secrets: {e}")
        return os.environ.get(key.upper(), default)
``
@lru_cache(maxsize=1)
def get_gcp_config() -> Dict[str, str]:
    """Get GCP configuration from secrets or environment variables."""
    return {
        "project_id": _get_secret("project_id"),
        "service_account_json": _get_secret("service_account_json")
    }

@lru_cache(maxsize=1)
def get_bq_client() -> bigquery.Client:
    cfg = get_gcp_config()
    service_account_json = cfg["service_account_json"]
    
    service_account_json = service_account_json.strip()
    
    try:
        info = json.loads(service_account_json)
    except json.JSONDecodeError:
        # If provided as a python-ish dict string, attempt eval as last resort
        info = eval(service_account_json)  # noqa: S307 - trusted runtime environment
    
    if not isinstance(info, dict):
        raise RuntimeError("service_account_json must be a valid JSON object")
    
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
def load_table(table_name):
    cfg = get_gcp_config()
    dataset_name = _get_secret("dataset_name", "ratesheet_processing_dataset")
    query = f"SELECT * FROM `{cfg['project_id']}.{dataset_name}.{table_name}`"
    return client.query(query).to_dataframe()

@st.cache_resource
def get_tables_and_client():
    client = get_bq_client()
    dataset = "ratesheet_processing_dataset"
    tables = client.list_tables(dataset)
    table_names = [t.table_id for t in tables]
    return client, table_names

# Initialize the client and get table names
client, table_names = get_tables_and_client()

st.write("✅ Found the following rate tables in BigQuery:")
st.write(table_names)

selected_table = st.selectbox("Select a Rate Table", table_names)

if selected_table:
    df = load_table(selected_table)
    df["POL"] = df["POL"].astype(str).str.strip()
    df["Destination"] = df["Destination"].astype(str).str.strip()
    df["Carrier"] = df["Carrier"].astype(str).str.strip()
    date_select = st.multiselect("Select Expiring Dates", sorted(df["Expiring_Date"].dropna().unique()))

    st.write(f"📊 Columns in `{selected_table}`:")
    st.write(df.columns.tolist())

    # Ensure necessary columns exist
    for col in ["COMM", "COMM_DETAILS"]:
        if col not in df.columns:
            df[col] = np.nan
    columns_needed = ["POL", "Destination", "Effective_Date", "Expiring_Date", "T_T_TO_POD", "Carrier", "GP20", "GP40", "COMM", "COMM_DETAILS"]
    existing_columns = [col for col in columns_needed if col in df.columns]
    df = df[existing_columns]

    st.write(f"📊 Showing data from {selected_table}:")
    st.write(df)

    # Sidebar UI
    st.sidebar.header("🔧 Filter Options")
    table_type = st.sidebar.radio("Table Type", ["Port to Port", "Port to Door"])

# Carrier Names
carrier_options = [
    "WANHAI", "SMLM", "YML", "MSC", "OOCL", "ONE", "EMC", "COSCO", 
    "HMM", "HPL", "CMA", "ZIM", "SLS", "TSL", "HEDE", "MATS"
]

origin_ports = [
    'SHENZHEN, GUANGDONG', 'JIUJIANG, GUANGDONG', 'HONG KONG', 'ZHUHAI, GUANGDONG', 
    'SHANGHAI', 'HUANGPU, GUANGDONG', 'XIAMEN, FUJIAN', 'FUZHOU, FUJIAN', 'ZHENJIANG, JIANGSU', 'ZHAPU, ZHEJIANG', 
    'ZHANGJIAGANG, JIANGSU', 'YUEYANG, HUNAN', 'YICHANG, HUBEI', 'YANGZHOU, JIANGSU', 'WUHAN, HUBEI', 'NINGBO, ZHEJIANG', 
    'NANTONG, JIANGSU', 'NANJING, JIANGSU', 'NANCHANG, JIANGXI', 'CHANGZHOU, JIANGSU', 'ANQING, ANHUI', 'XINGANG, TIANJIN', 
    'DALIAN, LIAONING', 'YOKOHAMA, JAPAN', 'VUNG TAU, VIETNAM', 'VISAKHAPATNAM, INDIA', 'TUTICORIN, INDIA', 'TOKYO, JAPAN', 
    'TAOYUAN, TAIWAN', 'TANJUNG PELEPAS, MALAYSIA', 'TAIPEI, TAIWAN', 'TAICHUNG, TAIWAN', 'SURABAYA, INDONESIA', 'SINGAPORE', 
    'SIHANOUKVILLE, CAMBODIA', 'SHIMIZU, JAPAN', 'SEMARANG, INDONESIA', 'QUI NHON, VIETNAM', 'PORT KLANG, MALAYSIA', 
    'PENANG, MALAYSIA', 'PASIR GUDANG, MALAYSIA', 'PALEMBANG, INDONESIA', 'OSAKA, JAPAN', 'NHAVA SHEVA, INDIA', 
    'NAGOYA, JAPAN', 'MUNDRA, INDIA', 'MOJI, JAPAN', 'MANILA, PHILIPPINES', 'MANILA NORTH HARBOUR', 'LAT KRABANG, THAILAND', 
    'LAEM CHABANG, THAILAND', 'KOLKATA(EX CALCUTTA), INDIA', 'KOBE, JAPAN', 'KEELUNG, TAIWAN', 'KARACHI, PAKISTAN', 
    'KAOHSIUNG, TAIWAN', 'JAKARTA, INDONESIA', 'HOCHIMINH CITY, VIETNAM', 'HAKATA, JAPAN', 'HAIPHONG, VIETNAM', 
    'DAVAO, PHILIPPINES', 'DANANG, VIETNAM', 'COLOMBO, SRI LANKA', 'COCHIN, INDIA', 'CHATTOGRAM, BANGLADESH', 
    'CHENNAI, INDIA', 'CEBU, PHILIPPINES', 'CAI MEP, VIETNAM', 'BUSAN, KOREA', 'BELAWAN, INDONESIA', 'BATAM, INDONESIA', 
    'BANGKOK, THAILAND', 'PANJANG, INDONESIA', 'PIPAVAV (VICTOR) PORT, INDIA', 'HAZIRA, INDIA', 'KATTUPALLI, INDIA'
]

destination_ports = [
    'LAX/LGB', 'OAKLAND, CA', 'PORTLAND, OR', 'SEATTLE, WA', 'TACOMA, WA', 'BALTIMORE, MD', 'BOSTON, MA', 'CHARLESTON, SC', 
    'HOUSTON, TX', 'JACKSONVILLE, FL', 'MIAMI, FL', 'NEW ORLEANS, LA', 'NEW YORK, NY', 'PHILADELPHIA, PA', 
    'PORT EVERGLADES, FL', 'SAVANNAH, GA', 'TAMPA, FL', 'WILMINGTON, NC', 'ATLANTA, GA', 'BIRMINGHAM, AL', 'BUFFALO, NY', 
    'CHARLOTTE, NC', 'CHATSWORTH, GA', 'CHICAGO, IL', 'CHIPPEWA FALLS, WI', 'CINCINNATI, OH', 'CLEVELAND, OH', 
    'COLUMBUS, OH', 'CRANDALL, GA', 'DALLAS, TX', 'DENVER, CO', 'DETROIT, MI', 'EAST ST.LOUIS, IL', 'EL PASO, TX', 
    'GREENSBORO, NC', 'HUNTSVILLE, AL', 'INDIANAPOLIS, IN', 'KANSAS CITY, MO', 'LOUISVILLE, KY', 'MEMPHIS, TN', 
    'MINNEAPOLIS, MN', 'NASHVILLE, TN', 'OMAHA, NE', 'PHOENIX, AZ', 'PITTSBURGH, PA', 'RICHMOND, VA', 'SALT LAKE CITY, UT', 
    'SAN ANTONIO, TX', 'SANTA TERESA, NM', 'ST. LOUIS, MO', 'ST. PAUL, MN', 'WORCESTER, MA'
]

@st.cache_resource
def get_tables_and_client():
    dataset = "ratesheet_processing_dataset"
    tables = client.list_tables(dataset)
    table_names = [t.table_id for t in tables]
    return client, table_names

# Streamlit UI
st.title("Shipping Rates Query")
origin_select = st.multiselect("Select Origin Ports", origin_ports)
destination_select = st.multiselect("Select Destination Ports", destination_ports)
carrier_select = st.multiselect("Select Carrier", carrier_options)

filtered_data = df[
    (df['POL'].isin(origin_select)) &
    (df['Destination'].isin(destination_select)) &
    (df['Carrier'].isin(carrier_select)) &
    (df["Expiring_Date"].isin(date_select))
]

trucking_fee = 0
if table_type == "Port to Door":
    trucking_fee = st.sidebar.number_input("Trucking Fee (USD)", min_value=0.0, step=10.0)

if origin_select and destination_select and carrier_select:
    routes = list(product(origin_select, destination_select, carrier_select))
    routes = [(o.upper(), d.upper(), c.upper()) for o,d,c in routes]
    st.markdown(f"### 🧾 Total {len(routes)} routes matched：")

    fixed_fields = {
        "ISF": 25,
        "Handling": 50,
        "Customs Clearance": 80,
        "Duty": "AT COST",
        "20' - CTF/PP": 48,
        "40' - CTF/PP": 95,
        "Chassis ($50/DAY) - min. 2 days": 100,
        "Trucking Fee": trucking_fee
    }

    combined_data = []

    for origin, destination, carrier in routes:
        found = False
        reasons = set()

        for table in table_names:
            df = load_table(table)
            if not {"POL","Destination","Carrier"}.issubset(df.columns):
                continue
            for c in ["POL","Destination","Carrier"]:
                df[c] = df[c].astype(str).str.strip().str.upper()

            m = df[(df["POL"]==origin) &
                   (df["Destination"]==destination) &
                   (df["Carrier"]==carrier)].copy()

            if not m.empty:
                for col, val in fixed_fields.items():
                    m.loc[:, col] = val
                m["20' - ALL IN"] = (m["GP20"] + fixed_fields["ISF"] +
                                     fixed_fields["Handling"] + fixed_fields["Customs Clearance"])
                m["40' or HC - ALL IN"] = (m["GP40"] + fixed_fields["ISF"] +
                                           fixed_fields["Handling"] + fixed_fields["Customs Clearance"])
                if table_type == "Port to Door":
                    m["20' - ALL IN"] += (fixed_fields["Trucking Fee"] +
                                          fixed_fields["20' - CTF/PP"] +
                                          fixed_fields["Chassis ($50/DAY) - min. 2 days"])
                    m["40' or HC - ALL IN"] += (fixed_fields["Trucking Fee"] +
                                                fixed_fields["40' - CTF/PP"] +
                                                fixed_fields["Chassis ($50/DAY) - min. 2 days"])
                m.loc[:, "Sheet type"] = table_type
                m.loc[:, "Source table"] = table
                combined_data.append(m)
                found = True
            else:
                if origin not in df["POL"].unique(): reasons.add(f"Cannot find POL: {origin}")
                if destination not in df["Destination"].unique(): reasons.add(f"Cannot find Destination: {destination}")
                if carrier not in df["Carrier"].unique(): reasons.add(f"Cannot find Carrier: {carrier}")

        if not found:
            print(f"❌ Unmatched: {origin} → {destination} ｜ {carrier} ｜ Reason: {'，'.join(sorted(reasons))}")


    if combined_data:
        total_df = pd.concat(combined_data, ignore_index=True)
        total_df["ISF"] = 25
        total_df["Handling"] = 50
        total_df["Customs Clearance"] = 80
        total_df["Duty"] = "AT COST"

        def is_integer_string(x):
            try:
                return float(x).is_integer()
            except:
                return False

        if "T_T_TO_POD" in total_df.columns:
            total_df["T_T_TO_POD"] = total_df["T_T_TO_POD"].apply(
                lambda x: f"{int(float(x))}-{int(float(x))+3} DAYS"
                if pd.notnull(x) and is_integer_string(x)
                else x
            )

        st.sidebar.markdown("### 💲 GP20 / GP40 Price Adjust")
        gp20_adjust = st.sidebar.number_input("Markup/Markdown GP20（Unit：$, positive/negative）", value=0.0, step=10.0)
        gp40_adjust = st.sidebar.number_input("Markup/Markdown GP40（Unit：$, positive/negative）", value=0.0, step=10.0)

        if 'GP20' in total_df.columns:
            total_df['GP20'] = total_df['GP20'] + gp20_adjust
        if 'GP40' in total_df.columns:
            total_df['GP40'] = total_df['GP40'] + gp40_adjust

        port_cols = ["POL", "Destination", "Carrier", "T_T_TO_POD",
                     "GP20", "GP40", "ISF", "Handling", "Customs Clearance", "Duty",
                     "20' - ALL IN", "40' or HC - ALL IN", "COMM", "COMM_DETAILS", "Expiring_Date", "来源表"]
        door_cols = ["POL", "Destination", "Carrier", "T_T_TO_POD", "GP20", "GP40", "ISF", "Handling", "Customs Clearance", "Duty", "Trucking Fee", "20' - CTF/PP", "40' - CTF/PP", "Chassis ($50/DAY) - min. 2 days", "20' - ALL IN", "40' or HC - ALL IN", "COMM", "COMM_DETAILS", "Expiring_Date", "来源表"]

        display_cols = door_cols if table_type == "Port to Door" else port_cols

        st.markdown("###✅ Final Ocean Freight Rate Sheet：")

        keyword = st.sidebar.text_input("Input keywords（Filter remark/COMM/COMM_DETAILS）")
        filter_action = st.sidebar.radio(
            "Filter options：",
            ("Do not filter", "Keep shifts containing keywords:", "Exclude shifts containing keywords:")
        )
        max_shown = st.sidebar.number_input("List up to how many cheapest shifts per route？", min_value=1, step=1, value=5)

        selected_rows = []
        col_20_all_in = "20' - ALL IN"

        grouped = total_df.groupby(["POL", "Destination"])

        for (pol, destination), group in grouped:
            group = group.sort_values(by="GP20", ascending=True)

            if keyword and filter_action != "Do not filter":
                contains_keyword = group[['remark', 'COMM', 'COMM_DETAILS']].apply(
                    lambda x: x.astype(str).str.contains(keyword, case=False, na=False)
                ).any(axis=1)
                if filter_action == "Keep shifts containing keywords:":
                    group = group[contains_keyword]
                elif filter_action == "Exclude shifts containing keywords:":
                    group = group[~contains_keyword]

            if not group.empty:
                with st.expander(f"{pol} → {destination}"):
                    selected_in_group = []
                    carrier_group = group.groupby("Carrier")

                    for carrier, carrier_df in carrier_group:
                        include = st.checkbox(f"✔ include {carrier}", value=True, key=f"include_{pol}_{destination}_{carrier}")
                        if include:
                            carrier_df_sorted = carrier_df.sort_values(by="GP20", ascending=True)
                            selected_idx = st.radio(
                                f"{carrier} Carrier options:",
                                options=list(range(len(carrier_df_sorted))),
                                format_func=lambda idx: (
                                    f"20' {carrier_df_sorted.iloc[idx].get('GP20', 'Null')}$ ｜ "
                                    f"40' {carrier_df_sorted.iloc[idx].get('GP40', 'Null')}$ ｜ "
                                    f"Remark: {carrier_df_sorted.iloc[idx].get('remark', 'Null')} ｜ "
                                    f"COMM: {carrier_df_sorted.iloc[idx].get('COMM', 'Null')} ｜ "
                                    f"COMM_DETAILS: {carrier_df_sorted.iloc[idx].get('COMM_DETAILS', 'Null')}"
                                ),
                                key=f"{pol}_{destination}_{carrier}"
                            )
                            selected_in_group.append(carrier_df_sorted.iloc[selected_idx])

                    if selected_in_group:
                        selected_df = pd.DataFrame(selected_in_group)
                        selected_df = selected_df.sort_values(by="GP40", ascending=True).head(max_shown)
                        selected_rows.extend(selected_df.to_dict("records"))

        final_selected_df = pd.DataFrame(selected_rows)
        final_selected_df = final_selected_df.reindex(columns=display_cols)

        if table_type == "Port to Port":
            final_selected_df.columns = [
                "ORIGIN", "DESTINATION", "Carrier", "Transit time (port to port)", "20'", "40' or HC", "ISF", "Handling", "Customs Clearance", "Duty", "20' - ALL IN", "40' or HC - ALL IN", "COMM", "COMM_DETAILS", "Expiring_Date", "来源表"
            ]
        else:
            final_selected_df.columns = [
                "ORIGIN", "DESTINATION", "Carrier", "Transit time (port to port)", "20'", "40' or HC", "ISF", "Handling", "Customs Clearance", "Duty", "Trucking Fee", "20' - CTF/PP", "40' - CTF/PP", "Chassis ($50/DAY) - min. 2 days", "20' - ALL IN", "40' or HC - ALL IN", "COMM", "COMM_DETAILS", "Expiring_Date", "来源表"
            ]

        st.markdown("### ✅ Final Ocean Freight Rate Sheet：")
        st.dataframe(final_selected_df)

        csv = final_selected_df.to_csv(index=False).encode("utf-8-sig")
        st.download_button("📥 Download final CSV", csv, "final_selected_routes.csv", "text/csv")
