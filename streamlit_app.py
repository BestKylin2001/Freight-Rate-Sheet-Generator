import os
import json
from functools import lru_cache
from typing import Optional, Dict

import streamlit as st
import numpy as np
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
    gcp = st.secrets["gcp"]
    info = {
        "type": "service_account",
        "project_id": gcp["project_id"],
        "private_key_id": gcp["private_key_id"],
        "private_key": gcp["private_key"],
        "client_email": gcp["client_email"],
        "client_id": gcp["client_id"],
        "token_uri": gcp.get("token_uri", "https://oauth2.googleapis.com/token"),
        "auth_uri": gcp.get("auth_uri", "https://accounts.google.com/o/oauth2/auth"),
        "auth_provider_x509_cert_url": gcp.get("auth_provider_x509_cert_url", "https://www.googleapis.com/oauth2/v1/certs"),
        "client_x509_cert_url": gcp.get("client_x509_cert_url"),
        "universe_domain": gcp.get("universe_domain", "googleapis.com"),
    }
    creds = service_account.Credentials.from_service_account_info(info)
    return bigquery.Client(project=gcp["project_id"], credentials=creds)


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
client = get_bq_client()


# ---------------------------
# Query Helpers
# ---------------------------

@st.cache_data(show_spinner=False)
def load_table(table_name):
    PROJECT_ID = "rate-sheet-sql-465312"
    DATASET_NAME = "ratesheet_processing_dataset"
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_NAME}.{table_name}`"
    return client.query(query).to_dataframe()

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

# 示例数据
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

client, table_names = get_tables_and_client()

st.write("✅ Found the following rate tables in BigQuery:")
st.write(table_names)

selected_table = st.selectbox("Select a Rate Table", table_names)

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
    st.markdown(f"### 🧾 共生成 {len(routes)} 条线路：")

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
                m.loc[:, "表格类型"] = table_type
                m.loc[:, "来源表"] = table
                combined_data.append(m)
                found = True
            else:
                if origin not in df["POL"].unique(): reasons.add(f"缺POL: {origin}")
                if destination not in df["Destination"].unique(): reasons.add(f"缺Destination: {destination}")
                if carrier not in df["Carrier"].unique(): reasons.add(f"缺Carrier: {carrier}")

        if not found:
            print(f"❌ 未匹配: {origin} → {destination} ｜ {carrier} ｜ 原因: {'，'.join(sorted(reasons))}")


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

        st.sidebar.markdown("### 💲 GP20 / GP40 调整")
        gp20_adjust = st.sidebar.number_input("调整 GP20（单位：美元，可为负数）", value=0.0, step=10.0)
        gp40_adjust = st.sidebar.number_input("调整 GP40（单位：美元，可为负数）", value=0.0, step=10.0)

        if 'GP20' in total_df.columns:
            total_df['GP20'] = total_df['GP20'] + gp20_adjust
        if 'GP40' in total_df.columns:
            total_df['GP40'] = total_df['GP40'] + gp40_adjust

        port_cols = ["POL", "Destination", "Carrier", "T_T_TO_POD",
                     "GP20", "GP40", "ISF", "Handling", "Customs Clearance", "Duty",
                     "20' - ALL IN", "40' or HC - ALL IN", "COMM", "COMM_DETAILS", "Expiring_Date", "来源表"]
        door_cols = ["POL", "Destination", "Carrier", "T_T_TO_POD", "GP20", "GP40", "ISF", "Handling", "Customs Clearance", "Duty", "Trucking Fee", "20' - CTF/PP", "40' - CTF/PP", "Chassis ($50/DAY) - min. 2 days", "20' - ALL IN", "40' or HC - ALL IN", "COMM", "COMM_DETAILS", "Expiring_Date", "来源表"]

        display_cols = door_cols if table_type == "Port to Door" else port_cols

        st.markdown("### 📊 合并后的总表：")

        keyword = st.sidebar.text_input("请输入关键词（用于过滤remark/COMM/COMM_DETAILS）")
        filter_action = st.sidebar.radio(
            "选择筛选方式：",
            ("不过滤", "只保留包含关键词的班次", "剔除包含关键词的班次")
        )
        max_shown = st.sidebar.number_input("每条线路最多列出几条最便宜的班次？", min_value=1, step=1, value=5)

        selected_rows = []
        col_20_all_in = "20' - ALL IN"

        grouped = total_df.groupby(["POL", "Destination"])

        for (pol, destination), group in grouped:
            group = group.sort_values(by="GP20", ascending=True)

            if keyword and filter_action != "不过滤":
                contains_keyword = group[['remark', 'COMM', 'COMM_DETAILS']].apply(
                    lambda x: x.astype(str).str.contains(keyword, case=False, na=False)
                ).any(axis=1)
                if filter_action == "只保留包含关键词的班次":
                    group = group[contains_keyword]
                elif filter_action == "剔除包含关键词的班次":
                    group = group[~contains_keyword]

            if not group.empty:
                with st.expander(f"{pol} → {destination}"):
                    selected_in_group = []
                    carrier_group = group.groupby("Carrier")

                    for carrier, carrier_df in carrier_group:
                        include = st.checkbox(f"✔ 包含 {carrier}", value=True, key=f"include_{pol}_{destination}_{carrier}")
                        if include:
                            carrier_df_sorted = carrier_df.sort_values(by="GP20", ascending=True)
                            selected_idx = st.radio(
                                f"{carrier} 班次选择",
                                options=list(range(len(carrier_df_sorted))),
                                format_func=lambda idx: (
                                    f"20尺 {carrier_df_sorted.iloc[idx].get('GP20', '无')}美元 ｜ "
                                    f"40尺 {carrier_df_sorted.iloc[idx].get('GP40', '无')}美元 ｜ "
                                    f"备注: {carrier_df_sorted.iloc[idx].get('remark', '无')} ｜ "
                                    f"COMM: {carrier_df_sorted.iloc[idx].get('COMM', '无')} ｜ "
                                    f"COMM_DETAILS: {carrier_df_sorted.iloc[idx].get('COMM_DETAILS', '无')}"
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

        st.markdown("### ✅ 最终选择的班次列表：")
        st.dataframe(final_selected_df)

        csv = final_selected_df.to_csv(index=False).encode("utf-8-sig")
        st.download_button("📥 下载最终选择后的总表 CSV", csv, "final_selected_routes.csv", "text/csv")
