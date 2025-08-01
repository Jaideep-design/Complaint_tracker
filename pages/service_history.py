# -*- coding: utf-8 -*-
"""
Streamlit app: Solar AC Complaint Tracker
Added search filters for Customer Name and Device ID.
Created on Fri Jul  4 15:19:18 2025
@author: Admin
"""
import re
import numpy
import base64
import json
import streamlit as st
import pandas as pd
from datetime import datetime
import gspread
from google.oauth2 import service_account
from typing import Optional
from collections import Counter
from google.oauth2.service_account import Credentials

# ───────────────────────── CONFIG ─────────────────────────
SERVICE_ACCOUNT_FILE = r"C:\Users\Admin\Desktop\solar-ac-customer-mapping-905e295dd0db.json"

SHEET_ID_1 = "1px_3F6UEjE3hD6UQMoCGThI7X1o9AK4ERfqwfOKlfY4"
SHEET_ID_2 = "1z1dyhCXHLN3pSZBhRdpmJ3XwvwV9zF7_QJ0qsZRSLzU"
SHEET_ID_3 = "11CBVvoJjfgvAaFsS-3I_sqQxql8n53JfSZA8CGT9mvA"

COMMENTS_SHEET_ID = "1vqk13WA77LuSl0xzb54ESO6GSUfqiM9dUgdLfnWdaj0"
COMMENTS_SHEET_NAME = "solarac_Comments_log"

TECH_COMMENTS_SHEET_ID = "1HSzwATv-nlzoIIvNNLwHQtRLYm_znPcXj1wYL5BUWXk"  # Your provided ID
TECH_COMMENTS_SHEET_NAME = "Technician_comments"  # Or whatever the technician sheet is called

SELECTED_COLUMNS_1 = [
    "Ticket ID",
    "Name",
    "Master Controller Serial No.",
    "Inverter Serial No.",
    "Status",
    "Created At",
    "Problem",
    "Problem Description",
    "Mob No.",
    "Issue Resolutions Plan",
    "Site Address",
    'Serial Number',
    'Complaint Reg Date',
    'Resolution Method',
    'Component',
    'Problem',
    "Solution",
    "Remarks",
    "Part Type",
    "Part Description",
    'Total Breakdown',
    'No of Solar panel',
    '1P-Voltage',
    'Battery Voltage',
    'Battery Capacity in AH',
    'Service Start Date', 
    'Service Start Time',
    "Service Completion Date",
    "Service Completion Time",
]
SELECTED_COLUMNS_2 = [
    "Ticket ID",
    'Name',
    'Created At',
    'Mob No.',
    'Site Address',
    'Inverter Serial No.',
    'Issue Resolutions Plan',
    'Assigned Service Engineer',
    'Total Breakdown Time(in Days)'
    'Issue Resolutions Plan',
    'Ecozen-Master Controller Serial No.',
    "Remark",
    "Problem Description",
    "Date of Issue",
    'Status',
    'Additional Remark'
]
SELECTED_COLUMNS_3 = [
    "Phone Number",
    "Customer ID",
    "Customer Name",
    'Varient Name',
    "Part ID",
    "Part ID Description",
]

sheet1_fields = [
    "Ticket ID", "Name", "Master Controller Serial No.", "Inverter Serial No.",
    "Status", "Created At", "Problem", "Problem Description", "Mob No.",
    "Issue Resolutions Plan", "Site Address", 'New Part', "Old/Replaced Part", "Complaint Reg Date",
    "Resolution Method", "Component", "Solution", "Remarks", "Part Type",
    "Part Description", "Total Breakdown", "No of Solar panel",
    "1P-Voltage", "Battery Voltage", "Battery Capacity in AH",
    "Service Completion Date", "Service Completion Time", "Service Start Date", "Service Start Time"
]

sheet2_fields = [
    "Ticket ID", "Name", "Created At", "Mob No.", "Site Address",
    "Inverter Serial No.", "Issue Resolutions Plan", "Assigned Service Engineer",
    "Total Breakdown Time(in Days)", "Ecozen-Master Controller Serial No.",
    "Remark", "Problem Description", "Date of Issue", "Status", "Additional Remark",
]

sheet3_fields = [
    "Phone Number",
    "Customer ID",
    "Customer Name",
    'Varient Name',
    "Part ID",
    "Part ID Description",
]
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

RENAME_MAP_1 = {
    "Serial Number": "New Part",
    "Serial Number_2": "Old/Replaced Part",
}

# Decode and load credentials from Streamlit Secrets
key_json = base64.b64decode(st.secrets["gcp_service_account"]["key_b64"]).decode("utf-8")
service_account_info = json.loads(key_json)

creds = service_account.Credentials.from_service_account_info(
    service_account_info, scopes=SCOPES
)

# creds = service_account.Credentials.from_service_account_file(
#     SERVICE_ACCOUNT_FILE, scopes=SCOPES
# )

gc = gspread.authorize(creds)
comment_ws = gc.open_by_key(COMMENTS_SHEET_ID).worksheet(COMMENTS_SHEET_NAME)
tech_comment_ws = gc.open_by_key(TECH_COMMENTS_SHEET_ID).worksheet(TECH_COMMENTS_SHEET_NAME)
# %%
# ───────────────────────── HELPERS ─────────────────────────
def clean_phone_number(value):
    if pd.isna(value) or str(value).strip().upper() in ["NA", "", "#ERROR!"]:
        return None

    # If multiple numbers, take the first
    first_part = str(value).split(",")[0].strip()

    # Remove all non-digit characters
    digits = re.sub(r"\D", "", first_part)

    # Handle country code (e.g., 91 or +91)
    if len(digits) > 10:
        digits = digits[-10:]  # take last 10 digits
    
    return digits if len(digits) == 10 else None


def read_selected_columns(sheet_id, selected_columns, rename_duplicates=None):
    """
    Read selected columns from a Google Sheet, handling duplicate column names.
    Optionally rename specific duplicates via `rename_duplicates` dict.

    Args:
        sheet_id (str): Google Sheet ID
        selected_columns (list): Columns to select (base names)
        rename_duplicates (dict): Mapping like {"Serial Number_2": "Serial Number (Returned)"}
    """

    # Decode and load credentials from Streamlit Secrets
    key_json = base64.b64decode(st.secrets["gcp_service_account"]["key_b64"]).decode("utf-8")
    service_account_info = json.loads(key_json)

    # Authenticate using decoded credentials
    creds = service_account.Credentials.from_service_account_info(
        service_account_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    )
    gc = gspread.authorize(creds)

    # Get worksheet data
    worksheet = gc.open_by_key(sheet_id).sheet1
    data = worksheet.get_all_values()

    # Extract header and data
    raw_header = data[0]
    rows = data[1:]

    # Rename duplicate columns
    def make_unique(headers):
        counter = Counter()
        new_headers = []
        for h in headers:
            counter[h] += 1
            if counter[h] > 1:
                new_headers.append(f"{h}_{counter[h]}")
            else:
                new_headers.append(h)
        return new_headers

    header = make_unique(raw_header)

    # Apply to DataFrame
    df = pd.DataFrame(rows, columns=header)

    # Keep only selected columns based on base name
    def base_name(col):
        return col.split("_")[0] if "_" in col else col

    df = df[[col for col in df.columns if base_name(col) in selected_columns]]

    # Rename duplicates if mapping is provided
    if rename_duplicates:
        df.rename(columns=rename_duplicates, inplace=True)

    return df

def process_sheets_and_transform() -> pd.DataFrame:
    """Read, merge, clean, and pivot data from the three Google Sheets."""

    # Step 1 – Read sheets
    df1 = read_selected_columns(SHEET_ID_1, SELECTED_COLUMNS_1, rename_duplicates=RENAME_MAP_1)
    df2 = read_selected_columns(SHEET_ID_2, SELECTED_COLUMNS_2)
    
    df2 = df2[df2["Ticket ID"].notna() & (df2["Ticket ID"].astype(str).str.strip() != "")]
    
    df2["Mob No."] = df2["Mob No."].apply(clean_phone_number)
    df3 = read_selected_columns(SHEET_ID_3, SELECTED_COLUMNS_3)

    # Step 2 – Merge Sheet 1 & 2 on "Ticket ID"
    df_merged = pd.merge(df1, df2, on="Ticket ID", how="outer")
    
    # Step – Create unified 'Mob No.' column by taking first non-null value
    df_merged["Mob No."] = df_merged[["Mob No._x", "Mob No._y"]].bfill(axis=1).iloc[:, 0]
    
    # Normalize phone numbers: keep only last 10 digits of digits-only version
    df_merged["Mob No."] = (
        df_merged["Mob No."]
        .astype(str)
        .str.replace(r"\D", "", regex=True)  # Remove all non-digits
        .str[-10:]  # Keep only last 10 digits
    )

    # Step 3 – Normalize phone numbers in df_merged and df3
    if "Mob No." in df_merged.columns:
        df_merged["Mob No."] = df_merged["Mob No."].str.replace(r"\D", "", regex=True).str[-10:]

    if "Phone Number" in df3.columns:
        df3["Phone Number"] = df3["Phone Number"].str.replace(r"\D", "", regex=True).str[-10:]
        
    # df_merged_filtered = df_merged[df_merged["Mob No."].notna()]

    # Filter df3 to only include non-empty, non-NaN phone numbers
    df3_filtered = df3[df3["Phone Number"].notna() & (df3["Phone Number"].str.strip() != "")]
    
    # Step 4 – Merge df3 using phone numbers
    df_final = pd.merge(
        df_merged,
        df3_filtered,
        left_on="Mob No.",
        right_on="Phone Number",
        how="left"
    )

    # Drop unwanted columns before pivoting
    df_final = df_final.drop(columns=["Mob No._x", "Mob No._y", "Phone Number"], errors="ignore")

    # Step 5 – Convert to vertical format
    vertical_rows = []
    for _, row in df_final.iterrows():
        ticket_id = row.get("Ticket ID")
        issue_date = row.get("Date of Issue")
        for col_name, val in row.items():
            if col_name in ["Ticket ID", "Date of Issue"]:
                continue
            if pd.isna(val) or str(val).strip() == "":
                continue
            vertical_rows.append(
                {
                    "Ticket ID": ticket_id,
                    "Issue_Date": issue_date,
                    "Fields": col_name,
                    "Value": val,
                }
            )

    vertical_df = pd.DataFrame(vertical_rows)
    vertical_df = vertical_df.sort_values(["Ticket ID", "Issue_Date", "Fields"])

    return vertical_df

def load_comments() -> pd.DataFrame:
    """Fetch the comments sheet as a tidy DataFrame (Topic, Timestamp, Comment)."""

    try:
        records = comment_ws.get_all_records()
        df = pd.DataFrame(records)
        df.columns = df.columns.str.strip()
        if "Ticket ID" in df.columns and "Topic" not in df.columns:
            df = df.rename(columns={"Ticket ID": "Topic"})

        for col in ["Topic", "Timestamp", "Comment"]:
            if col not in df.columns:
                df[col] = None
        return df[["Topic", "Timestamp", "Comment"]]
    except Exception as e:  # noqa: BLE001
        st.error(f"Error reading comments: {e}")
        return pd.DataFrame(columns=["Topic", "Timestamp", "Comment"])


def add_comment(topic: str, text: str) -> None:
    """Append a new comment (UTC timestamp) to the Google Sheet."""

    try:
        stamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        comment_ws.append_row([topic, stamp, text.strip()])
    except Exception as e:  # noqa: BLE001
        st.error(f"Error adding comment: {e}")
        
def load_tech_comments() -> pd.DataFrame:
    try:
        records = tech_comment_ws.get_all_records()
        df = pd.DataFrame(records)
        df.columns = df.columns.str.strip()
        if "Ticket ID" in df.columns and "Topic" not in df.columns:
            df = df.rename(columns={"Ticket ID": "Topic"})

        for col in ["Topic", "Timestamp", "Comment"]:
            if col not in df.columns:
                df[col] = None
        return df[["Topic", "Timestamp", "Comment"]]
    except Exception as e:
        st.error(f"Error reading technician comments: {e}")
        return pd.DataFrame(columns=["Topic", "Timestamp", "Comment"])


def add_tech_comment(topic: str, text: str) -> None:
    try:
        stamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        tech_comment_ws.append_row([topic, stamp, text.strip()])
    except Exception as e:
        st.error(f"Error adding technician comment: {e}")

def strip_suffix(field_name: str) -> str:
    """Strip _x or _y suffix from a field name."""
    return re.sub(r"(_x|_y)$", "", field_name).strip()

def build_display_df(source_df: pd.DataFrame, ticket_id: str) -> pd.DataFrame:
    display_rows = []
    
    # Filter for the given ticket_id
    df = source_df[source_df['Ticket ID'] == ticket_id]

    # Fill NaN in Issue_Date with placeholder for grouping
    df['Issue_Date'] = df['Issue_Date'].fillna('No Issue Date')

    # Group by Issue_Date and Fields
    grouped = df.groupby(['Issue_Date', 'Fields'])

    for (issue_dt, field), group in grouped:
        unique_values = group['Value'].dropna().unique()
        
        if len(unique_values) == 1:
            value = unique_values[0]
        else:
            value = ", ".join(str(v) for v in unique_values)

        display_rows.append({
            "Ticket ID": ticket_id,
            "Issue Date": issue_dt,
            "Fields": field,
            "Value": value,
        })

    return pd.DataFrame(display_rows)

def build_parts_display_df(source_df: pd.DataFrame, ticket_id: str) -> pd.DataFrame:
    # Define expected fields
    part_fields = [
        "Part Type",
        "Part Description",
        "New Part",
        "Old/Replaced Part"
    ]

    # Filter for the ticket
    df = source_df[source_df["Ticket ID"] == ticket_id].copy()

    # Initialize dictionary
    field_values = {field: [] for field in part_fields}

    for field in part_fields:
        values = df[df["Fields"] == field]["Value"].dropna().tolist()
        field_values[field] = values

    # Find max length to align all lists
    max_len = max((len(v) for v in field_values.values()), default=0)

    # Pad shorter lists
    for field in part_fields:
        while len(field_values[field]) < max_len:
            field_values[field].append(None)

    # Build rows
    rows = []
    for i in range(max_len):
        row = {field: field_values[field][i] for field in part_fields}
        rows.append(row)

    # Build DataFrame, ensuring schema is retained
    df_result = pd.DataFrame(rows, columns=part_fields)

    return df_result

# Extract values from display dataframes
def get_field_value(df_display, field_name):
    if df_display.empty or "Fields" not in df_display.columns or "Value" not in df_display.columns:
        return None

    row = df_display[df_display["Fields"] == field_name]
    if row.empty:
        return None

    value = row["Value"].values
    return value[0] if len(value) > 0 else None

# Helper function to display summary fields in a table-like format
def render_summary_table_multi_column(summary_data, columns=3):
    items = list(summary_data.items())
    chunk_size = (len(items) + columns - 1) // columns  # Divide items evenly among columns

    cols = st.columns(columns)
    for i in range(columns):
        col_items = items[i * chunk_size : (i + 1) * chunk_size]
        for key, value in col_items:
            # Format list or Series values as comma-separated
            if isinstance(value, (list, pd.Series)):
                value = ", ".join(map(str, value))
            elif pd.isna(value):
                value = "—"
            else:
                value = str(value)
            # Display field and value in column
            cols[i].markdown(f"**{key}**  \n{value}")

# Extract part-related tables
def render_part_table(label, column_data):
    st.markdown(f"**{label}**")
    for i, val in enumerate(column_data, start=1):
        st.markdown(f"- {i}. {val}")
        
def linkify(comment: str) -> str:
    url_pattern = r'(https?://[^\s]+)'
    return re.sub(url_pattern, r'[\1](\1)', comment)

def summary_for_customer(df):
    # Step 1: Convert relevant date columns to datetime (only if they exist)
    date_columns = ['Created At_x', 'Created At_y', 'Complaint Reg Date', 'Service Start Date']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Step 2: Create 'Created At' field with fallback logic
    if 'Created At_y' in df.columns and 'Created At_x' in df.columns:
        df['Created At'] = df['Created At_y'].combine_first(df['Created At_x'])
    elif 'Created At_y' in df.columns:
        df['Created At'] = df['Created At_y']
    elif 'Created At_x' in df.columns:
        df['Created At'] = df['Created At_x']
    else:
        df['Created At'] = pd.NaT
    
    df = df.sort_values(by='Created At').reset_index(drop=True)

    if df.empty:
        return pd.DataFrame()  # handle case where df has no valid rows

    # Step 3: Extract customer summary (from latest ticket)
    latest = df.iloc[-1]

    def safe_get(row, col):
        return row[col] if col in row and pd.notna(row[col]) else np.nan

    customer_summary = {
        'Customer Name': safe_get(latest, 'Customer Name'),
        'Customer ID': safe_get(latest, 'Customer ID'),
        'Phone Number': safe_get(latest, 'Mob No.'),
        'Installation Address': safe_get(latest, 'Site Address_x'),
        'Master Controller Serial No.': safe_get(latest, 'Master Controller Serial No.'),
        'Installation Summary': safe_get(latest, 'Varient Name'),
        'Latest Ticket Date': safe_get(latest, 'Created At'),
    }

    # Step 4: Helper function to combine columns safely
    def combine_first_row(row, *columns):
        return next((row[col] for col in columns if col in row and pd.notna(row[col])), np.nan)

    # Step 5: Create timeline
    timeline = []
    for _, row in df.iterrows():
        ticket_data = {
            'Ticket ID': safe_get(row, 'Ticket ID'),
            'Issue Date': safe_get(row, 'Created At'),
            'Complaint Reg Date': safe_get(row, 'Complaint Reg Date'),
            'Created At': safe_get(row, 'Created At'),
            'Problem Description': combine_first_row(row, 'Problem Description_y', 'Problem Description_x'),
            'Customer Remark': combine_first_row(row, 'Remark'),
            # 'Issue Resolution Plan (Customer)': safe_get(row, 'Issue Resolutions Plan_y'),
            'Issue Resolution Plan (Technician)': combine_first_row(row, 'Issue Resolutions Plan_x', 'Issue Resolutions Plan_y'),
            'Service Start Date': safe_get(row, 'Service Start Date'),
            'Service Completion Date': safe_get(row, 'Service Completion Date'),
            'Component': safe_get(row, 'Component'),
            'Part Description': combine_first_row(row, 'Part Description', 'Part Description_2'),
            'Technician Remarks': combine_first_row(row, 'Remarks'),
            'Solution': safe_get(row, 'Solution'),
            'New Part': safe_get(row, 'New Part'),
            'Old/Replaced Part': safe_get(row, 'Old/Replaced Part'),
        }
        timeline.append(ticket_data)

    timeline_df = pd.DataFrame(timeline)
    return timeline_df



# %%
# ───────────────────────── STREAMLIT UI ─────────────────────────
st.set_page_config(page_title="Solar AC Complaint Tracker", layout="wide")
st.title("📊 Service Timeline for Customer")

# ---------- Refresh ----------
if st.button("🔄 Refresh & Process Data"):
    with st.spinner("Fetching data from Google Sheets…"):
        st.session_state.vertical_df = process_sheets_and_transform()
    st.success("✅ Data refreshed and processed!")

# ---------- Main ----------
if "vertical_df" in st.session_state:
    vertical_df = st.session_state.vertical_df.copy()

    # ── Sidebar filters ──
    st.sidebar.header("🔎 Filters")
    
    # Get unique values for dropdowns
    name_df = vertical_df[vertical_df["Fields"].str.lower().isin(["name_x", "customer name", "name_y"])]

    all_names = sorted(name_df["Value"].dropna().unique())
    # all_devices = sorted(device_df["Value"].dropna().unique())

    # Dropdown search filters
    search_name = st.sidebar.selectbox(
        "Search by Customer Name",
        options=[""] + all_names,
        format_func=lambda x: "Select..." if x == "" else x
    )

    # # Global search text input
    # global_search = st.sidebar.text_input("🔍 Global Search", "")

    # Initialize candidate tickets from all available ones
    candidate_ids = set(vertical_df["Ticket ID"].dropna())
    
    # Apply name filter
    if search_name:
        name_mask = (
            vertical_df["Fields"].str.lower().isin(["name_x", "customer name", "name_y"])
            & vertical_df["Value"].str.contains(search_name, case=False, na=False)
        )
        candidate_ids &= set(vertical_df.loc[name_mask, "Ticket ID"])

    # # Apply global keyword filter
    # if global_search:
    #     relevant_keywords = ["problem", "remark", "issue", "resolutions", "description", "observation", "plan", "name", "controller", "serial", "engineer"]

    #     field_mask = vertical_df["Fields"].str.lower().apply(
    #         lambda x: any(keyword in x for keyword in relevant_keywords)
    #     )

    #     def normalize(text):
    #         if pd.isna(text):
    #             return ""
    #         return text.lower().replace(" ", "")

    #     normalized_search = normalize(global_search)
    #     value_normalized = vertical_df["Value"].astype(str).apply(normalize)

    #     keyword_mask = field_mask & value_normalized.str.contains(normalized_search, na=False)
    #     candidate_ids &= set(vertical_df.loc[keyword_mask, "Ticket ID"])

    candidate_ids = sorted(candidate_ids)
    if not candidate_ids:
        st.sidebar.info("No tickets match the given search criteria.")
        st.stop()

    # ── Matching Tickets Display ──
    st.subheader(f"🎯 Tickets Matching '{search_name}'")

    # Filter vertical_df to include only rows from matching ticket IDs
    matching_df = vertical_df[vertical_df["Ticket ID"].isin(candidate_ids)].copy()

    # Pivot data to wide format
    pivot_df = matching_df.pivot_table(index='Ticket ID', columns='Fields', values='Value', aggfunc='first').reset_index()

    for date_field in ['Complaint Reg Date', 'Created At_x', 'Issue_Date', 'Service Start Date', 'Service Completion Date']:
        if date_field in pivot_df.columns:
            pivot_df[date_field] = pd.to_datetime(pivot_df[date_field], errors='coerce')

    sort_field = next((col for col in ['Complaint Reg Date', 'Created At_x', 'Issue_Date'] if col in pivot_df.columns), 'Ticket ID')
    pivot_df = pivot_df.sort_values(by=sort_field)
    history_df = summary_for_customer(pivot_df)
    # pivot_df = pivot_df.dropna(subset=['Ticket ID'])
    # pivot_df = pivot_df[pivot_df['Ticket ID'].astype(str).str.strip() != '']
    
    # # Display Timeline View
    # st.subheader("🕒 Ticket History Timeline")
    
    # Sort history_df by 'Issue Date' if available
    if 'Issue Date' in history_df.columns:
        history_df = history_df.sort_values(by='Issue Date')
    
    # Iterate and show timeline-like blocks
    for idx, row in history_df.iterrows():
        with st.expander(f"🧾 Ticket ID: {row['Ticket ID']} | Date of Issue: {row['Issue Date'].strftime('%Y-%m-%d') if pd.notna(row['Issue Date']) else 'N/A'}"):
            st.markdown(f"**Problem Description:** {row['Problem Description'] or 'N/A'}")
            st.markdown(f"**Customer Helpline Remark:** {row['Customer Remark'] or 'N/A'}")
            # st.markdown(f"**Issue Resolution Plan (Customer):** {row['Issue Resolution Plan (Customer)'] or 'N/A'}")
            st.markdown(f"**Issue Resolution Plan (Technician):** {row['Issue Resolution Plan (Technician)'] or 'N/A'}")
            st.markdown(f"**Solution:** {row['Solution'] or 'N/A'}")
            st.markdown(f"**Service Start Date:** {row['Service Start Date'].strftime('%Y-%m-%d') if pd.notna(row['Service Start Date']) else 'N/A'}")
            st.markdown(f"**Service Completion Date:** {row['Service Completion Date'].strftime('%Y-%m-%d') if pd.notna(row['Service Completion Date']) else 'N/A'}")

            
            st.markdown("---")
            # Create a two-column layout for additional info
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(f"**Component:** {row['Component'] or 'N/A'}")
                st.markdown(f"**Part Description:** {row['Part Description'] or 'N/A'}")
                st.markdown(f"**New Part:** {row['New Part'] or 'N/A'}")
            with col2:
                st.markdown(f"**Technician Remarks:** {row['Technician Remarks'] or 'N/A'}")
                st.markdown(f"**Old/Replaced Part:** {row['Old/Replaced Part'] or 'N/A'}")
                
    
    # Optionally add a horizontal line
    st.markdown("---")


    
