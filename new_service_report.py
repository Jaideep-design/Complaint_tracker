# -*- coding: utf-8 -*-
"""
Streamlit app: Solar AC Complaint Tracker
Added search filters for Customer Name and Device ID.
Created on Fri Jul  4 15:19:18 2025
@author: Admin
"""
import re
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
# SERVICE_ACCOUNT_FILE = r"C:\Users\Admin\Desktop\solar-ac-customer-mapping-905e295dd0db.json"

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
    '1. AC Serial Number',
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
    'Remarks (if any)',
    "Part ID",
    "Part ID Description",
]

sheet1_fields = [
    "Ticket ID", "Name", "Master Controller Serial No.", "Inverter Serial No.",
    "Status", "Created At", "Problem", "Problem Description", "Mob No.",
    "Issue Resolutions Plan", "Site Address", 'New Part', "Old/Replaced Part", "Complaint Reg Date",
    "Resolution Method", "Component", "Solution", "Remarks", "Part Type",
    "Part Description", "Total Breakdown", "1. AC Serial Number", "No of Solar panel",
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
    'Remarks (if any)',
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

# creds = service_account.Credentials.from_service_account_file(
#     SERVICE_ACCOUNT_FILE, scopes=SCOPES
# )

# Decode and load credentials from Streamlit Secrets
key_json = base64.b64decode(st.secrets["gcp_service_account"]["key_b64"]).decode("utf-8")
service_account_info = json.loads(key_json)

creds = service_account.Credentials.from_service_account_info(
    service_account_info, scopes=SCOPES
)

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


def get_value(field_name: str) -> Optional[str]:
    # Try _x version first
    row = df_ticket[df_ticket["Fields"] == f"{field_name}_x"]
    if not row.empty:
        return row["Value"].values[0]
    
    # Then try _y version
    row = df_ticket[df_ticket["Fields"] == f"{field_name}_y"]
    if not row.empty:
        return row["Value"].values[0]    
    
    # Finally try without suffix
    row = df_ticket[df_ticket["Fields"] == field_name]
    if not row.empty:
        return row["Value"].values[0]

    return None

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
# %%
# ───────────────────────── STREAMLIT UI ─────────────────────────
st.set_page_config(page_title="Solar AC Complaint Tracker", layout="wide")
st.title("📊 Complaint History")

# ---------- Refresh ----------
if st.button("🔄 Refresh & Process Data"):
    with st.spinner("Fetching data from Google Sheets…"):
        st.session_state.vertical_df = process_sheets_and_transform()
    st.success("✅ Data refreshed and processed!")

# ---------- Main ----------
if "vertical_df" in st.session_state:
    vertical_df = st.session_state.vertical_df.copy()

    # ── Sidebar filters ──
    st.sidebar.header("🔎 Filters")
    
    # Filter for relevant fields
    name_df = vertical_df[vertical_df["Fields"].str.lower().isin(["name_x", "customer name", "name_y"])]
    device_df = vertical_df[vertical_df["Fields"].str.lower().isin([
        "master controller serial no.",
        "ecozen-master controller serial no.",
        "inverter serial no.",
        "device id"
    ])]

    # Get unique values for dropdowns
    all_names = sorted(name_df["Value"].dropna().unique())
    all_devices = sorted(device_df["Value"].dropna().unique())

    # Dropdown search filters
    search_name = st.sidebar.selectbox(
        "Search by Customer Name",
        options=[""] + all_names,
        format_func=lambda x: "Select..." if x == "" else x
    )

    search_device = st.sidebar.selectbox(
        "Search by Device ID / Serial No.",
        options=[""] + all_devices,
        format_func=lambda x: "Select..." if x == "" else x
    )

    # Initialize candidate tickets from all available ones
    candidate_ids = set(vertical_df["Ticket ID"].dropna())

    # Apply name filter
    if search_name:
        name_mask = (
            vertical_df["Fields"].str.lower().isin(["name_x", "customer name", "name_y"])
            & vertical_df["Value"].str.contains(search_name, case=False, na=False)
        )
        candidate_ids &= set(vertical_df.loc[name_mask, "Ticket ID"])

    # Apply device filter (using multiple device fields)
    if search_device:
        device_fields = [
            "master controller serial no.",
            "ecozen-master controller serial no.",
            "inverter serial no.",
            "device id"
        ]
        device_mask = (
            vertical_df["Fields"].str.lower().isin(device_fields)
            & vertical_df["Value"].str.contains(search_device, case=False, na=False)
        )
        candidate_ids &= set(vertical_df.loc[device_mask, "Ticket ID"])

    candidate_ids = sorted(candidate_ids)

    if not candidate_ids:
        st.sidebar.info("No tickets match the given search criteria.")
        st.stop()

    ticket_id = st.sidebar.selectbox("Select Ticket ID", candidate_ids)

    # ── Ticket‑specific slice ──
    df_ticket = vertical_df.loc[vertical_df["Ticket ID"] == ticket_id].copy()    

    # ---------- Latest comment ----------
    comments_df = load_comments()
    latest_comment_text = ""
    if not comments_df.empty:
        comments_df["Timestamp"] = pd.to_datetime(comments_df["Timestamp"], utc=True).dt.tz_convert(
            "Asia/Kolkata"
        )
        latest = (
            comments_df.loc[comments_df["Topic"] == ticket_id]
            .sort_values("Timestamp", ascending=False)
            .head(1)
        )
        if not latest.empty:
            latest_comment_text = latest.iloc[0]["Comment"]
            df_ticket = pd.concat(
                [
                    df_ticket,
                    pd.DataFrame(
                        [
                            {
                                "Ticket ID": ticket_id,
                                "Issue_Date": "",
                                "Fields": "Latest Comment",
                                "Value": latest_comment_text,
                            }
                        ]
                    ),
                ],
                ignore_index=True,
            )

    # ---------- Separate Vertical Details for Sheet 1 and Sheet 2 ----------    
    # Step 1: Create helper columns
    df_ticket["Suffix"] = df_ticket["Fields"].str.extract(r"(_x|_y)$")[0]
    df_ticket["Normalized_Field"] = df_ticket["Fields"].apply(strip_suffix)
    
    # Step 2: Prepare field sets
    sheet1_fields_normalized = set(f.strip() for f in sheet1_fields)
    sheet2_fields_normalized = set(f.strip() for f in sheet2_fields)
    sheet3_fields_normalized = set(f.strip() for f in sheet3_fields)
    
    # Step 3: Filter rows based on suffix explicitly
    df_sheet1 = df_ticket[
        (df_ticket["Normalized_Field"].isin(sheet1_fields_normalized)) &
        (
            (df_ticket["Suffix"] == "_x") |
            (df_ticket["Suffix"].isna())
        )
    ].copy()

    
    df_sheet2 = df_ticket[
        (df_ticket["Normalized_Field"].isin(sheet2_fields_normalized)) &
        (
            (df_ticket["Suffix"] == "_y") |
            (df_ticket["Suffix"].isna())
        )
    ].copy()
    
    # Sheet 3 has no suffix constraint
    df_sheet3 = df_ticket[
        (df_ticket["Normalized_Field"].isin(sheet3_fields_normalized))
    ].copy()
    
    # Step 4: Clean the `Fields` column
    df_sheet1["Fields"] = df_sheet1["Normalized_Field"]
    df_sheet2["Fields"] = df_sheet2["Normalized_Field"]
    df_sheet3["Fields"] = df_sheet3["Normalized_Field"]
    
    # ---------- Display Outputs ----------
    # Format each sheet with build_display_df and store them
    df_parts_display1 = build_parts_display_df(df_sheet1, ticket_id)
    df_display1 = build_display_df(df_sheet1, ticket_id)
    df_display2 = build_display_df(df_sheet2, ticket_id)
    df_display3 = build_display_df(df_sheet3, ticket_id)
    
    # ---------- Combined Summary View ----------
    
    # Use exact field names from the df_ticket["Fields"] column
    created_at = pd.to_datetime(get_value("Created At_y"))
    completion_date = pd.to_datetime(get_value("Service Completion Date"))
    service_start_date = pd.to_datetime(get_value("Service Start Date"))
    site_address = get_value("Site Address")
    
    # Assuming all rows have the same Issue_Date and it's consistent across the group
    date_of_issue = pd.to_datetime(df_ticket["Issue_Date"].iloc[0]) if "Issue_Date" in df_ticket.columns else None

    status = get_value("Status")  # Update if your df_ticket["Fields"] contains 'Status'
    due_days = (
        (completion_date.date() - created_at.date()).days
        if pd.notna(completion_date) and pd.notna(created_at)
        else None
    )
    
    # ---------- Ticket Details ----------
    ticket_details = {
        "Date of Issue": date_of_issue.date() if pd.notna(date_of_issue) else None,
        "Created At": created_at.date() if pd.notna(created_at) else None,
        "Device ID": get_field_value(df_display1, "Master Controller Serial No.") or get_field_value(df_display2, "Ecozen-Master Controller Serial No."),
        "Customer ID": get_field_value(df_display3, "Customer ID"),
        "Customer Name": get_field_value(df_display3, "Customer Name"),
        "Service Start Date": service_start_date.date() if pd.notna(service_start_date) else None,
        "Service Completion Date": completion_date.date() if pd.notna(completion_date) else None,
        "Status": status,
        "Due Days": due_days,
    }
    
    st.markdown(f"### 📋 Ticket Summary `{ticket_id}` - {ticket_details['Customer Name']}")
    render_summary_table_multi_column(ticket_details, columns=3)

    
    # ---------- Issue Details (Customer Perspective) ----------
    # st.markdown("---")
    issue_details = {
        "Problem Description (Customer)": get_field_value(df_display2, "Problem Description"),
        "Remark (Customer helpline)": get_field_value(df_display2, "Remark"),
        "Issue Resolution Plan": get_field_value(df_display1, "Issue Resolutions Plan"),
    }
    st.markdown("### 📝 Issue Details (Customer helpline)")
    render_summary_table_multi_column(issue_details, columns=2)
    
    # ---------- Service Details (Technician Input & Parts Info) ----------
    # st.markdown("---")
    service_details = {
        "Problem Description (Technician)": get_field_value(df_display1, "Problem Description"),
        "Remarks (Technician)": get_field_value(df_display1, "Remarks"),
        "Solution": get_field_value(df_display1, "Solution"),
    }
    st.markdown("### 🔧 Service Details")
    render_summary_table_multi_column(service_details, columns=2)
    
    # ---------- Comments Section (Compact) ----------
    st.markdown("### 💬 Technician Comment")
    
    # Load existing technician comments
    tech_comments_df = load_tech_comments()
    
    # Filter for current ticket
    ticket_comments = tech_comments_df.loc[tech_comments_df["Topic"] == ticket_id].sort_values("Timestamp", ascending=False)
    
    # # Show latest comment in a single line box
    # if not ticket_comments.empty:
    #     latest_comment = ticket_comments.iloc[0]["Comment"]
    #     st.markdown(f"**Latest Comment:** {latest_comment}")
    # else:
    #     st.info("No previous comments for this ticket.")
    
    # Add new comment
    with st.expander("➕ Add a new comment", expanded=False):
        new_comment = st.text_area("Comment", height=70, placeholder="Write technician comment here...")
        if st.button("Submit Comment", key="submit_tech_comment"):
            if new_comment.strip():
                add_tech_comment(ticket_id, new_comment)
                st.success("Comment added!")
                st.rerun()
            else:
                st.warning("Please enter a comment before submitting.")
    
    # Optional: Show full comment history in expandable
    with st.expander("📜 Full Comment History", expanded=False):
        if ticket_comments.empty:
            st.write("No comments yet.")
        else:
            for _, row in ticket_comments.iterrows():
                st.markdown(f"- `{row['Timestamp']}` — {row['Comment']}")

    
    # ---------- Display Tabular Fields (Parts Info) ----------
    # st.markdown("---")
    st.markdown("### 🔧 Parts Details")
    
    cols_to_show = [
        "Part Type", "Part Description", "New Part", "Old/Replaced Part"
    ]
    
    if not df_parts_display1.empty and "Part Type" in df_parts_display1.columns:
        # Safe to access columns
        cols_to_show = [
            "Part Type", "Part Description", "New Part", "Old/Replaced Part"
        ]
        
        part_cols = st.columns([2, 5, 5, 5])
        for i, col_name in enumerate(cols_to_show):
            part_cols[i].markdown(f"**{col_name}**")
    
        for idx in range(len(df_parts_display1)):
            part_cols = st.columns([2, 5, 5, 5])
            for i, col_name in enumerate(cols_to_show):
                val = df_parts_display1.iloc[idx].get(col_name, "—")
                part_cols[i].markdown(str(val) if pd.notna(val) else "—")
    else:
        st.markdown("_No parts data available._")

#   ---------------------Customer helpline-------------------------------
    fields_to_exclude = [
        "Created At",
        "Problem Description",
        "Remark",
        "Ecozen-Master Controller Serial No.",
        "Mob No.",
        "Name",
        "Site Address",
        "Issue Resolutions Plan"
    ]
    fields_to_remove = [
        "Ticket ID",
        "Issue Date"
        ]
    df_display2_cleaned = df_display2.drop(columns=[col for col in fields_to_remove if col in df_display2.columns])
    df_display2_filtered = df_display2_cleaned[~df_display2_cleaned["Fields"].isin(fields_to_exclude)]
    st.markdown("### 📄 Ticket Details — Solar AC: Customer_Helpline")
    st.dataframe(df_display2_filtered.reset_index(drop=True), use_container_width=True)

#   ------------------------Service------------------------------------------
    fields_to_exclude = [
        "Created At",
        "Problem Description",
        "Remarks",
        "Solution",
        "Name",
        "Master Controller Serial No.",
        "Part Type", 
        "Part Description", 
        "New Part", 
        "Old/Replaced Part",
        "Service Start Date",
        "Service Completion Date",
        "Status"
    ]
    fields_to_remove = [
        "Ticket ID",
        "Issue Date"
        ]
    df_display1_cleaned = df_display1.drop(columns=[col for col in fields_to_remove if col in df_display1.columns])
    df_display1_filtered = df_display1_cleaned[~df_display1_cleaned["Fields"].isin(fields_to_exclude)]
    st.markdown("### 📄 Ticket Details — Solar AC: Service")
    st.dataframe(df_display1_filtered.reset_index(drop=True), use_container_width=True)

    
    st.markdown("Solar AC: Order History")
    fields_to_remove = [
        "Ticket ID",
        "Issue Date"
        ]
    df_display3_cleaned = df_display3.drop(columns=[col for col in fields_to_remove if col in df_display3.columns])
    st.dataframe(df_display3_cleaned.reset_index(drop=True), use_container_width=True)

    # ---------- Previous comments ----------
    # st.subheader("📝 Previous Comments")
    # ticket_comments = comments_df.loc[comments_df["Topic"] == ticket_id]

    # if ticket_comments.empty:
    #     st.info("No comments yet for this ticket.")
    # else:
    #     ticket_comments = ticket_comments.sort_values("Timestamp", ascending=False)
    #     st.dataframe(ticket_comments, use_container_width=True)
    
    st.subheader("📝 Previous Comments")
    ticket_comments = comments_df.loc[comments_df["Topic"] == ticket_id]
    
    if ticket_comments.empty:
        st.info("No comments yet for this ticket.")
    else:
        ticket_comments = ticket_comments.sort_values("Timestamp", ascending=False)
    
        # Table header
        col1, col2, col3 = st.columns([1, 2, 6])
        col1.markdown("**Topic**")
        col2.markdown("**Timestamp**")
        col3.markdown("**Comment**")
    
        for _, row in ticket_comments.iterrows():
            col1, col2, col3 = st.columns([1, 2, 6])
            col1.markdown(row["Topic"])
            col2.markdown(row["Timestamp"])
            col3.markdown(linkify(row["Comment"]), unsafe_allow_html=True)


    # ---------- Add new comment ----------
    new_comment = st.text_area("Add a new comment:")
    if st.button("Submit Comment"):
        if new_comment.strip():
            add_comment(ticket_id, new_comment)
            st.success("Comment added!")
            st.rerun()
        else:
            st.warning("Please enter a comment before submitting.")
else:
    st.info("Click “🔄 Refresh & Process Data” to begin.")
