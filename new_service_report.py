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
SERVICE_ACCOUNT_FILE = r"C:\Users\Admin\Desktop\solar-ac-customer-mapping-905e295dd0db.json"

SHEET_ID_1 = "1px_3F6UEjE3hD6UQMoCGThI7X1o9AK4ERfqwfOKlfY4"
SHEET_ID_2 = "1z1dyhCXHLN3pSZBhRdpmJ3XwvwV9zF7_QJ0qsZRSLzU"
SHEET_ID_3 = "11CBVvoJjfgvAaFsS-3I_sqQxql8n53JfSZA8CGT9mvA"

COMMENTS_SHEET_ID = "1vqk13WA77LuSl0xzb54ESO6GSUfqiM9dUgdLfnWdaj0"
COMMENTS_SHEET_NAME = "solarac_Comments_log"

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
    'Voltage',
    '1P-Voltage',
    'Battery Voltage',
    'Battery Capacity in AH',
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

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

RENAME_MAP_1 = {
    "Serial Number": "Serial Number (Used)",
    "Serial Number_2": "Serial Number (Returned)",
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
# %%
# ───────────────────────── HELPERS ─────────────────────────

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

    # Step 4 – Merge df3 using phone numbers
    df_final = pd.merge(
        df_merged, df3, left_on="Mob No.", right_on="Phone Number", how="left"
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

    # ---------- Ticket summary ----------
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
    
    # Use exact field names from the df_ticket["Fields"] column
    created_at = pd.to_datetime(get_value("Created At_y"))
    completion_date = pd.to_datetime(get_value("Service Completion Date"))  # Update if this field exists in df_ticket
    # Assuming all rows have the same Issue_Date and it's consistent across the group
    date_of_issue = pd.to_datetime(df_ticket["Issue_Date"].iloc[0]) if "Issue_Date" in df_ticket.columns else None

    status = get_value("Status")  # Update if your df_ticket["Fields"] contains 'Status'
    due_days = (
        (completion_date.date() - created_at.date()).days
        if pd.notna(completion_date) and pd.notna(created_at)
        else None
    )
    
    summary_df = pd.DataFrame(
        [
            {
                "Date of Issue": date_of_issue.date() if pd.notna(date_of_issue) else None,
                "Created At": created_at.date() if pd.notna(created_at) else None,
                "Service Completion Date": completion_date.date()
                if pd.notna(completion_date)
                else None,
                "Status": status,
                "Due Days": due_days,
            }
        ]
    )
    
    st.markdown("### Ticket Summary")
    st.dataframe(summary_df, hide_index=True)

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

    sheet1_fields = [
        "Ticket ID", "Name", "Master Controller Serial No.", "Inverter Serial No.",
        "Status", "Created At", "Problem", "Problem Description", "Mob No.",
        "Issue Resolutions Plan", "Site Address", 'Serial Number (Used)', "Serial Number (Returned)", "Complaint Reg Date",
        "Resolution Method", "Component", "Solution", "Remarks", "Part Type",
        "Part Description", "Total Breakdown", "1. AC Serial Number", "No of Solar panel",
        "Voltage", "1P-Voltage", "Battery Voltage", "Battery Capacity in AH",
        "Service Completion Date", "Service Completion Time",
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
    
    def strip_suffix(field_name: str) -> str:
        """Strip _x or _y suffix from a field name."""
        return re.sub(r"(_x|_y)$", "", field_name).strip()
    
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
    
    # ---------- Helper to Build Display Table ----------
    def build_display_df(source_df: pd.DataFrame, ticket_id: str) -> pd.DataFrame:
        display_rows = []
        
        # Filter for the given ticket_id
        df = source_df[source_df['Ticket ID'] == ticket_id]
    
        # Group by Issue_Date (including NaN by filling with placeholder)
        for issue_dt, grp in df.groupby(df['Issue_Date'].fillna('No Issue Date')):
            first = True
            for _, r in grp.iterrows():
                display_rows.append({
                    "Ticket ID": ticket_id if first else "",
                    "Issue Date": issue_dt if first else "",
                    "Fields": r["Fields"],
                    "Value": r["Value"],
                })
                first = False
                
        return pd.DataFrame(display_rows)    
    # ---------- Display Outputs ----------
    st.markdown("### 📄 Ticket Details — Solar AC: Service")
    st.dataframe(build_display_df(df_sheet1, ticket_id), use_container_width=True)
    
    st.markdown("### 📄 Ticket Details — Solar AC: Customer Helpline")
    st.dataframe(build_display_df(df_sheet2, ticket_id), use_container_width=True)
    
    st.markdown("### 📄 Ticket Details — Solar AC: Order Book")
    st.dataframe(build_display_df(df_sheet3, ticket_id), use_container_width=True)

    # ---------- Previous comments ----------
    # st.subheader("📝 Previous Comments")
    # ticket_comments = comments_df.loc[comments_df["Topic"] == ticket_id]

    # if ticket_comments.empty:
    #     st.info("No comments yet for this ticket.")
    # else:
    #     ticket_comments = ticket_comments.sort_values("Timestamp", ascending=False)
    #     st.dataframe(ticket_comments, use_container_width=True)
    
    
    def linkify(comment: str) -> str:
        url_pattern = r'(https?://[^\s]+)'
        return re.sub(url_pattern, r'[\1](\1)', comment)
    
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
