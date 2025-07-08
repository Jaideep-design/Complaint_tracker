# -*- coding: utf-8 -*-
"""
Streamlit app: Solar AC Complaint Tracker
Added search filters for Customer Name and Device ID.
Created on Fri Jul  4 15:19:18 2025
@author: Admin
"""
import base64
import json
import re
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
    # Authenticate
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
    gc = gspread.authorize(creds)

    # Get worksheet data
    worksheet = gc.open_by_key(sheet_id).sheet1
    data = worksheet.get_all_values()

    # Extract header and data
    raw_header = data[0]
    rows = data[1:]

    # Rename duplicates (e.g., Serial Number, Serial Number_2, Serial Number_3, etc.)
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

    # Keep only selected columns (based on base name)
    def base_name(col):
        return col.split("_")[0] if "_" in col else col

    df = df[[col for col in df.columns if base_name(col) in selected_columns]]

    # Rename duplicates using custom map (if provided)
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
             
# ── Recent 5 Tickets Display ──
    st.subheader("🕔 Recent 5 Tickets")

    # Filter for rows where Created At_x or Created At_y are present
    created_at_df = vertical_df[
        vertical_df["Fields"].str.lower().isin(["created at_x", "created at_y"])
    ].copy()

    # Convert Value column (which holds datetime) to datetime dtype
    created_at_df["Created At"] = pd.to_datetime(created_at_df["Value"], errors="coerce")

    # Sort by latest date and remove duplicates by Ticket ID to get most recent entry per ticket
    recent_tickets = (
        created_at_df.dropna(subset=["Created At", "Ticket ID"])
        .sort_values("Created At", ascending=False)
        .drop_duplicates(subset=["Ticket ID"])
    )

    # Function to extract the first matching field value from a ticket slice
    def get_field(ticket_slice, field_names):
        for fname in field_names:
            match = ticket_slice[ticket_slice["Fields"].str.lower() == fname.lower()]
            if not match.empty:
                return match.iloc[0]["Value"]
        return None

    # Build summary list
    recent_display = []
    for _, row in recent_tickets.head(5).iterrows():
        ticket_id = row["Ticket ID"]
        created_at = row["Created At"]

        ticket_slice = vertical_df[vertical_df["Ticket ID"] == ticket_id]

        device_no = get_field(ticket_slice, [
            "Master Controller Serial No.", "Ecozen-Master Controller Serial No."])
        problem_desc = get_field(ticket_slice, [
            "Problem description_x", "Problem description_y"
        ])
        remark = get_field(ticket_slice, [
            "Remark", "Remarks"
        ])

        recent_display.append({
            "Ticket ID": ticket_id,
            "Created At": created_at,
            "Device Number": device_no,
            "Problem Description": problem_desc,
            "Remark": remark
        })

    # Display the final table
    st.dataframe(pd.DataFrame(recent_display))
