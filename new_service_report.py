# -*- coding: utf-8 -*-
"""
Created on Fri Jul  4 15:19:18 2025

@author: Admin
"""
import json
import base64
import gspread
import streamlit as st
import pandas as pd
from datetime import datetime
import gspread
from google.oauth2 import service_account

# ───────────────────────── CONFIG ─────────────────────────
SERVICE_ACCOUNT_FILE = r"C:\Users\Admin\Desktop\solar-ac-customer-mapping-905e295dd0db.json"

SHEET_ID_1 = "1px_3F6UEjE3hD6UQMoCGThI7X1o9AK4ERfqwfOKlfY4"
SHEET_ID_2 = "1z1dyhCXHLN3pSZBhRdpmJ3XwvwV9zF7_QJ0qsZRSLzU"
SHEET_ID_3 = "11CBVvoJjfgvAaFsS-3I_sqQxql8n53JfSZA8CGT9mvA"

COMMENTS_SHEET_ID   = "1vqk13WA77LuSl0xzb54ESO6GSUfqiM9dUgdLfnWdaj0"
COMMENTS_SHEET_NAME = "solarac_Comments_log"

SELECTED_COLUMNS_1 = [
    "Ticket ID", "Master Controller Serial No.", "Inverter Serial No.",
    "Status", "Created At", "Problem", "Problem Description", "Mob No.",
    "Issue Resolutions Plan", "Site Address", "Solution", "Remarks",
    "Part Type", "Part Description", "Part  SAP-ID",
    "Service Completion Date", "Service Completion Time"
]
SELECTED_COLUMNS_2 = [
    "Ticket ID", "Inverter Serial No.", "Remark",
    "Problem Description", "Date of Issue"
]
SELECTED_COLUMNS_3 = [
    "Phone Number", "Customer ID", "Customer Name",
    "Part ID", "Part ID Description"
]

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

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

# ───────────────────────── HELPERS ─────────────────────────
def read_selected_columns(sheet_id: str, selected_cols: list[str]) -> pd.DataFrame:
    """Read a Google Sheet and return only the requested columns (handles dups)."""
    try:
        data = gc.open_by_key(sheet_id).sheet1.get_all_values()
        headers = data[0]
        # make duplicate headers unique
        seen, unique_headers = {}, []
        for h in headers:
            cnt = seen.get(h, 0)
            unique_headers.append(f"{h}_{cnt}" if cnt else h)
            seen[h] = cnt + 1
        df = pd.DataFrame(data[1:], columns=unique_headers)
        existing = [c for c in selected_cols if c in df.columns]
        return df[existing]
    except Exception as e:                       # noqa: BLE001
        st.error(f"Error reading sheet {sheet_id}: {e}")
        return pd.DataFrame(columns=selected_cols)

def process_sheets_and_transform():
    """
    Reads data from three Google Sheets, merges and transforms the data,
    and returns a vertical DataFrame ready for further use.
    """
    # Step 1: Read all 3 sheets
    df1 = read_selected_columns(SHEET_ID_1, SELECTED_COLUMNS_1)
    df2 = read_selected_columns(SHEET_ID_2, SELECTED_COLUMNS_2)
    df3 = read_selected_columns(SHEET_ID_3, SELECTED_COLUMNS_3)

    # Step 2: Rename columns in Sheet 2 to avoid conflicts
    df2_renamed = df2.rename(columns={
        'Problem Description': 'Problem_Description_CH',
        'Remark': 'Remark_CH'
    })

    # Step 3: Merge Sheet 1 and 2 on 'Ticket ID'
    df_merged = pd.merge(df1, df2_renamed, on='Ticket ID', how='outer')

    # Step 4: Normalize phone numbers
    if 'Mob No.' in df_merged.columns:
        df_merged['Mob No.'] = df_merged['Mob No.'].str.replace(r'\D', '', regex=True).str[-10:]
    if 'Phone Number' in df3.columns:
        df3['Phone Number'] = df3['Phone Number'].str.replace(r'\D', '', regex=True).str[-10:]

    # Step 5: Merge Sheet 3 on phone number
    df_final = pd.merge(df_merged, df3, left_on='Mob No.', right_on='Phone Number', how='left')

    # Step 6: Create consolidated 'Final Name'
    df_final["Name of Customer"] = df_final.get("Customer Name", pd.Series([pd.NA] * len(df_final)))
    df_final["Name of Customer"] = df_final["Name of Customer"].fillna("").replace("", pd.NA)
    df_final["Name of Customer"] = df_final["Name of Customer"].combine_first(df_final.get("Name", pd.Series([pd.NA] * len(df_final))))
    df_final["Name of Customer"] = df_final["Name of Customer"].combine_first(df_final.get("Name_2", pd.Series([pd.NA] * len(df_final))))

    # Step 7: Create consolidated 'Final Phone'
    df_final["Customer_contact"] = df_final.get("Phone Number", pd.Series([pd.NA] * len(df_final)))
    df_final["Customer_contact"] = df_final["Customer_contact"].fillna("").replace("", pd.NA)
    df_final["Customer_contact"] = df_final["Customer_contact"].combine_first(df_final.get("Mob No.", pd.Series([pd.NA] * len(df_final))))
    df_final["Customer_contact"] = df_final["Customer_contact"].combine_first(df_final.get("Phone_2", pd.Series([pd.NA] * len(df_final))))

    # Step 8: Drop old columns if they exist
    columns_to_drop = ["Customer Name", "Name", "Mob No.", "Phone Number"]
    columns_to_drop = [col for col in columns_to_drop if col in df_final.columns]
    df_final = df_final.drop(columns=columns_to_drop)

    # Step 9: Transform to vertical format
    all_rows = []
    for idx, row in df_final.iterrows():
        ticket_id = row.get('Ticket ID', None)
        issue_date = row.get('Date of Issue', None)

        for col in df_final.columns:
            if col not in ['Ticket ID', 'Issue_Date']:
                value = row[col]
                if not pd.isna(value) and str(value).strip() != "":
                    all_rows.append({
                        'Ticket ID': ticket_id,
                        'Issue_Date': issue_date,
                        'Fields': col,
                        'Value': value
                    })

    vertical_df = pd.DataFrame(all_rows)
    vertical_df = vertical_df.sort_values(['Ticket ID', 'Issue_Date', 'Fields'])

    return vertical_df

def load_comments() -> pd.DataFrame:
    """Return comments as df with columns Topic, Timestamp, Comment (UTC ts)."""
    try:
        raw = comment_ws.get_all_records()
        df = pd.DataFrame(raw)
        df.columns = df.columns.str.strip()
        # If sheet uses "Ticket ID" instead of "Topic", standardise to 'Topic'
        if "Ticket ID" in df.columns and "Topic" not in df.columns:
            df = df.rename(columns={"Ticket ID": "Topic"})
        required = ["Topic", "Timestamp", "Comment"]
        for col in required:
            if col not in df.columns:
                df[col] = None
        return df[required]
    except Exception as e:                       # noqa: BLE001
        st.error(f"Error reading comments: {e}")
        return pd.DataFrame(columns=["Topic", "Timestamp", "Comment"])

def add_comment(topic: str, comment_text: str) -> None:
    """Append a new comment to the Google Sheet."""
    try:
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        comment_ws.append_row([topic, ts, comment_text.strip()])
    except Exception as e:                       # noqa: BLE001
        st.error(f"Error adding comment: {e}")

# ───────────────────────── STREAMLIT UI ─────────────────────────
st.set_page_config(page_title="Solar AC Complaint Tracker", layout="wide")
st.title("📊 Complaint History")

# ---------- Refresh Button ----------
if st.button("🔄 Refresh & Process Data"):
    with st.spinner("Fetching data from Google Sheets…"):
        st.session_state.vertical_df = process_sheets_and_transform()
    st.success("✅ Data refreshed and processed!")

# ---------- Main Display ----------
if "vertical_df" in st.session_state:
    vertical_df = st.session_state.vertical_df.copy()
    
    # Extract values from the vertical DF
    def get_value(param_name):
        row = vertical_df[vertical_df['Fields'] == param_name]
        return row['Value'].values[0] if not row.empty else None
    
    # Extract and parse required fields
    date_of_issue = pd.to_datetime(get_value("Date of Issue"))
    created_at = pd.to_datetime(get_value("Created At"))
    completion_date = pd.to_datetime(get_value("Service Completion Date"))
    status = get_value("Status")
    
    # Compute Due days
    due_days = (completion_date.date() - created_at.date()).days if completion_date and date_of_issue else None
    
    # Construct summary DataFrame
    summary_df = pd.DataFrame([{
        "Date of Issue": date_of_issue,
        "createdAt": created_at.date(),
        "Service Completion date": completion_date.date(),
        "Status": status,
        "Due days": due_days
    }])
    
    # Display summary row
    st.markdown("### Ticket Summary")
    st.dataframe(summary_df, hide_index=True)
    
    ##############################################################################################################
    # Display full vertical DF below
    st.markdown("### Ticket Details")
    # sidebar filter
    st.sidebar.header("🔎 Filters")
    ticket_id = st.sidebar.selectbox(
        "Select Ticket ID",
        sorted(vertical_df["Ticket ID"].dropna().unique().tolist()),
    )

    df_ticket = vertical_df.loc[vertical_df["Ticket ID"] == ticket_id].copy()

    # ── bring in LATEST comment as an extra “field” row ──
    comments = load_comments()
    if not comments.empty:
        comments["Timestamp"] = pd.to_datetime(comments["Timestamp"], utc=True).dt.tz_convert(
            "Asia/Kolkata"
        )
        latest = (
            comments.loc[comments["Topic"] == ticket_id]
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

    # ---------- vertical table ----------
    display_rows = []
    for (tkt, issue_date), grp in df_ticket.groupby(["Ticket ID", "Issue_Date"]):
        first = True
        for _, row in grp.iterrows():
            display_rows.append(
                {
                    "Ticket ID": tkt if first else "",
                    "Issue Date": issue_date if first else "",
                    "Fields": row["Fields"],
                    "Value": row["Value"],
                }
            )
            first = False
    display_df = pd.DataFrame(display_rows)
    st.dataframe(display_df, use_container_width=True)

    # ---------- ALL previous comments below ----------
    st.subheader("📝 Previous Comments")
    ticket_comments = comments.loc[comments["Topic"] == ticket_id]
    if ticket_comments.empty:
        st.info("No comments yet for this ticket.")
    else:
        ticket_comments = ticket_comments.sort_values("Timestamp", ascending=False)
        st.dataframe(ticket_comments, use_container_width=True)

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
    st.info("Click “🔄 Refresh & Process Data” to begin.")
