import os
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import ast
from datetime import date
from pandas.api.types import CategoricalDtype
from psycopg2 import errors

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from broker_report import broker_report

st.set_page_config(
    page_title="Exhaustion Monitoring Dashboard",
    layout="wide",  # <--- This makes the page use the full width
    initial_sidebar_state="expanded"  # optional: sidebar expanded by default
)

st.title("Exhaustion Monitoring Dashboard")

google_creds=os.getenv('google_credentials')
temp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
temp.write(google_creds.encode())
temp.close()

credentials=temp.name

def connect_to_gsheet(creds_json,spreadsheet_name,sheet_name):
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    
    credentials = ServiceAccountCredentials.from_json_keyfile_name(creds_json, scope)
    client = gspread.authorize(credentials)
    spreadsheet = client.open(spreadsheet_name)  # Access the first sheet
    return spreadsheet.worksheet(sheet_name)
    
SPREADSHEET_NAME = 'Sample'
SHEET_NAME = 'Sheet1'
CREDENTIALS_FILE = credentials

sheet_by_name = connect_to_gsheet(CREDENTIALS_FILE, SPREADSHEET_NAME, sheet_name=SHEET_NAME)
x=sheet_by_name.get_all_records()
df=pd.DataFrame(x)
st.write(df)