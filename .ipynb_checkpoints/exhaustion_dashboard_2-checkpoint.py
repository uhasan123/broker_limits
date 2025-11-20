import os
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import ast
from datetime import date
from pandas.api.types import CategoricalDtype
from psycopg2 import errors
import tempfile

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from broker_report import broker_report

st.set_page_config(
    page_title="Exhaustion Monitoring Dashboard",
    layout="wide",  # <--- This makes the page use the full width
    initial_sidebar_state="expanded"  # optional: sidebar expanded by default
)

st.title("Exhaustion Monitoring Dashboard")

typee=os.getenv('type')
project_id=os.getenv('project_id')
private_key_id=os.getenv('private_key_id')
private_key=os.getenv('private_key')
client_email=os.getenv('client_email')
client_id=os.getenv('client_id')
auth_uri=os.getenv('auth_uri')
token_uri=os.getenv('token_uri')
auth_provider_x509_cert_url=os.getenv('auth_provider_x509_cert_url')
client_x509_cert_url=os.getenv('client_x509_cert_url')
universe_domain=os.getenv('universe_domain')

google_credentials={'type':typee, 'project_id':project_id, 'private_key_id':private_key_id, 'private_key':private_key, 'client_email':client_email, 'client_id':client_id, 'auth_uri':auth_uri, 'token_uri':token_uri, 'auth_provider_x509_cert_url':auth_provider_x509_cert_url, 'client_x509_cert_url':client_x509_cert_url, 'universe_domain':universe_domain}

temp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
json.dump(google_credentials, temp)
credentials=temp.name
temp.close()

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