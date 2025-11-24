import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
# from dotenv import load_dotenv
import sys
import os
import schedule
import time

# sys.path.append('C://Users//Lenovo//Documents//Work//github_code//broker_limits')
from broker_report import broker_report

def connect_to_gsheet(creds_json,spreadsheet_name,sheet_name):
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    
    credentials = ServiceAccountCredentials.from_json_keyfile_name(creds_json, scope)
    client = gspread.authorize(credentials)
    spreadsheet = client.open(spreadsheet_name)  # Access the first sheet
    return spreadsheet.worksheet(sheet_name)

def get_exhausted_debtors():
    query='''select distinct a.*, b.dot from 
(select id, name, debtor_limit/100 as debtor_limit, approved_total/100 as approved_total from debtors d where d.approved_total>=d.debtor_limit and d.status = 'active' and d.debtor_limit<>100) a
left join
(select debtor_id, dot from brokers) b 
on a.id=b.debtor_id'''
    exhaust_debtors=pd.read_sql_query(query, conn)
    return exhaust_debtors

# def job():
SPREADSHEET_NAME = 'Sample'
SHEET_NAME = 'Sheet1'
CREDENTIALS_FILE = './credentials.json'

sheet_by_name = connect_to_gsheet(CREDENTIALS_FILE, SPREADSHEET_NAME, sheet_name=SHEET_NAME)

# load_dotenv("config_ssh.env")
obj=broker_report()
conn=obj.make_db_connection()
conn.autocommit=True

exhausted_df=get_exhausted_debtors()
data_to_upload = [exhausted_df.columns.values.tolist()] + exhausted_df.values.tolist()
# data_to_upload
sheet_by_name.clear()
sheet_by_name.append_rows(data_to_upload)
print('uploaded')
    
# schedule.every().hour.do(job)

# while True:
#     schedule.run_pending()
#     time.sleep(60)