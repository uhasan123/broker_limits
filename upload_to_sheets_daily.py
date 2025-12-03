import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
# from dotenv import load_dotenv
import sys
import os
# import schedule
# import time
import tempfile
import numpy as np

# sys.path.append('C://Users//Lenovo//Documents//Work//github_code//broker_limits')
from broker_report import broker_report

def connect_to_gsheet(creds_json,spreadsheet_name,sheet_name):
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    
    credentials = ServiceAccountCredentials.from_json_keyfile_name(creds_json, scope)
    client = gspread.authorize(credentials)
    spreadsheet = client.open(spreadsheet_name)  # Access the first sheet
    return spreadsheet.worksheet(sheet_name)

def calc_open_invoice_volume():
    obj=broker_report()
    conn=obj.make_db_connection()
    conn.autocommit=True
    with open('calc_open_invoice_volume.sql', 'r') as file:
        query=file.read()

    open_invoice_df=pd.read_sql_query(query, conn)
    open_invoice_df=open_invoice_df[['id', 'snapshot_date', 'approved_amount']]
    # conn.close()
    # tunnel.stop()
    return open_invoice_df

def calc_debtor_limit():
    obj=broker_report()
    conn=obj.make_db_connection()
    conn.autocommit=True
    with open('calc_debtor_limit.sql', 'r') as file:
        query=file.read()
    
    debtor_limit_df=pd.read_sql_query(query, conn)
    debtor_limit_df = debtor_limit_df.drop_duplicates(subset=['original_id', 'snapshot_date'], keep='first')
    debtor_limit_df['debtor_limit']=debtor_limit_df['debtor_limit']/100
    debtor_limit_df=debtor_limit_df[['original_id', 'snapshot_date', 'debtor_limit']]
    # conn.close()
    # tunnel.stop()
    return debtor_limit_df

def calc_broker_limit_breach():
    obj=broker_report()
    conn=obj.make_db_connection()
    conn.autocommit=True
    with open('broker_limit_breach_query.sql', 'r') as file:
        query=file.read()

    broker_limit_breach_df=pd.read_sql_query(query, conn)
    broker_limit_breach_df['created_at'] = pd.to_datetime(broker_limit_breach_df['created_at'], errors='coerce')
    broker_limit_breach_df['created_date']=broker_limit_breach_df['created_at'].dt.date
    # conn.close()
    # tunnel.stop()
    return broker_limit_breach_df

def sum_until_zero(g):
    zero_idx = g.index[g['is_exhausted'] == 0]
    if len(zero_idx) == 0:
        # No zero → sum all
        return g['is_exhausted'].sum()
    else:
        stop = zero_idx[0]
        return g.loc[g.index < stop, 'is_exhausted'].sum()

def create_debtor_level_view(debtor_limit):
    open_invoice_df=calc_open_invoice_volume()
    debtor_limit_df=calc_debtor_limit()
    broker_limit_breach_df=calc_broker_limit_breach()

    ageing_df=open_invoice_df.merge(debtor_limit_df, left_on=['id', 'snapshot_date'], right_on=['original_id', 'snapshot_date'], how='inner')
    ageing_df['is_exhausted']=ageing_df.apply(lambda x: 1 if x['approved_amount']>=x['debtor_limit']else 0, axis=1)
    ageing_df=ageing_df.sort_values(['id', 'snapshot_date'], ascending=[True, False]).reset_index()

    debtor_ageing = ageing_df.groupby('id').apply(sum_until_zero).reset_index(name='ageing')
    debtor_ageing['ageing']=debtor_ageing['ageing'].apply(lambda x: x+1)

    # debtor_limit=get_exhausted_debtors() # take from sheets

    debtor_level_view=debtor_ageing.merge(debtor_limit, on='id', how='outer')
    debtor_level_view['utilization_rate']=debtor_level_view['approved_total'] / debtor_level_view['debtor_limit']
    debtor_level_view['unnaturality']=debtor_level_view['approved_total']-debtor_level_view['debtor_limit']

    grouped=broker_limit_breach_df.groupby('debtor_id')
    debtor_level_view_2=grouped.apply(lambda g: pd.Series({
        'invoice_created_l30': g.loc[
            g['created_date'].isna()==False,
            'id'
        ].nunique(),
        'invoice_flagged_l30': g.loc[
            (g['created_date'].isna()==False) & (g['limit_exceeded']==1),
            'id'
        ].nunique()
    })).reset_index()
    
    debtor_level_view_2['perc_invoices_flagged_l30']=debtor_level_view_2['invoice_flagged_l30'] / debtor_level_view_2['invoice_created_l30']
    debtor_level=debtor_level_view.merge(debtor_level_view_2, left_on='id', right_on='debtor_id', how='outer')

    # debtor_level['ageing_cohort']=debtor_level['ageing'].apply(lambda x: ageing_cohort(x))
    # ageing_cohort_df=debtor_level.groupby('ageing_cohort').agg(broker_count=('id', 'nunique')).reset_index()

    return debtor_level

def generate_segment_level_data():
    obj=broker_report()
    conn=obj.make_db_connection()
    conn.autocommit=True
    with open('generate_segment_level_data_day.sql', 'r') as file:
        query=file.read()
    generate_segment_level_data_day=pd.read_sql_query(query, conn)
    with open('generate_segment_level_data_week.sql', 'r') as file:
        query1=file.read()
    generate_segment_level_data_week=pd.read_sql_query(query1, conn)
    with open('generate_segment_level_data_month.sql', 'r') as file:
        query2=file.read()
    generate_segment_level_data_month=pd.read_sql_query(query2, conn)
    with open('generate_segment_level_data_current.sql', 'r') as file:
        query3=file.read()
    generate_segment_level_data_current=pd.read_sql_query(query3, conn)
    return generate_segment_level_data_day, generate_segment_level_data_week, generate_segment_level_data_month, generate_segment_level_data_current


# def job():
private_key_json=os.getenv('private_key_json')

with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as tmp:
    tmp.write(private_key_json.encode())  # write bytes
    tmp.flush()
    creds_path = tmp.name
    
SPREADSHEET_NAME = 'Raw Data'
# SHEET_NAME = 'Sheet1'
CREDENTIALS_FILE = creds_path # './crendentials.json'

# load_dotenv("config_ssh.env")
obj=broker_report()
conn=obj.make_db_connection()
conn.autocommit=True

sheet_by_name = connect_to_gsheet(CREDENTIALS_FILE, SPREADSHEET_NAME, sheet_name='exhausted_debtors')
x=sheet_by_name.get_all_records()
exhaust_debtors=pd.DataFrame(x)

sheet_by_name = connect_to_gsheet(CREDENTIALS_FILE, SPREADSHEET_NAME, sheet_name='debtor_level')
debtor_level=create_debtor_level_view(exhaust_debtors)
debtor_level = debtor_level.replace([np.inf, -np.inf], np.nan)
debtor_level = debtor_level.fillna('')
data_to_upload = [debtor_level.columns.values.tolist()] + debtor_level.values.tolist()
# data_to_upload
sheet_by_name.clear()
sheet_by_name.append_rows(data_to_upload)

generate_segment_level_data_day, generate_segment_level_data_week, generate_segment_level_data_month, generate_segment_level_data_current=generate_segment_level_data()
sheet_by_name = connect_to_gsheet(CREDENTIALS_FILE, SPREADSHEET_NAME, sheet_name='segment_level_data_daily')
# debtor_level=create_debtor_level_view(exhaust_debtors)
generate_segment_level_data_day = generate_segment_level_data_day.replace([np.inf, -np.inf], np.nan)
generate_segment_level_data_day = generate_segment_level_data_day.fillna('')
data_to_upload = [generate_segment_level_data_day.columns.values.tolist()] + generate_segment_level_data_day.values.tolist()
# data_to_upload
sheet_by_name.clear()
sheet_by_name.append_rows(data_to_upload)

sheet_by_name = connect_to_gsheet(CREDENTIALS_FILE, SPREADSHEET_NAME, sheet_name='segment_level_data_weekly')
# debtor_level=create_debtor_level_view(exhaust_debtors)
generate_segment_level_data_week = generate_segment_level_data_week.replace([np.inf, -np.inf], np.nan)
generate_segment_level_data_week = generate_segment_level_data_week.fillna('')
data_to_upload = [generate_segment_level_data_week.columns.values.tolist()] + generate_segment_level_data_week.values.tolist()
# data_to_upload
sheet_by_name.clear()
sheet_by_name.append_rows(data_to_upload)

sheet_by_name = connect_to_gsheet(CREDENTIALS_FILE, SPREADSHEET_NAME, sheet_name='segment_level_data_monthly')
# debtor_level=create_debtor_level_view(exhaust_debtors)
generate_segment_level_data_month = generate_segment_level_data_month.replace([np.inf, -np.inf], np.nan)
generate_segment_level_data_month = generate_segment_level_data_month.fillna('')
data_to_upload = [generate_segment_level_data_month.columns.values.tolist()] + generate_segment_level_data_month.values.tolist()
# data_to_upload
sheet_by_name.clear()
sheet_by_name.append_rows(data_to_upload)

sheet_by_name = connect_to_gsheet(CREDENTIALS_FILE, SPREADSHEET_NAME, sheet_name='segment_level_data_week_start_to_date')
# debtor_level=create_debtor_level_view(exhaust_debtors)
generate_segment_level_data_current = generate_segment_level_data_current.replace([np.inf, -np.inf], np.nan)
generate_segment_level_data_current = generate_segment_level_data_current.fillna('')
data_to_upload = [generate_segment_level_data_current.columns.values.tolist()] + generate_segment_level_data_current.values.tolist()
# data_to_upload
sheet_by_name.clear()
sheet_by_name.append_rows(data_to_upload)

print('uploaded')
