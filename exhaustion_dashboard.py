import os
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import ast
from datetime import date

from broker_report import broker_report

def get_exhausted_debtors():
    obj=broker_report()
    conn=obj.make_db_connection()
    conn.autocommit=True
    query="select id, debtor_limit/100 as debtor_limit, approved_total/100 as approved_total from debtors d where d.approved_total>=d.debtor_limit and d.status = 'active' and d.debtor_limit<>100"
    exhaust_debtors=pd.read_sql_query(query, conn)
    return exhaust_debtors

def calc_open_invoice_volume(conn):
    with open('calc_open_invoice_volume.sql', 'r') as file:
        query=file.read()

    open_invoice_df=pd.read_sql_query(query, conn)
    open_invoice_df=open_invoice_df[['id', 'snapshot_date', 'approved_amount']]
    # conn.close()
    # tunnel.stop()
    return open_invoice_df
def calc_open_invoice_volume_l90(debtor_id, conn):
    with open('calc_open_invoice_volume_l90.sql', 'r') as file:
        query=file.read()
    query=query.format(debtor_id=debtor_id)

    open_invoice_df_l90=pd.read_sql_query(query, conn)
    open_invoice_df_l90=open_invoice_df_l90[['id', 'snapshot_date', 'approved_amount']]
    # conn.close()
    # tunnel.stop()
    return open_invoice_df_l90

def calc_debtor_limit(conn):
    with open('calc_debtor_limit.sql', 'r') as file:
        query=file.read()
    
    debtor_limit_df=pd.read_sql_query(query, conn)
    debtor_limit_df = debtor_limit_df.drop_duplicates(subset=['original_id', 'snapshot_date'], keep='first')
    debtor_limit_df['debtor_limit']=debtor_limit_df['debtor_limit']/100
    debtor_limit_df=debtor_limit_df[['original_id', 'snapshot_date', 'debtor_limit']]
    # conn.close()
    # tunnel.stop()
    return debtor_limit_df
def calc_debtor_limit_l90(debtor_id, conn):
    with open('calc_debtor_limit_l90.sql', 'r') as file:
        query=file.read()
    query=query.format(debtor_id=debtor_id)
    
    debtor_limit_df_l90=pd.read_sql_query(query, conn)
    debtor_limit_df_l90 = debtor_limit_df_l90.drop_duplicates(subset=['original_id', 'snapshot_date'], keep='first')
    debtor_limit_df_l90['debtor_limit']=debtor_limit_df_l90['debtor_limit']/100
    debtor_limit_df_l90=debtor_limit_df_l90[['original_id', 'snapshot_date', 'debtor_limit']]
    # conn.close()
    # tunnel.stop()
    return debtor_limit_df_l90

def calc_broker_limit_breach(conn):
    with open('broker_limit_breach_query.sql', 'r') as file:
        query=file.read()

    broker_limit_breach_df=pd.read_sql_query(query, conn)
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
def ageing_cohort(x):
    if x<=7:
        return 'less than 7 days'
    elif (x>7) & (x<=20):
        return '8 to 20 days'
    elif x>20:
        return 'greater than 21 days'
    else:
        return None
def limit_cohort(x):
    if x==10000:
        return '10k'
    elif (x>10000) & (x<=20000):
        return '10k to 20k'
    elif (x>20000) & (x<=40000):
        return '20k to 40k'
    elif (x>40000) & (x<=60000):
        return '40k to 60k'
    elif (x>60000) & (x<=80000):
        return '60k to 80k'
    elif (x>80000) & (x<=100000):
        return '80k to 100k'
    elif x>100000:
        return 'greater than 100k'
    else:
        return None

def create_debtor_level_view():
    obj=broker_report()
    conn=obj.make_db_connection()
    conn.autocommit=True
    open_invoice_df=calc_open_invoice_volume(conn)
    debtor_limit_df=calc_debtor_limit(conn)
    broker_limit_breach_df=calc_broker_limit_breach(conn)

    # conn.close()
    # tunnel.stop()
    
    ageing_df=open_invoice_df.merge(debtor_limit_df, left_on=['id', 'snapshot_date'], right_on=['original_id', 'snapshot_date'], how='inner')
    ageing_df['is_exhausted']=ageing_df.apply(lambda x: 1 if x['approved_amount']>=x['debtor_limit']else 0, axis=1)
    ageing_df=ageing_df.sort_values(['id', 'snapshot_date'], ascending=[True, False]).reset_index()

    debtor_ageing = ageing_df.groupby('id').apply(sum_until_zero).reset_index(name='ageing')
    debtor_ageing['ageing']=debtor_ageing['ageing'].apply(lambda x: x+1)

    debtor_limit=get_exhausted_debtors()

    debtor_level_view=debtor_ageing.merge(debtor_limit, on='id', how='outer')
    debtor_level_view['utilization_rate']=debtor_level_view['approved_total'] / debtor_level_view['debtor_limit']
    debtor_level_view['unnaturality']=debtor_level_view['approved_total']-debtor_level_view['debtor_limit']

    grouped=broker_limit_breach_df.groupby('debtor_id')
    debtor_level_view_2=grouped.apply(lambda g: pd.Series({
        'invoice_created_l30': g.loc[
            g['created_date'].isna()==False,
            'id'
        ].nunique(),
        'invoice_breached_l30': g.loc[
            (g['created_date'].isna()==False) & (g['limit_exceeded']==1),
            'id'
        ].nunique()
    })).reset_index()
    
    debtor_level_view_2['perc_invoices_breached_l30']=debtor_level_view_2['invoice_breached_l30'] / debtor_level_view_2['invoice_created_l30']
    debtor_level=debtor_level_view.merge(debtor_level_view_2, left_on='id', right_on='debtor_id', how='outer')
    debtor_level['ageing_cohort']=debtor_level['ageing'].apply(lambda x: ageing_cohort(x))
    ageing_cohort_df=debtor_level.groupby('ageing_cohort').agg(broker_count=('id', 'nunique')).reset_index()
    debtor_level['limit_cohort']=debtor_level['debtor_limit'].apply(lambda x: limit_cohort(x))
    limit_cohort_df=debtor_level.groupby('limit_cohort').agg(broker_count=('id', 'nunique')).reset_index()

    return debtor_level, ageing_cohort_df, limit_cohort_df

def generate_data_for_payment_trend(debtor_id):
    for attempt in range(3):
        try:
            query = "select * from invoices i where debtor_id=%s and i.approved_date is not null"
            cur = conn.cursor()
            cur.execute(query, (debtor_id,))
            results=cur.fetchall()
            break
        except errors.SerializationFailure:
            conn.rollback()
            time.sleep(1)
            continue
    col_name=[i[0] for i in cur.description]
    invoice_df=pd.DataFrame(results, columns=col_name)
    invoice_df['approved_accounts_receivable_amount']=invoice_df['approved_accounts_receivable_amount']/100
    
    query = "select * from debtors d where id=%s"
    cur = conn.cursor()
    cur.execute(query, (debtor_id,))
    results=cur.fetchall()
    col_name=[i[0] for i in cur.description]
    debtors_df=pd.DataFrame(results, columns=col_name)
    debtors_df['debtor_limit']=debtors_df['debtor_limit']/100
    debtors_df['approved_total']=debtors_df['approved_total']/100
    
    query = "select * from brokers b where debtor_id=%s"
    cur = conn.cursor()
    cur.execute(query, (debtor_id,))
    results=cur.fetchall()
    col_name=[i[0] for i in cur.description]
    brokers_df=pd.DataFrame(results, columns=col_name)
    return invoice_df, debtors_df, brokers_df

if 'tab1' not in st.session_state:
    st.session_state.tab1=False
if 'tab2' not in st.session_state:
    st.session_state.tab2=False
if 'tab3' not in st.session_state:
    st.session_state.tab3=False
    
tab1, tab2, tab3=st.tabs(['TAB 1', 'TAB 2', 'TAB 3'])
with tab1:
    if st.button("Refresh", key='refresh_tab1'):
        st.session_state.tab1=True
    if st.session_state.tab1==True:
        exhaust_debtors=get_exhausted_debtors()
        debtor_level, ageing_cohort_df,limit_cohort_df=create_debtor_level_view()
        debtor_level=debtor_level[['id', 'debtor_limit', 'approved_total', 'utilization_rate', 'invoice_created_l30', 'invoice_breached_l30', 'perc_invoices_breached_l30']]
        brokers_exhausted=exhaust_debtors['id'].nunique()
        st.write('Exhaustion counter', brokers_exhausted)
        colss=st.columns(2)
        colss[0].write(ageing_cohort_df)
        colss[1].write(limit_cohort_df)
        st.write(debtor_level.sort_values(by='utilization_rate', ascending=False).reset_index().drop('index', axis=1))
        st.session_state.tab1=False

with tab2:
    obj=broker_report()
    conn=obj.make_db_connection()
    conn.autocommit=True
    
    debtor_id=st.text_input("debtor id : ", key="debtor_id_t2")
    if st.button("Submit", key='submit_tab2'):
        st.session_state.tab2=True

    if st.session_state.tab2==True:
        debtor_limit=get_exhausted_debtors()
        if debtor_id !='':
            open_invoice_df_l90=calc_open_invoice_volume_l90(debtor_id, conn)
            debtor_limit_df_l90=calc_debtor_limit_l90(debtor_id, conn)
    
        # conn.close()
        # tunnel.stop()
        
            df_l90=open_invoice_df_l90.merge(debtor_limit_df_l90, left_on=['id', 'snapshot_date'], right_on=['original_id', 'snapshot_date'], how='outer')
            df_l90['approved_amount']=df_l90['approved_amount'].apply(lambda x: 0 if str(x)=='nan' else x)
            df_l90['limit_exceed']=df_l90['approved_amount']>=df_l90['debtor_limit']
            df_l90['limit_exceed_shift']=df_l90['limit_exceed'].shift(-1)
            df_l90['breach_count']=df_l90.apply(lambda x: 1 if (x['limit_exceed']==False) & (x['limit_exceed_shift']==True) else 0, axis=1)
        
            df_l90_=debtor_limit[debtor_limit['id']==debtor_id]
            df_l90_['utilization_rate']=df_l90_['approved_total'] / df_l90_['debtor_limit']
            df_l90_['unnaturality']=df_l90_['approved_total']-df_l90_['debtor_limit']
            df_l90_['debtor_limit_change']=df_l90_['debtor_limit']-df_l90[df_l90['snapshot_date']==df_l90['snapshot_date'].min()]['approved_amount'].iloc[0]
            df_l90_['no_of_exhaustions']=df_l90['breach_count'].sum()
            st.write(df_l90_)
        
            fig = go.Figure([
            go.Scatter(x=df_l90['snapshot_date'], y=df_l90['approved_amount'], mode='lines+markers', name='Approved amount', yaxis='y1'),
            go.Scatter(x=df_l90['snapshot_date'], y=df_l90['debtor_limit'], mode='lines+markers', name='Debtor Limit', yaxis='y1'),
            # go.Scatter(x=df['snapshot_date'], y=df['invoice_approved_dollars'], mode='lines+markers', name='Invoices Approved (dollars)', yaxis='y1'),
            # go.Scatter(x=df['snapshot_date'], y=df['invoice_paid_dollars'], mode='lines+markers', name='Invoices Paid (dollars)', yaxis='y1')
            ])
            
            fig.update_layout(
                title="Broker Approved invoices against debtor limit day wise",
                xaxis_title="Date",
                yaxis_title="Amount in dollars",
                template="plotly_white",
                legend=dict(x=1.1, y=1.1),
                height=500
            )
        
            st.plotly_chart(fig, use_container_width=True)
            st.session_state.tab2=False

with tab3:
    obj=broker_report()
    conn=obj.make_db_connection()
    conn.autocommit=True

    debtor_id=st.text_input("debtor id : ", key="debtor_id_t3")
    cohort=st.text_input("cohort: ", key="cohort")
    payment_trend_count=int(st.number_input("payment trend count: ", key='payment_trend_count'))
    payment_trend_step=st.text_input("payment trend step: ", key='payment_trend_step')
    step=st.text_input("step: ", key='step')
    generate_broker_report=st.checkbox("generate_broker_report")
    if cohort!='':
        cohort=ast.literal_eval(cohort)

    if st.button("Submit", key='submit_tab3'):
        st.session_state.tab3=True
        
    if st.session_state.tab3==True:
        if debtor_id !='':
            invoice_df, debtors_df, brokers_df=generate_data_for_payment_trend(debtor_id)
            if generate_broker_report:
                date_today=date.today()
                date_last_year=date_today - pd.Timedelta(days=365)
                # start_date=(date_last_year + pd.Timedelta(days=(8-date_last_year.isoweekday())%7) + pd.Timedelta(days=7))-pd.Timedelta(days=1) # for weekly
                start_date=invoice_df['approved_date'].min().date() # for monthly
                end_date=date_today
                
                broker_level_df=broker_report.generate_segment_level_data(start_date, end_date, debtors_df, brokers_df, invoice_df, step=step)
                pivot_table, df_t, pivot_table_client_conc=broker_report.generate_report(broker_level_df, broker_profile_report=generate_broker_report, cohort=cohort,payment_trend_count=payment_trend_count, payment_trend_step=payment_trend_step, debtors_df=debtors_df, brokers_df=brokers_df, invoice_df=invoice_df)
                st.write(df_t)
                st.write(pivot_table)
                st.write(pivot_table_client_conc)

            else:
                date_today=date.today()
                date_last_year=date_today - pd.Timedelta(days=365)
                start_date=(date_last_year + pd.Timedelta(days=(8-date_last_year.isoweekday())%7) + pd.Timedelta(days=7))-pd.Timedelta(days=1) # for weekly
                # start_date=invoice_df['approved_date'].min().date() # for monthly
                end_date=date_today
                
                broker_level=broker_report.generate_segment_level_data(start_date, end_date, debtors_df, brokers_df, invoice_df, step=step)
                broker_level_current=broker_report.generate_segment_level_data(start_date=None, end_date=end_date, debtors_df=debtors_df, brokers_df=brokers_df, invoice_df=invoice_df, step='current')
                # broker_level_df=broker_level[broker_level['dtp'].isna()==False]
                broker_level_df=pd.concat([broker_level, broker_level_current], ignore_index=True)
                # broker_level_df.head()
                pivot_table, df_t, pivot_table_client_conc=broker_report.generate_report(broker_level_df, broker_profile_report=generate_broker_report, cohort=cohort,payment_trend_count=payment_trend_count, payment_trend_step=payment_trend_step, debtors_df=debtors_df, brokers_df=brokers_df, invoice_df=invoice_df)
                fig=broker_report.payment_trend_graph(df_t.T.reset_index())
                st.write(pivot_table)
                st.plotly_chart(fig, use_container_width=True)

            st.session_state.tab3=False



