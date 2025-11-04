import os
import pandas as pd
import streamlit as st

from broker_report import broker_report

def get_exhausted_debtors():
    obj=broker_report()
    conn=obj.make_db_connection()
    query="select id, debtor_limit/100 as debtor_limit, approved_total/100 as approved_total from debtors d where d.approved_total>=d.debtor_limit and d.status = 'active' and d.debtor_limit<>100"
    exhaust_debtors=pd.read_sql_query(query, conn)
    return exhaust_debtors

def calc_open_invoice_volume(conn):
    with open('calc_open_invoice_volume.sql', 'r') as file:
        query=file.read()

    open_invoice_df=pd.read_sql_query(query, conn)
    open_invoice_df=open_invoice_df[['id', 'snapshot_date', 'approved_amount']]
    return open_invoice_df

def calc_debtor_limit(conn):
    with open('calc_debtor_limit.sql', 'r') as file:
        query=file.read()
    
    debtor_limit_df=pd.read_sql_query(query, conn)
    debtor_limit_df = debtor_limit_df.drop_duplicates(subset=['original_id', 'snapshot_date'], keep='first')
    debtor_limit_df['debtor_limit']=debtor_limit_df['debtor_limit']/100
    debtor_limit_df=debtor_limit_df[['original_id', 'snapshot_date', 'debtor_limit']]
    return debtor_limit_df

def calc_broker_limit_breach(conn):
    with open('broker_limit_breach_query.sql', 'r') as file:
        query=file.read()

    broker_limit_breach_df=pd.read_sql_query(query, conn)
    broker_limit_breach_df['created_date']=broker_limit_breach_df['created_at'].dt.date
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
    open_invoice_df=calc_open_invoice_volume(conn)
    debtor_limit_df=calc_debtor_limit(conn)
    broker_limit_breach_df=calc_broker_limit_breach(conn)
    
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


tab1, tab2, tab3=st.tabs(['TAB 1', 'TAB 2', 'TAB 3'])
with tab1:
    exhaust_debtors=get_exhausted_debtors()
    debtor_level, ageing_cohort_df,limit_cohort_df=create_debtor_level_view()
    debtor_level=debtor_level[['id', 'debtor_limit', 'approved_total', 'utilization_rate', 'invoice_created_l30', 'invoice_breached_l30', 'perc_invoices_breached_l30']]
    brokers_exhausted=exhaust_debtors['id'].nunique()
    st.write('Exhaustion counter', brokers_exhausted)
    colss=st.columns(2)
    colss[0].write(ageing_cohort_df)
    colss[1].write(limit_cohort_df)
    st.write(debtor_level)


