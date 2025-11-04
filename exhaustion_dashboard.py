import os
import pandas as pd
import streamlit as st

from broker_report import broker_report

def get_exhausted_debtors():
    obj=broker_report()
    conn=obj.make_db_connection()
    query="select count(distinct id) as exhasuted_brokers from debtors d where d.approved_total>=d.debtor_limit and d.status = 'active' and d.debtor_limit<>100"
    exhaustion_count=pd.read_sql_query(query, conn)
    exhaustion_count=exhaustion_count['exhasuted_brokers'].iloc[0]
    return exhaustion_count

tab1, tab2, tab3=st.tabs(['TAB 1', 'TAB 2', 'TAB 3'])
with tab1:
    brokers_exhausted=get_exhausted_debtors()
    st.write('Exhaustion counter', brokers_exhausted)
        