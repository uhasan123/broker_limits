import pandas as pd
import os
import math

class broker_limit:
    def __init__(self, conn):
        self.conn=conn

    @staticmethod
    def assign_broker_limit(corr_flag, unnatural_flag, curr_approved_total, adj_broker_limit):
        if corr_flag==1:
            return 10000
        elif unnatural_flag==True:
            return math.ceil(curr_approved_total/1000) * 1000
        elif unnatural_flag==False:
            return math.ceil(adj_broker_limit/1000) * 1000
        else:
            return None

    def run_broker_limit_model(self):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        sql_path = os.path.join(base_dir, "broker_limit_query.sql")
        with open(sql_path, 'r') as file:
            query=file.read()

        broker_limit_df=pd.read_sql_query(query, self.conn)
        broker_limit_df['debtor_limit']=broker_limit_df['debtor_limit']/100
        broker_limit_df['unnatural_flag']=broker_limit_df['current_approved_total']>broker_limit_df['adjusted_broker_limit']
        broker_limit_df['adjusted_broker_limit_2']=broker_limit_df.apply(lambda x: broker_limit.assign_broker_limit(x['correction_flag'], x['unnatural_flag'], 
                                                                                              x['current_approved_total'], x['adjusted_broker_limit']), axis=1)

        broker_limit_df['change']=broker_limit_df['debtor_limit']==broker_limit_df['adjusted_broker_limit_2']

        broker_limits=broker_limit_df[broker_limit_df['change']==False][['id', 'adjusted_broker_limit_2']]
        broker_limits=broker_limits.rename(columns={'adjusted_broker_limit_2':'broker_limit'})

        return broker_limit_df, broker_limits

    def broker_limit_audit(self, destination_path):
        broker_new_limits=pd.read_excel(destination_path)
        broker_limits_changed=broker_new_limits[broker_new_limits['change']==False]
        brokers=tuple(broker_limits_changed['id'])

        query = "select id, debtor_limit from debtors d where id in %s"
        cur = self.conn.cursor()
        cur.execute(query, (brokers,))
        results=cur.fetchall()
        col_name=[i[0] for i in cur.description]
        debtors_df=pd.DataFrame(results, columns=col_name)
        debtors_df['debtor_limit']=debtors_df['debtor_limit']/100

        debtors_df_2=debtors_df.merge(broker_new_limits[['id', 'adjusted_broker_limit_2']], on='id', how='left')

        debtors_df_2['limit_audit']=debtors_df_2['debtor_limit']==debtors_df_2['adjusted_broker_limit_2']
        return debtors_df_2, debtors_df_2['limit_audit'].value_counts().reset_index()








        