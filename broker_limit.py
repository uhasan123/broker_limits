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

    def run_broker_limit_model(self, cohort_size, threshold, p2_over_o2_thresh):
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

        ## incorporate less recent payment logic
        pay_df=self.generate_payment_data(cohort_size, threshold, p2_over_o2_thresh)

        broker_limit_df_2=broker_limit_df.merge(pay_df[['debtor_id', 'O2', 'avg_weekly_paid_amount_p2', 'avg_weekly_paid_amount_p1', 'p2', 'p1']], left_on='id', right_on='debtor_id', how='left')
        broker_limit_df_2=broker_limit_df_2.rename(columns={'debtor_id': 'no_limit_increase_id'})
        broker_limit_df_2['no_limit_increase_id']=broker_limit_df_2['no_limit_increase_id'].astype(str)
        broker_limit_df_2['is_limit_increase']=broker_limit_df_2['adjusted_broker_limit_2']>broker_limit_df_2['debtor_limit']
        broker_limit_df_2['exclude_from_limit_change']=broker_limit_df_2.apply(lambda x: 1 if ((x['is_limit_increase']==True) & (x['no_limit_increase_id']!='nan')) else 0, axis=1)
        broker_limit_df_2['final_limit_change']=broker_limit_df_2.apply(lambda x: 1 if ((x['change']==False) & (x['exclude_from_limit_change']==0)) else 0, axis=1)

        broker_limits_before_filter=broker_limit_df_2[broker_limit_df_2['change']==False][['id', 'adjusted_broker_limit_2']]
        broker_limits_before_filter=broker_limits_before_filter.rename(columns={'adjusted_broker_limit_2':'broker_limit'})        
        
        broker_limits_after_filter=broker_limit_df_2[broker_limit_df_2['final_limit_change']==1][['id', 'adjusted_broker_limit_2']]
        broker_limits_after_filter=broker_limits_after_filter.rename(columns={'adjusted_broker_limit_2':'broker_limit'})

        return broker_limit_df_2, broker_limits_before_filter, broker_limits_after_filter

    def broker_limit_audit(self, destination_path):
        broker_new_limits=pd.read_excel(destination_path)
        broker_limits_changed=broker_new_limits[broker_new_limits['final_limit_change']==1]
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

    def generate_payment_data(self, cohort_size, threshold, p2_over_o2_thresh):
        # query=f''' select * from
        # (select i.*, d.debtor_limit, i.open_invoice_volume/d.debtor_limit as util_rate, i.open_invoice_volume-d.debtor_limit as unnaturality
        # from
        # (select debtor_id,
        # sum(case when paid_date <=current_date and paid_date >= current_date-interval '{cohort_size} weeks' then approved_accounts_receivable_amount/100 else 0 end) as paid_0_{cohort_size},
        # sum(case when paid_date <current_date-interval '{cohort_size} weeks' and paid_date >= current_date-interval '{cohort_size*2} weeks' then approved_accounts_receivable_amount/100 else 0 end) as paid_{cohort_size}_{cohort_size*2},
        # sum(case when approved_date is not null and paid_date is null then approved_accounts_receivable_amount/100 else 0 end) as open_invoice_volume
        # from invoices
        # group by debtor_id) i 
        # left join
        # (select id, debtor_limit/100 as debtor_limit from debtors where status='active') d 
        # on i.debtor_id=d.id
        # where d.debtor_limit is not null) a'''
        cohort_size_2=(cohort_size*2)-1
        cohort_size_3=cohort_size*2

        p1=f'{cohort_size_3}'
        p2=f'{cohort_size}'
        
        base_dir = os.path.dirname(os.path.abspath(__file__))
        sql_path = os.path.join(base_dir, "weekly_snapshot_level_metrics_query.sql")
        with open(sql_path, 'r') as file:
            query=file.read()
        query=query.format(cohort_size_2=cohort_size_2)
        df=pd.read_sql_query(query, self.conn)
        df['snapshot_date']=pd.to_datetime(df['snapshot_date']).dt.date
        df=df.sort_values(['id', 'snapshot_date'])

        results = {
            'last_4': (
                df.groupby(['id', 'name', 'dot', 'current_approved_total', 'debtor_limit'])
                  .tail(4)
                  .groupby(['id', 'name', 'dot', 'current_approved_total', 'debtor_limit'])['open_invoice_amount']
                  .mean()
                  .rename('O2')
            )}
        results_2 = (
            df.groupby('id')
              .apply(lambda x: x.iloc[-int(p2):]['invoices_paid_dollars'].mean())
              .reset_index(name='avg_weekly_paid_amount_p2')
        )
        results_3 = (
            df.groupby('id')
              .apply(lambda x: x.iloc[-int(p1):-int(p2)]['invoices_paid_dollars'].mean())
              .reset_index(name='avg_weekly_paid_amount_p1')
        )
        results_4 = (
            df.groupby('id')
              .apply(lambda x: x.iloc[-int(p2):]['invoices_approved_dollars'].mean())
              .reset_index(name='avg_weekly_approved_amount_p2')
        )
        results_5 = (
            df.groupby('id')
              .apply(lambda x: x.iloc[-int(p1):-int(p2)]['invoices_approved_dollars'].mean())
              .reset_index(name='avg_weekly_approved_amount_p1')
        )
        
        
        final_df = pd.concat(results.values(), axis=1).reset_index()
        # final_df
        a1=final_df.merge(results_2, on='id', how='outer')
        a2=a1.merge(results_3, on='id', how='outer')
        a3=a2.merge(results_4, on='id', how='outer')
        a4=a3.merge(results_5, on='id', how='outer')
        a4['paid_p2_perc_of_open_invoice_l4']=a4['avg_weekly_paid_amount_p2']/a4['O2']
        a4['p2']=a4['avg_weekly_paid_amount_p2']*cohort_size
        a4['p1']=a4['avg_weekly_paid_amount_p1']*cohort_size
        
        a4['payment_decline_perc']=(a4['p1']-a4['p2'])/a4['p1']
        a4=a4[(a4['payment_decline_perc']>=threshold) | ((a4['p2']==0) & (a4['p1']==0)) | (a4['current_approved_total']==0) | ((a4['p2']!=0) & (a4['p1']==0) & (a4['paid_p2_perc_of_open_invoice_l4']<=p2_over_o2_thresh))]
        a4=a4.rename(columns={'id': 'debtor_id'})
        return a4








        
