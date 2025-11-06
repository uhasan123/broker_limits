import os
# from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder
import pandas as pd
import psycopg2
from datetime import date
from datetime import datetime
import statistics as stats
import plotly.graph_objects as go
import tempfile

class broker_report:
    def __init__(self):
        # load_dotenv(config_path)
        self.ssh_host = os.getenv("ssh_host")
        self.ssh_user = os.getenv("ssh_user")
        self.ssh_key_temp = os.getenv("ssh_key")
        self.db_host = os.getenv("db_host")
        self.db_port = int(os.getenv("db_port"))
        self.db_name = os.getenv("db_name")
        self.db_user = os.getenv("db_user")
        self.db_password = os.getenv("db_password")

        temp = tempfile.NamedTemporaryFile(delete=False, suffix=".pem")
        temp.write(self.ssh_key_temp.encode())
        temp.close()

        self.ssh_key=temp.name

    def make_db_connection(self):

        # conn = psycopg2.connect(
        #     host=self.db_host,
        #     port=self.db_port,
        #     database=self.db_name,
        #     user=self.db_user,
        #     password=self.db_password,
        #     sslmode="require"
        # )

        # Open SSH tunnel
        tunnel = SSHTunnelForwarder(
            (self.ssh_host, 22),
            ssh_username=self.ssh_user,
            ssh_pkey=self.ssh_key,
            remote_bind_address=(self.db_host, self.db_port)
        )
        
        tunnel.start()
        # Connect to DB through the tunnel
        conn = psycopg2.connect(
            host="127.0.0.1",
            port=tunnel.local_bind_port,
            database=self.db_name,
            user=self.db_user,
            password=self.db_password
        )
        return conn

    def invoice_table(self):
        conn=self.make_db_connection()
        query='select * from invoices limit 10'
        invoice_df=pd.read_sql_query(query, conn)
        conn.close()
        tunnel.stop()
        return invoice_df

    @staticmethod
    def generate_date_series(start_date=None, end_date=None, step='weekly'):
        mondays=[]
        if step=='weekly':
            days=7
        elif step=='monthly':
            days=30
        elif step=='daily':
            days=1
        elif step=='current': # weekly for now
            mondays_df=pd.DataFrame()
            mondays_df['snapshot_date']=[end_date]
            days=mondays_df['snapshot_date'].iloc[0].isoweekday() # exlude -1
            return mondays_df, days
        else:
            days=None
        while start_date<=end_date:
            mondays.append(start_date)
            start_date=start_date + pd.Timedelta(days=days)
        mondays_df=pd.DataFrame()
        mondays_df['snapshot_date']=mondays
        return mondays_df, days

    @staticmethod
    def generate_segment_level_data(start_date, end_date,debtors_df, brokers_df, invoice_df, step='weekly'): #start date default None
        mondays_df, days=broker_report.generate_date_series(start_date, end_date, step)
        xx=mondays_df.merge(debtors_df[['id', 'name', 'rating', 'approved_total','debtor_limit', 'created_at']], how='cross')
        yy=xx.merge(brokers_df[['debtor_id', 'mc', 'dot']], left_on='id', right_on='debtor_id', how='left')[['snapshot_date', 'id', 'name', 'rating', 'approved_total','debtor_limit', 'created_at', 'mc', 'dot']]

        yy['longevity_in_days'] = yy.apply(lambda x: (end_date - datetime.date(x['created_at'])).days, axis=1)
        yy=yy[['snapshot_date', 'id', 'name', 'rating', 'approved_total', 'debtor_limit', 'mc', 'dot', 'longevity_in_days']]

        invoice_df['dtp']=invoice_df['paid_date']-invoice_df['approved_date']
        invoice_df['dtp']=invoice_df['dtp'].apply(lambda x: x.days if pd.isna(x)==False else x)

        if 'id' in invoice_df.columns.to_list():
            invoice_df=invoice_df.rename(columns={'id': 'invoice_id'})

        if type(datetime.date(datetime.today()))!=type(invoice_df['approved_date'].iloc[0]):
            invoice_df['approved_date']=invoice_df['approved_date'].apply(lambda x: datetime.date(x) if pd.isna(x)==False else x)
        if type(datetime.date(datetime.today()))!=type(invoice_df['paid_date'].iloc[0]):
            invoice_df['paid_date']=invoice_df['paid_date'].apply(lambda x: datetime.date(x) if pd.isna(x)==False else x)
        if type(datetime.date(datetime.today()))!=type(invoice_df['created_at'].iloc[0]):
            invoice_df['created_at']=invoice_df['created_at'].apply(lambda x: datetime.date(x) if pd.isna(x)==False else x)

        grouping_cols=yy.columns.to_list()
        if len(brokers_df)==0:
            grouping_cols.remove('mc')
            grouping_cols.remove('dot')
        final=yy.merge(invoice_df, left_on='id', right_on='debtor_id', how='left').groupby(grouping_cols)

        broker_level=final.apply(lambda g: pd.Series({
            'invoice_approved': g.loc[
            (g['approved_date'].between(
            g['snapshot_date']-pd.Timedelta(days=days),g['snapshot_date'], inclusive='right')
            )
            & (g['approved_date'].isna()==False),
        'invoice_id'].nunique(),
        
            'invoice_approved_dollars': g.loc[
            (g['approved_date'].between(
            g['snapshot_date']-pd.Timedelta(days=days),g['snapshot_date'], inclusive='right')
            )
            & (g['approved_date'].isna()==False),
            'approved_accounts_receivable_amount'
        ].sum(),
        
            'invoice_paid': g.loc[
            (g['paid_date'].between(
            g['snapshot_date']-pd.Timedelta(days=days),g['snapshot_date'], inclusive='right')
            )
            & (g['paid_date'].isna()==False),
        'invoice_id'].nunique(),
        
            'invoice_paid_dollars': g.loc[
            (g['paid_date'].between(
            g['snapshot_date']-pd.Timedelta(days=days),g['snapshot_date'], inclusive='right')
            )
            & (g['paid_date'].isna()==False),
            'approved_accounts_receivable_amount'
        ].sum(),
        
            'dtp': g.loc[
            (g['paid_date'].between(
            g['snapshot_date']-pd.Timedelta(days=days),g['snapshot_date'], inclusive='right')
            )
            & (g['paid_date'].isna()==False),
            'dtp'
        ].mean(),
        
            'open_invoices_till_date': g.loc[
            (g['approved_date'].between(
            g['snapshot_date']-pd.Timedelta(days=days),g['snapshot_date'], inclusive='right')
            )
            & (g['paid_date'].isna()==True),
            'invoice_id'
        ].nunique(),
        
            'open_invoices_in_point': g.loc[
            (g['approved_date']<g['snapshot_date'])
            & ((g['paid_date']>=g['snapshot_date']) | (g['paid_date'].isna()==True)),
            'approved_accounts_receivable_amount'
        ].sum(),
        
            'no_of_clients': g.loc[
            (g['approved_date'].between(
            g['snapshot_date']-pd.Timedelta(days=days),g['snapshot_date'], inclusive='right')
            )
            & (g['approved_date'].isna()==False),
            'client_id'
        ].nunique()
                    })
                   ).reset_index()

        not_paid_df=invoice_df[(invoice_df['paid_date'].isna()) & (invoice_df['approved_date'].isna()==False)]
        not_paid_df['avg_ageing'] = not_paid_df.apply(lambda x: (end_date - x['approved_date']).days, axis=1)
        broker_level['avg_ageing']=not_paid_df['avg_ageing'].mean()

        return broker_level

    @staticmethod
    def payment_trend(broker_level_df, count, step='default', debtors_df=None, brokers_df=None, invoice_df=None):
        days_diff=(broker_level_df['snapshot_date'].iloc[1] - broker_level_df['snapshot_date'].iloc[0]).days
        if step=='default':
            df_t=broker_level_df[['snapshot_date','invoice_approved', 'invoice_approved_dollars','open_invoices_in_point', 'invoice_paid', 'invoice_paid_dollars']][-count:].set_index('snapshot_date').T
        elif ((days_diff!=7) & (step=='weekly')):
            weekly_start_date=date.today()-pd.Timedelta(weeks=count)
            weekly_end_date=date.today()
            df_t1=broker_report.generate_segment_level_data(weekly_start_date, weekly_end_date,debtors_df, brokers_df, invoice_df, step=step)
            df_t2=broker_report.generate_segment_level_data(start_date=None, end_date=date.today(), debtors_df=debtors_df, brokers_df=brokers_df, invoice_df=invoice_df, step='current')
            df_t=pd.concat([df_t1, df_t2], ignore_index=True)
            # add "current" segment level data as well
            df_t=df_t[['snapshot_date','invoice_approved', 'invoice_approved_dollars','open_invoices_in_point', 'invoice_paid', 'invoice_paid_dollars']].set_index('snapshot_date').T
            
        elif ((days_diff!=1) & (step=='daily')):
            daily_start_date=date.today()-pd.Timedelta(days=count)
            daily_end_date=date.today()
            df_t=broker_report.generate_segment_level_data(daily_start_date, daily_end_date,debtors_df, brokers_df, invoice_df, step=step)
            df_t=df_t[['snapshot_date','invoice_approved', 'invoice_approved_dollars','open_invoices_in_point', 'invoice_paid', 'invoice_paid_dollars']].set_index('snapshot_date').T
        else:
            None

        # broker_report.payment_trend_graph(df_t.T.reset_index())
        return df_t

    @staticmethod
    def client_conc(broker_level_df, invoice_df, cohort):
        cols=['client count (absolute)', 'top 3 client share', '25% share client count', '50% share client count', '75% share client count', 'invoice amount per client']
        days_diff=(broker_level_df['snapshot_date'].iloc[1] - broker_level_df['snapshot_date'].iloc[0]).days
        days_diff_dict={'7': 'weeks', '30':'months', '1': 'days'}
        d=days_diff_dict[str(days_diff)]
        df_list=[]

        years_in_business=(date.today()-invoice_df.sort_values('created_at', ascending=True).iloc[0]['created_at'].date()).days / 365
        # print(years_in_business)
        total_approved_invoices_dollars=invoice_df['approved_accounts_receivable_amount'].sum()
        # print(total_approved_invoices_dollars)
        total_approved_invoices=invoice_df['id'].nunique()
        # print(total_approved_invoices)

        debtor_info=pd.DataFrame()
        debtor_info['debtor_name']=[invoice_df['debtor_selected_name'].iloc[0]]
        debtor_info['years_in_business']=[years_in_business]
        debtor_info['total_approved_invoices_dollars']=[total_approved_invoices_dollars]
        debtor_info['total_approved_invoices']=[total_approved_invoices]
        # print(debtor_info)
        
        for i in cohort:
            if i!='all':
                d1=invoice_df.iloc[-1]['approved_date']
                d2=invoice_df.iloc[-1]['approved_date']-pd.Timedelta(days=i*days_diff) # putting approved date filter here because client kpis
                invoice_df_new=invoice_df[invoice_df['approved_date'].between(d2,d1)]
            else:
                invoice_df_new=invoice_df

            # rows=broker_level_df_new.shape[0]
            
            debtor_client_level=invoice_df_new.groupby(['debtor_id','client_id']).agg(client_approved_invoices=('approved_accounts_receivable_amount', 'sum')).reset_index().sort_values('client_approved_invoices', ascending=False)
            
            total_approved_invoices_dollars=invoice_df_new['approved_accounts_receivable_amount'].sum()
            debtor_client_level['client_invoices_cumsum']=debtor_client_level['client_approved_invoices'].cumsum()
            debtor_client_level['share']=(debtor_client_level['client_invoices_cumsum']/total_approved_invoices_dollars) *100
            debtor_client_level['row_number']=range(1, len(debtor_client_level)+1)
            # debtor_client_level = debtor_client_level.reset_index().rename(columns={'index': 'row_number'})
            # debtor_client_level['row_number'] += 1
            
            debtor_client_dict={}
            debtor_client_dict[f'client_count_l{i}_{d}']=debtor_client_level.shape[0]
            debtor_client_dict[f'top_3_client_share_l{i}_{d}']=debtor_client_level[debtor_client_level['row_number']==3]['share'].iloc[0]
            debtor_client_dict[f'25_perc_share_clients_l{i}_{d}']=debtor_client_level[debtor_client_level['share']>=25]['row_number'].min()
            debtor_client_dict[f'50_perc_share_clients_l{i}_{d}']=debtor_client_level[debtor_client_level['share']>=50]['row_number'].min()
            debtor_client_dict[f'75_perc_share_clients_l{i}_{d}']=debtor_client_level[debtor_client_level['share']>=75]['row_number'].min()
            debtor_client_dict[f'invoice_amount_per_client_l{i}_{d}']=debtor_client_level['client_approved_invoices'].sum() / debtor_client_level.shape[0]

            values=[debtor_client_dict[f'client_count_l{i}_{d}'], debtor_client_dict[f'top_3_client_share_l{i}_{d}'], debtor_client_dict[f'25_perc_share_clients_l{i}_{d}'], debtor_client_dict[f'50_perc_share_clients_l{i}_{d}'], debtor_client_dict[f'75_perc_share_clients_l{i}_{d}'], debtor_client_dict[f'invoice_amount_per_client_l{i}_{d}']]
            df=pd.DataFrame()
            df['metrics']=cols
            df['client_metric']=values
            df['cohort']= f'last {i} {d}'
            df_list.append(df)
        df_=pd.concat(df_list)

        return debtor_info, df_

        
    @staticmethod
    def generate_report(broker_level_df, broker_profile_report=False, cohort=[52, 12],payment_trend_count=5, payment_trend_step='default', debtors_df=None, brokers_df=None, invoice_df=None):
        cols=['open_invoices_in_point', 'invoice_approved', 'invoice_paid', 'invoice_approved_dollars', 'invoice_paid_dollars', 'dtp']
        days_diff=(broker_level_df['snapshot_date'].iloc[1] - broker_level_df['snapshot_date'].iloc[0]).days
        days_diff_dict={'7': 'weeks', '30':'months', '1': 'days'}
        d=days_diff_dict[str(days_diff)]
        df_list=[]
        for i in cohort:
            if i!='all':
                d1=broker_level_df.iloc[-1]['snapshot_date']
                d2=broker_level_df.iloc[-1]['snapshot_date']-pd.Timedelta(days=i*days_diff)
                broker_level_df_new=broker_level_df[broker_level_df['snapshot_date'].between(d2,d1)]
            else:
                broker_level_df_new=broker_level_df
                
            rows=broker_level_df_new.shape[0]
            metric='mean'
            dictt={}
            dictt[f"open_invoice_{metric}_l{rows}_{d}"]=broker_level_df_new['open_invoices_in_point'].mean()
            dictt[f"approved_invoice_{metric}_l{rows}_{d}"]=broker_level_df_new['invoice_approved'].mean()
            dictt[f"paid_invoice_{metric}_l{rows}_{d}"]=broker_level_df_new['invoice_paid'].mean()
            dictt[f"approved_invoice_dollars_{metric}_l{rows}_{d}"]=broker_level_df_new['invoice_approved_dollars'].mean()
            dictt[f"paid_invoice_dollars_{metric}_l{rows}_{d}"]=broker_level_df_new['invoice_paid_dollars'].mean()
            if len(broker_level_df_new[broker_level_df_new['dtp'].isna()==False])==0:
                dictt[f"dtp_{metric}_l{rows}_{d}"]='NA'
            else:
                dictt[f"dtp_{metric}_l{rows}_{d}"]=broker_level_df_new[broker_level_df_new['dtp'].isna()==False]['dtp'].mean()
        
            values_mean=[dictt[f"open_invoice_{metric}_l{rows}_{d}"], dictt[f"approved_invoice_{metric}_l{rows}_{d}"], dictt[f"paid_invoice_{metric}_l{rows}_{d}"], dictt[f"approved_invoice_dollars_{metric}_l{rows}_{d}"], dictt[f"paid_invoice_dollars_{metric}_l{rows}_{d}"], dictt[f"dtp_{metric}_l{rows}_{d}"]]
        
            metric='stdev'
            try:
                dictt[f"open_invoice_{metric}_l{rows}_{d}"]=stats.stdev(broker_level_df_new['open_invoices_in_point'])
            except stats.StatisticsError as e:
                dictt[f"open_invoice_{metric}_l{rows}_{d}"]='NA'
            try:
                dictt[f"approved_invoice_{metric}_l{rows}_{d}"]=stats.stdev(broker_level_df_new['invoice_approved'])
            except stats.StatisticsError as e:
                dictt[f"approved_invoice_{metric}_l{rows}_{d}"]='NA'
            try:
                dictt[f"paid_invoice_{metric}_l{rows}_{d}"]=stats.stdev(broker_level_df_new['invoice_paid'])
            except stats.StatisticsError as e:
                dictt[f"paid_invoice_{metric}_l{rows}_{d}"]='NA'
            try:
                dictt[f"approved_invoice_dollars_{metric}_l{rows}_{d}"]=stats.stdev(broker_level_df_new['invoice_approved_dollars'])
            except stats.StatisticsError as e:
                dictt[f"approved_invoice_dollars_{metric}_l{rows}_{d}"]='NA'
            try:
                dictt[f"paid_invoice_dollars_{metric}_l{rows}_{d}"]=stats.stdev(broker_level_df_new['invoice_paid_dollars'])
            except stats.StatisticsError as e:
                dictt[f"paid_invoice_dollars_{metric}_l{rows}_{d}"]='NA'
            try:    
                dictt[f"dtp_{metric}_l{rows}_{d}"]=stats.stdev(broker_level_df_new[broker_level_df_new['dtp'].isna()==False]['dtp'])
            except stats.StatisticsError as e:
                dictt[f"dtp_{metric}_l{rows}_{d}"]='NA'
        
            values_stdev=[dictt[f"open_invoice_{metric}_l{rows}_{d}"], dictt[f"approved_invoice_{metric}_l{rows}_{d}"], dictt[f"paid_invoice_{metric}_l{rows}_{d}"], dictt[f"approved_invoice_dollars_{metric}_l{rows}_{d}"], dictt[f"paid_invoice_dollars_{metric}_l{rows}_{d}"], dictt[f"dtp_{metric}_l{rows}_{d}"]]

            # if broker_profile_report==False:
            df=pd.DataFrame()
            df['metrics']=cols
            df['mean']=values_mean
            df['std_dev']=values_stdev
            df['cohort']= f'last {rows} {d}'
            df_list.append(df)
        df_=pd.concat(df_list)

        if broker_profile_report==False:
            df_=df_[df_['metrics']!=cols[-1]]
            df_t=broker_report.payment_trend(broker_level_df, count=payment_trend_count, step=payment_trend_step, debtors_df=debtors_df, brokers_df=brokers_df, invoice_df=invoice_df) # df_t here is paymengt trend
            pivot_table_client_conc=None
        else:
            df_t, df_y=broker_report.client_conc(broker_level_df, invoice_df, cohort) #df_t here is debtor_info
            # print(type(df_y))
            # print(df_t.head())
            pivot_table_client_conc = df_y.pivot_table(index='metrics', columns='cohort', values='client_metric')
            pivot_table_client_conc=pivot_table_client_conc.sort_index(axis=1, level=[1, 0])

        # print(df_)
        pivot_table = df_.pivot_table(index='metrics', columns='cohort', values=['mean', 'std_dev'])
        pivot_table=pivot_table.sort_index(axis=1, level=[1, 0])
        
        if broker_profile_report==False:
            pivot_table=pd.concat([pivot_table, df_t], axis=1)
            
            # need to cater for current dates. Should report take into account curretn dates


            # else:
            #     df=pd.DataFrame()
            #     df['metrics']=cols
            #     df['mean']=values_mean
            #     df['std_dev']=values_stdev
            #     df['cohort']= f'last {rows} {d}'
            #     df_list.append(df)

            #     df_=pd.concat(df_list)

            #     pivot_table = df_.pivot_table(index='metrics', columns='cohort', values=['mean', 'std_dev'])
            #     pivot_table=pivot_table.sort_index(axis=1, level=[1, 0])
            
        return pivot_table, df_t, pivot_table_client_conc

    @staticmethod
    def payment_trend_graph(df):
        fig = go.Figure([
        go.Scatter(x=df['snapshot_date'], y=df['invoice_approved'], mode='lines+markers', name='Approved invoices', yaxis='y2'),
        go.Scatter(x=df['snapshot_date'], y=df['invoice_paid'], mode='lines+markers', name='Paid Invoices', yaxis='y2'),
        go.Scatter(x=df['snapshot_date'], y=df['open_invoices_in_point'], mode='lines+markers', name='Open Invoices', yaxis='y1'),
        # go.Scatter(x=df['snapshot_date'], y=df['invoice_approved_dollars'], mode='lines+markers', name='Invoices Approved (dollars)', yaxis='y1'),
        # go.Scatter(x=df['snapshot_date'], y=df['invoice_paid_dollars'], mode='lines+markers', name='Invoices Paid (dollars)', yaxis='y1')
    ])
    
        fig.update_layout(
            title="Broker Weekly Invoice Trend",
            xaxis_title="Date",
            yaxis_title="Open invoices amount",
            template="plotly_white",
            yaxis2=dict(
            title='Invoice count amount',
            overlaying='y',     # overlay on the same plotting area
            side='right'        # place on right
            ),
            legend=dict(x=1.1, y=1.1),
            height=500
        )
        return fig
        
        # fig.show()
                    
            
            
            
            
            
            
                    
            
            
            
                    
