

with months as
(
SELECT generate_series(
    date_trunc('month', current_date - interval '12 months'),  -- start: first day of month 12 months ago
    date_trunc('month', current_date),                        -- end: first day of current month
    interval '1 month'                                        -- step: 1 month
)::date AS snapshot_date;

),
avg_debtor_aeging as (
select debtor_id, avg(avg_ageing) as avg_ageing
from
(select *, extract(days from current_date-approved_date) as avg_ageing from invoices where paid_date is null and approved_date is not null) ag
group by debtor_id
)
,generate_segment_level_data as (
select
m.snapshot_date,
d.id,
d."name",
d.rating,
d.approved_total/100.0 as current_approved_total,
d.debtor_limit,
b.mc,
b.dot,
extract(days from (current_date-d.created_at)) as longevity_in_days,
--sum(i.approved_accounts_receivable_amount/100.0) as approved_amount
count(distinct case when (i.approved_date>snapshot_date-30 and i.approved_date<=snapshot_date) and i.approved_date is not null then i.id else null end) as invoice_approved,
sum(case when (i.approved_date>snapshot_date-30 and i.approved_date<=snapshot_date) and i.approved_date is not null then i.approved_accounts_receivable_amount/100.0 else 0 end) as invoice_approved_dollars,
count(distinct case when (i.paid_date>snapshot_date-30 and i.paid_date<=snapshot_date) and i.paid_date is not null then i.id else null end) as invoice_paid,
sum(case when (i.paid_date>snapshot_date-30 and i.paid_date<=snapshot_date) and i.paid_date is not null then i.approved_accounts_receivable_amount/100.0 else 0 end) as invoice_paid_dollars,
avg(case when (i.paid_date>snapshot_date-30 and i.paid_date<=snapshot_date) and i.paid_date is not null then i.dtp else null end) as dtp,
count(distinct case when (i.approved_date>snapshot_date-30 and i.approved_date<=snapshot_date) and i.paid_date is null then i.id else null end) as open_invoices_till_date,
sum(case when i.approved_date<snapshot_date and (i.paid_date>=snapshot_date or i.paid_date is null) then i.approved_accounts_receivable_amount/100 else 0 end) as open_invoices_in_point,
count(distinct case when (i.approved_date>snapshot_date-30 and i.approved_date<=snapshot_date) and i.approved_date is not null then i.client_id else null end) as no_of_clients
from months m
cross join debtors d
left join (select *, extract(days from paid_date-approved_date) as dtp from invoices) i
on d.id = i.debtor_id
left join brokers b
on d.id = b.debtor_id
where 1=1
--and (i.approved_date at time zone 'US/Eastern' is not null and i.approved_date at time zone 'US/Eastern' < m.snapshot_date)
--and (i.paid_date at time zone 'US/Eastern' is null or i.paid_date at time zone 'US/Eastern' >= m.snapshot_date)
and (i.approved_date >= current_date-730 or i.paid_date >= current_date-730)
and d.status = 'active'
--and d."name" = 'TOTAL QUALITY LOGISTICS'
group by
d.id,
d."name",
b.mc,
b.dot,
d.rating,
m.snapshot_date,
d.approved_total,
d.debtor_limit
)
select a.*, b.avg_ageing 
from
(select * from generate_segment_level_data) a 
left join
(select * from avg_debtor_aeging) b
on a.id=b.debtor_id
where invoice_approved<>0 or invoice_paid<>0
