with cal_debtor_weekly_open_invoice_volume as --here
(
select
d.id,
-- d."name",
b.mc,
b.dot,
d.rating,
d.approved_total/100.0 as current_approved_total,
d.debtor_limit/100 as debtor_limit,
gs::date as snapshot_date,
sum(i.approved_accounts_receivable_amount/100.0) as approved_amount
from
(select * from debtors
where status='active' 
and debtor_limit<>100 
and approved_total>=debtor_limit) d 
cross join lateral 
generate_series(d.created_at, current_date, interval '1 day') gs
left join invoices i
on d.id = i.debtor_id
left join brokers b
on d.id = b.debtor_id
where 1=1
and (i.approved_date is not null and i.approved_date < gs)
and (i.paid_date is null or i.paid_date >= gs)
-- and i.approved_date >= current_date-455
and d.status = 'active'
--and d."name" = 'TOTAL QUALITY LOGISTICS'
group by
d.id,
-- d."name",
b.mc,
b.dot,
d.rating,
snapshot_date,
d.approved_total,
d.debtor_limit
)

,a1 as (select a.*, 
case when a.created_at>=current_date-30 then a.id else null end as invoice_id_created_l30,
case when a.created_at>=current_date-30 then b.limit_exceeded else null end as limit_exceeded, 
case when a.created_at>=current_date-30 then b.flagged_redd else null end as flagged_redd,
case when a.created_at>=current_date-30 then b.bypass_flag else null end as bypass_flag,
case when a.paid_date>=current_date-30 then extract(days from a.paid_date-a.approved_date) else null end as dtp
from
(select *
-- case when paid_date is not null then extract(days from paid_date-approved_date) else null end as dtp
from invoices where debtor_id in (select distinct id from cal_debtor_weekly_open_invoice_volume)) a
left join
(select invoice_id, max(limit_exceed_flag) as limit_exceeded, max(redd_flag) as flagged_redd, max(bypass_flag_notes) as bypass_flag from
(select invoice_id, case when update_status::text like '%broker limit exceeded%' then 1 else 0 end as limit_exceed_flag,
case when notes::text like '%redd%' then 1 else 0 end as redd_flag,
case when (notes::text like '%bypass%') then 1 else 0 end as bypass_flag_notes
from invoice_updates) x
group by invoice_id) b 
on a.id=b.invoice_id)

select * from a1
