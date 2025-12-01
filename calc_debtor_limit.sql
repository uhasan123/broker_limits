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

,debtors_create_date as (
select dh.*, d.debtor_create_date
from
(select * from debtors_history where debtor_limit is not null and original_id in (select distinct id from cal_debtor_weekly_open_invoice_volume)) dh
left join
(select id, created_at as debtor_create_date from debtors) d
on dh.original_id=d.id

)


,calc_debtor_limit as (
select * from
(select *, extract(days from (snapshot_date-created_at)) as dayss
from
(select
dh.original_id,
dh.debtor_limit,
dh.created_at,
gs::date snapshot_date
from
debtors_create_date dh 
cross join lateral 
generate_series(dh.debtor_create_date, current_date, interval '1 day') gs
--and d."name" = 'TOTAL QUALITY LOGISTICS'
group by
dh.original_id,
dh.debtor_limit,
dh.created_at   ,
snapshot_date
) a ) b
where dayss>=0
)

select * from calc_debtor_limit
order by created_at desc