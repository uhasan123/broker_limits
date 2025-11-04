with mondays as
(
SELECT generate_series(current_date-30,current_date,'1 day'::interval)::date AS snapshot_date
),

cal_debtor_weekly_open_invoice_volume as --here
(
select
d.id,
d."name",
b.mc,
b.dot,
d.rating,
d.approved_total/100.0 as current_approved_total,
d.debtor_limit,
m.snapshot_date,
sum(i.approved_accounts_receivable_amount/100.0) as approved_amount
from mondays m
cross join (select * from debtors where debtor_limit<>100) d
left join invoices i
on d.id = i.debtor_id
left join brokers b
on d.id = b.debtor_id
where 1=1
and (i.approved_date is not null and i.approved_date < m.snapshot_date)
and (i.paid_date is null or i.paid_date >= m.snapshot_date)
and i.approved_date >= current_date-455
and d.status = 'active'
--and d."name" = 'TOTAL QUALITY LOGISTICS'
group by
d.id,
d."name",
b.mc,
b.dot,
d.rating,
snapshot_date,
d.approved_total,
d.debtor_limit
)

select * from cal_debtor_weekly_open_invoice_volume
