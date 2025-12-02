with debtor_invoices as (
    select distinct id, created_at from invoices where debtor_id='{debtor_id}'
)


,cal_debtor_weekly_open_invoice_volume as --here
(
select
d.id,
-- d."name",
b.mc,
b.dot,
d.rating,
d.approved_total/100.0 as current_approved_total,
d.debtor_limit/100 as debtor_limit,
gs::date snapshot_date,
sum(i.approved_accounts_receivable_amount/100.0) as approved_amount
from
(select * from debtors where id='{debtor_id}') d 
cross join
generate_series(current_date-90, current_date, interval '1 day') gs
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

select * from cal_debtor_weekly_open_invoice_volume
