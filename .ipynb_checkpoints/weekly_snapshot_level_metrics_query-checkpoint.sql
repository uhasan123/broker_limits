with mondays as
(
SELECT generate_series(
    date_trunc('week', current_date) - interval '{cohort_size_2} weeks',
    date_trunc('week', current_date),
    interval '1 week'
)::date AS snapshot_date
)

select 
d.id,
d.name,
b.mc,
b.dot,
d.approved_total/100.0 as current_approved_total,
d.debtor_limit/100 as debtor_limit,
m.snapshot_date,
sum(case when i.approved_date is not null and i.approved_date < m.snapshot_date and (i.paid_date is null or i.paid_date >= m.snapshot_date)
then i.approved_accounts_receivable_amount/100.0 else 0 end) as open_invoice_amount,
sum (case when i.approved_date is not null and i.approved_date <m.snapshot_date and i.approved_date>=m.snapshot_date-7 then i.approved_accounts_receivable_amount/100.0 else 0 end) as invoices_approved_dollars,
sum (case when i.paid_date is not null and i.paid_date <m.snapshot_date and i.paid_date>=m.snapshot_date-7 then i.approved_accounts_receivable_amount/100.0 else 0 end) as invoices_paid_dollars
from mondays m
cross join debtors d
left join invoices i
on d.id = i.debtor_id
left join brokers b
on d.id = b.debtor_id
--where d.id in ('00b6cae4-0c7c-42b7-b035-206212f725c4')
group by 
d.id,
d.name,
b.mc,
b.dot,
current_approved_total,
debtor_limit,
m.snapshot_date
order by m.snapshot_date asc