with debtor_invoices as (
    select distinct id, created_at from invoices where debtor_id='{debtor_id}'
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
(select * from debtors_history where original_id='{debtor_id}') dh 
cross join
generate_series(current_date-90, current_date, interval '1 day') gs
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