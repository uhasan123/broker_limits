with calc_debtor_limit as (
select * from
(select *, extract(days from (snapshot_date-created_at)) as dayss
from
(select
dh.original_id,
dh.debtor_limit,
lag(dh.debtor_limit, 1) over (order by dh.created_at asc) as previous_debtor_limit,
dh.created_at,
gs::date snapshot_date
from
(select * from debtors_history where debtor_limit is not null) dh 
cross join
generate_series(current_date-90, current_date, interval '1 day') gs
group by
dh.original_id,
dh.debtor_limit,
dh.created_at   ,
snapshot_date
) a ) b
where dayss>=0
)

select distinct on (original_id, snapshot_date)
* from 
calc_debtor_limit order by original_id,snapshot_date, created_at desc