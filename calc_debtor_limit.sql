with mondays as
(
SELECT generate_series(current_date-30,current_date,'1 day'::interval)::date AS snapshot_date
),

calc_debtor_limit as (
-- select *, dense_rank() over (partition by name, snapshot_date order by dayss asc) as rn
-- from
-- (
select * from
(select *, extract(days from (snapshot_date-created_at)) as dayss
from
(select
dh."name",
dh.debtor_limit,
dh.created_at,
m.snapshot_date
from mondays m
cross join debtors_history dh
--and d."name" = 'TOTAL QUALITY LOGISTICS'
group by
dh."name",
dh.debtor_limit,
dh.created_at,
snapshot_date
) a ) b
where dayss>=0 and debtor_limit is not null
-- ) c
-- where rn=1
)

select * from calc_debtor_limit
order by created_at desc