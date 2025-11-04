select a.*, b.limit_exceeded, b.flagged_redd, b.bypass_flag
from
(select * from invoices where created_at<=current_date and created_at>current_date-30) a
left join
(select invoice_id, max(limit_exceed_flag) as limit_exceeded, max(redd_flag) as flagged_redd, max(bypass_flag_notes) as bypass_flag from
(select invoice_id, case when update_status::text like '%broker limit exceeded%' then 1 else 0 end as limit_exceed_flag,
case when notes::text like '%redd%' then 1 else 0 end as redd_flag,
case when (notes::text like '%bypass%') then 1 else 0 end as bypass_flag_notes
from invoice_updates) x
group by invoice_id) b 
on a.id=b.invoice_id