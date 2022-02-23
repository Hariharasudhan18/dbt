{{ config(materialized='table') }}

select distinct entry_date, agent_name
FROM client_wayfair.wayfair_agent_reliability a
where a.scheduled_hours < 1
and entry_date >= trunc(getdate())-2 --start_dt 
and entry_date < trunc(getdate())+1 --end_dt + 1
union
select entry_date, agent_name 
from client_wayfair.wayfair_exclude_agent
where entry_date >= trunc(getdate())-2 --start_dt 
and entry_date < trunc(getdate())+1 --end_dt + 1
