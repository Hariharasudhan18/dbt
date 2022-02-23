{{ config(materialized='table') }}

--Call Data Dimensions
SELECT
date(convert_timezone('PST','EST',A.date_completed)) as enrollment_date
, A.owner_role as "teams"
, B.site as "sites"
, assigned as agents
, CASE 
  WHEN B.SITE LIKE '%FPS%' THEN
         CASE 
              WHEN date(convert_timezone('PST','EST',A.date_completed)) >= C.production_date then 'Production'
              WHEN date(convert_timezone('PST','EST',A.date_completed)) >= C.nesting_date and date(convert_timezone('PST','EST',A.date_completed)) < dateadd(day, 14, C.nesting_date) then 'Nesting Phase 1'
              WHEN date(convert_timezone('PST','EST',A.date_completed)) >= C.nesting_date and date(convert_timezone('PST','EST',A.date_completed)) < C.production_date then 'Nesting Phase 2'
              WHEN date(convert_timezone('PST','EST',A.date_completed)) >= C.training_date then 'Training' 
              Else 'Not Existing in the File Provided'
         END 
  WHEN B.SITE LIKE '%OP360%' then 
         CASE 
              WHEN date(convert_timezone('PST','EST',A.date_completed)) >= C.production_date then 'Production'
              WHEN date(convert_timezone('PST','EST',A.date_completed)) >= C.nesting_date and date(convert_timezone('PST','EST',A.date_completed)) < dateadd(day, 7, C.nesting_date) then 'Nesting Phase 1'
              WHEN date(convert_timezone('PST','EST',A.date_completed)) >= C.nesting_date and date(convert_timezone('PST','EST',A.date_completed)) < C.production_date then 'Nesting Phase 2'
              WHEN date(convert_timezone('PST','EST',A.date_completed)) >= C.training_date then 'Training' 
              Else 'Production'
         END
  ELSE 'Production'
  END as "production_status"
, CASE 
        WHEN C.status ='LOA' then 'I' 
        WHEN C.status ='Termed' then 'T' 
        ELSE 'A'
  END as "agent_status"
, nvl(C.wave,'0') as "wave"
, C.production_date
, C.supervisor_name

from client_wayfair.wayfair_call_logs A
left join client_wayfair.sites B
on A.owner_role = B.enrolled_by_role
and date(convert_timezone('PST','EST',A.date_completed)) between B.start_date and B.end_date
left join client_wayfair.wayfair_demographics C
on A.assigned = C.salesforce_username
and date(convert_timezone('PST','EST',A.date_completed)) between C.start_date and C.end_date
where task_subtype = 'Call'
and call_duration > 0
and (date(convert_timezone('PST','EST',A.date_completed)),a.assigned) not in 
           (
		   select entry_date, agent_name from {{ ref('stg_agent_attr') }} 
           )
--where A.assigned not in ('Joyce Molina', 'Chris Medina', 'Raul Manalac', 'Joselito GO') --Exclude Test Data
group by 1,2,3,4,5,6,7,8,9
