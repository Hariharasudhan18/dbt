{{ config(materialized='table') }}

--Enrollment Data Dimensions
select 
date(convert_timezone('PST','EST',A.enrollment_date)) as enrollment_date
, A.enrolled_by_role as "teams"
, B.site as "sites"
, A.enrolled_by as agents
, CASE 
	WHEN B.SITE LIKE '%FPS%' THEN
		CASE 
			WHEN date(convert_timezone('PST','EST',A.enrollment_date)) >= C.production_date then 'Production'
			WHEN date(convert_timezone('PST','EST',A.enrollment_date)) >= C.nesting_date and date(convert_timezone('PST','EST',A.enrollment_date)) < dateadd(day, 14, C.nesting_date) then 'Nesting Phase 1'
			WHEN date(convert_timezone('PST','EST',A.enrollment_date)) >= C.nesting_date and date(convert_timezone('PST','EST',A.enrollment_date)) < C.production_date then 'Nesting Phase 2'
			WHEN date(convert_timezone('PST','EST',A.enrollment_date)) >= C.training_date then 'Training' 
			Else 'Not Existing in the File Provided'
		END 
	WHEN B.SITE LIKE '%OP360%' then 
		CASE 
			WHEN date(convert_timezone('PST','EST',A.enrollment_date)) >= C.production_date then 'Production'
			WHEN date(convert_timezone('PST','EST',A.enrollment_date)) >= C.nesting_date and date(convert_timezone('PST','EST',A.enrollment_date)) < dateadd(day, 7, C.nesting_date) then 'Nesting Phase 1'
			WHEN date(convert_timezone('PST','EST',A.enrollment_date)) >= C.nesting_date and date(convert_timezone('PST','EST',A.enrollment_date)) < C.production_date then 'Nesting Phase 2'
			WHEN date(convert_timezone('PST','EST',A.enrollment_date)) >= C.training_date then 'Training' 
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

from client_wayfair.wayfair_enrollments A
left join client_wayfair.sites B
on A.enrolled_by_role = B.enrolled_by_role
and date(convert_timezone('PST','EST',A.enrollment_date)) between B.start_date and B.end_date
left join client_wayfair.wayfair_demographics C
on A.enrolled_by = C.salesforce_username
and date(convert_timezone('PST','EST',A.enrollment_date)) between C.start_date and C.end_date
--where A.enrolled_by not in ('Joyce Molina', 'Chris Medina', 'Raul Manalac', 'Joselito GO') --Exclude Test Data
WHERE (date(convert_timezone('PST','EST',A.enrollment_date)),a.enrolled_by) not in 
           (
		   select entry_date, agent_name from {{ ref('stg_agent_attr') }} 
           )
group by 1,2,3,4,5,6,7,8,9
