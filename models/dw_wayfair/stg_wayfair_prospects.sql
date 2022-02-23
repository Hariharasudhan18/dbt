{{ config(materialized='table') }}

--Prospects from Call Data, Per Day Level
        select 
        --Dimensions
        date(convert_timezone('PST','EST',A.date_completed)) as enrollment_date
        , 0 as enrollment_hour
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
        
        --Enrollment
        , 0 as "enrollments"
        , 0 as "live_transfers"
        , 0 as "dead"
        , 0 as "same_day_activations"
        , 0 as "phone_enrollments"
        , 0 as "email_enrollments"
        , 0 as "no_outreach_enrollments"
        , 0 as "ib_enrollments"
        , 0 as "ib_live_transfers"
        
        --Call
        , 0 as "calls"
        , 0 as "call_completed"
        , 0 as "call_could_not_connect"
        , 0 as "call_spoke_to_a_decisionmaker"
        , 0 as "call_spoke_to_a_gatekeeper"
        , 0 as "call_number_not_valid"
        , 0 as "call_not_wrapped"
        , 0 as "call_left_vm"
        , 0 as "call_total"
        , 0 as "call_duration_all"
        , 0 as "call_duration_completed"
        , 0 as "call_duration_could_not_connect"
        , 0 as "call_duration_spoke_to_a_decisionmaker"
        , 0 as "call_duration_spoke_to_a_gatekeeper"
        , 0 as "call_duration_number_not_valid"
        , 0 as "call_duration_not_wrapped"
        , 0 as "call_duration_left_vm"
        , count(distinct cast(nullif(right(regexp_replace(A.phone,'[^[:digit:]]',''),10),'') as bigint)) as "prospects"
        , 0 as "ib_calls"
        , 0 as "ib_contacts"
        , 0 as ib_call_completed
        , 0 as ib_call_could_not_connect
        , 0 as ib_call_spoke_to_a_decisionmaker
        , 0 as ib_call_spoke_to_a_gatekeeper
        , 0 as ib_call_number_not_valid
        , 0 as ib_call_not_wrapped
        , 0 as ib_call_left_vm
        , 0 as ib_call_duration_completed
        , 0 as ib_call_duration_could_not_connect
        , 0 as ib_call_duration_spoke_to_a_decisionmaker
        , 0 as ib_call_duration_spoke_to_a_gatekeeper
        , 0 as ib_call_duration_number_not_valid
        , 0 as ib_call_duration_not_wrapped
        , 0 as ib_call_duration_left_vm

        
        --Productivity from ActiveTrack
        , 0 as "productive_time"
        , 0 as "unproductive_time"
        , 0 as "undefined_time"
        
        --Activations
        , 0 as "activations"
        , 0 as "activations_enrollment"
        
        --Paid Time from Sprout
        , 0 as "paid_time"        
        
        --QA Score              
        , 0 as "quality_score_internal"      
        , 0 as "quality_score_external"      
        , 0 as "quality_evaluations_internal"
        , 0 as "quality_evaluations_external" 
		
	--Enrollment
	,0 as "waychat_enrollments" 
	
        --Reliability
        , 0 as "work_hours"
        , 0 as "scheduled_hours" 
        
        from client_wayfair.wayfair_call_logs A
        left join client_wayfair.sites B
        on A.owner_role = B.enrolled_by_role
        and date(convert_timezone('PST','EST',A.date_completed)) between B.start_date and B.end_date
        left join client_wayfair.wayfair_demographics C
        on A.assigned = C.salesforce_username
        and date(convert_timezone('PST','EST',A.date_completed)) between C.start_date and C.end_date
        WHERE date(convert_timezone('PST','EST',A.date_completed)) >= trunc(getdate())-2 --start_dt 
		AND date(convert_timezone('PST','EST',A.date_completed)) < trunc(getdate())+1 --end_dt + 1
        and task_subtype = 'Call'
        and call_duration > 0
        and (date(convert_timezone('PST','EST',A.date_completed)),a.assigned) not in 
           (
		   select entry_date, agent_name from {{ ref('stg_agent_attr') }} 
           )
        --and A.assigned not in ('Joyce Molina', 'Chris Medina', 'Raul Manalac', 'Joselito GO') --Exclude Test Data
        group by 1,3,4,5,6,7,8,9,10
--End of Prospects from Call Data, Per Day Level
