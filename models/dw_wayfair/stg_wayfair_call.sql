{{ config(materialized='table') }}

--Call Data 
        select 
        --Dimensions
        date(convert_timezone('PST','EST',A.date_completed)) as enrollment_date
        , cast(date_part(h, convert_timezone('PST','EST',A.date_completed)) as int) as enrollment_hour
        , A.owner_role as "teams"
        , B.site as "sites"
        , A.assigned as agents
        ,CASE 
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
        , cast(count(A.date_completed) as int) as "calls"
        , sum(case when A.reporting_status = 'Completed' then 1 else 0 end) as "Call_COMPLETED"
        , sum(case when A.reporting_status in ('Could not connect','No Answer','No Answer-No VM No EM') then 1 else 0 end) as "Call_Could_not_connect" --added No Answer
        , sum(case when A.reporting_status in ('Spoke to a Decisionmaker','Spoke to Decisionmaker','Spoke to Purchaser/DM/GK') then 1 else 0 end) as "Call_Spoke_to_a_Decisionmaker"
        , sum(case when A.reporting_status in ('Spoke to a Gatekeeper','Spoke to Gatekeeper') then 1 else 0 end) as "Call_Spoke_to_a_Gatekeeper"
        , sum(case when A.reporting_status in ('Number not valid','Could not connect-invalid number') then 1 else 0 end) as "Call_Number_not_valid" -- added Could not connect-invalid number
        , sum(case when A.reporting_status = 'Not wrapped' then 1 else 0 end) as "Call_Not_wrapped"
        , sum(case when A.reporting_status in ('Left VM','No Answer-Left VM','No Answer-Left VM & Send EM') then 1 else 0 end) as "Call_Left_VM" --added 'No Answer-Left VM','No Answer-Left VM & Send EM'
        , sum(case when A.reporting_status is not null then 1 else null end) as "Call_total"
        , sum(A.call_duration) as "call_duration_all"
        , sum(case when A.reporting_status = 'Completed' then call_duration else 0 end) as "call_duration_completed"
        , sum(case when A.reporting_status in ('Could not connect','No Answer','No Answer-No VM No EM') then call_duration else 0 end) as  "call_duration_could_not_connect"
        , sum(case when A.reporting_status in ('Spoke to a Decisionmaker','Spoke to Purchaser/DM/GK') then call_duration else 0 end) as  "call_duration_spoke_to_a_decisionmaker"
        , sum(case when A.reporting_status in ('Spoke to a Gatekeeper','Spoke to Gatekeeper') then call_duration else 0 end) as  "call_duration_spoke_to_a_gatekeeper"
        , sum(case when A.reporting_status in ('Number not valid','Could not connect-invalid number') then call_duration else 0 end) as  "call_duration_number_not_valid"
        , sum(case when A.reporting_status = 'Not wrapped' then call_duration else 0 end) as  "call_duration_not_wrapped"
        , sum(case when A.reporting_status in ('Left VM','No Answer-Left VM','No Answer-Left VM & Send EM') then call_duration else 0 end) as  "call_duration_left_vm"
        , 0 as "prospects"
        , sum(case when (a.subject ilike 'IBC%' OR a.call_type = 'Inbound') then 1 else 0 end) as "ib_calls"
        , sum(case
                when (a.subject ilike 'IBC%' OR a.call_type = 'Inbound') and B.site in ('Layton','Layton EEVO') and A.reporting_status in ('Spoke to a Decisionmaker','Spoke to Decisionmaker','Spoke to a Gatekeeper','Spoke to Gatekeeper','Completed','Spoke to Purchaser/DM/GK') then 1
                when (a.subject ilike 'IBC%' OR a.call_type = 'Inbound') and B.site not in ('Layton','Layton EEVO') and A.reporting_status in ('Spoke to a Decisionmaker','Spoke to Decisionmaker','Spoke to a Gatekeeper','Spoke to Gatekeeper','Spoke to Purchaser/DM/GK') then 1
                else 0
             end) as "ib_contacts"
        , sum(case when (a.subject ilike 'IBC%' OR a.call_type = 'Inbound') and A.reporting_status = 'Completed' then 1 else 0 end) as "ib_call_completed"
        , sum(case when (a.subject ilike 'IBC%' OR a.call_type = 'Inbound') and A.reporting_status in ('Could not connect','No Answer','No Answer-No VM No EM') then 1 else 0 end) as "ib_call_could_not_connect"
        , sum(case when (a.subject ilike 'IBC%' OR a.call_type = 'Inbound') and A.reporting_status in ('Spoke to a Decisionmaker','Spoke to Decisionmaker','Spoke to Purchaser/DM/GK') then 1 else 0 end) as "ib_call_spoke_to_a_decisionmaker"
        , sum(case when (a.subject ilike 'IBC%' OR a.call_type = 'Inbound') and A.reporting_status in ('Spoke to a Gatekeeper','Spoke to Gatekeeper') then 1 else 0 end) as "ib_call_spoke_to_a_gatekeeper"
        , sum(case when (a.subject ilike 'IBC%' OR a.call_type = 'Inbound') and A.reporting_status in ('Number not valid','Could not connect-invalid number') then 1 else 0 end) as "ib_call_number_not_valid"
        , sum(case when (a.subject ilike 'IBC%' OR a.call_type = 'Inbound') and A.reporting_status = 'Not wrapped' then 1 else 0 end) as "ib_call_not_wrapped"
        , sum(case when (a.subject ilike 'IBC%' OR a.call_type = 'Inbound') and A.reporting_status in ('Left VM','No Answer-Left VM','No Answer-Left VM & Send EM') then 1 else 0 end) as "ib_call_left_vm"
        , sum(case when (a.subject ilike 'IBC%' OR a.call_type = 'Inbound') and A.reporting_status = 'Completed' then call_duration else 0 end) as "ib_call_duration_completed"
        , sum(case when (a.subject ilike 'IBC%' OR a.call_type = 'Inbound') and A.reporting_status in ('Could not connect','No Answer','No Answer-No VM No EM') then call_duration else 0 end) as  "ib_call_duration_could_not_connect"
        , sum(case when (a.subject ilike 'IBC%' OR a.call_type = 'Inbound') and A.reporting_status in ('Spoke to a Decisionmaker','Spoke to Purchaser/DM/GK') then call_duration else 0 end) as  "ib_call_duration_spoke_to_a_decisionmaker"
        , sum(case when (a.subject ilike 'IBC%' OR a.call_type = 'Inbound') and A.reporting_status in ('Spoke to a Gatekeeper','Spoke to Gatekeeper') then call_duration else 0 end) as  "ib_call_duration_spoke_to_a_gatekeeper"
        , sum(case when (a.subject ilike 'IBC%' OR a.call_type = 'Inbound') and A.reporting_status in ('Number not valid','Could not connect-invalid number') then call_duration else 0 end) as  "ib_call_duration_number_not_valid"
        , sum(case when (a.subject ilike 'IBC%' OR a.call_type = 'Inbound') and A.reporting_status = 'Not wrapped' then call_duration else 0 end) as  "ib_call_duration_not_wrapped"
        , sum(case when (a.subject ilike 'IBC%' OR a.call_type = 'Inbound') and A.reporting_status in ('Left VM','No Answer-Left VM','No Answer-Left VM & Send EM') then call_duration else 0 end) as  "ib_call_duration_left_vm"
        
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
        and date(convert_timezone('PST','EST',A.date_completed)) between c.start_date and c.end_date
        WHERE date(convert_timezone('PST','EST',A.date_completed)) >= trunc(getdate())-2 --start_dt 
		AND date(convert_timezone('PST','EST',A.date_completed)) < trunc(getdate())+1 --end_dt + 1
        and task_subtype = 'Call'
        and call_duration > 0
        and (date(convert_timezone('PST','EST',A.date_completed)),a.assigned) not in 
           (
		   select entry_date, agent_name from {{ ref('stg_agent_attr') }} 
           )
        --and A.assigned not in ('Joyce Molina', 'Chris Medina', 'Raul Manalac', 'Joselito GO') --Exclude Test Data
        group by 1,2,3,4,5,6,7,8,9,10
--End Of Call Data
