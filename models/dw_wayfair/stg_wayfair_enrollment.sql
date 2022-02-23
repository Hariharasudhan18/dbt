 {{ config(materialized='table') }}

 --Enrollment Data
        select
        --Dimensions
        date(convert_timezone('PST','EST',A.enrollment_date)) as enrollment_date
        , cast(date_part(h, convert_timezone('PST','EST',A.enrollment_date)) as int) as enrollment_hour
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
        
        --Enrollment
        , nvl(cast(count(A.enrollment_date) as int),0) as "enrollments"
        , nvl(sum(case when A.enrollment_method LIKE '%Live Transfer%' then 1 else null end),0) as "live_transfers"
        , nvl(sum(case when A.status LIKE '%Dead%' then 1 else null end),0) as "dead"
        , nvl(sum(case when date(A.enrollment_date) = date(A.activation_date) then 1 else 0 end),0) as "same_day_activations"
        , nvl(sum(case when A.enrollment_method in ('Manual: Phone - Live Transfer','Manual: Phone','Directly Enrolled: Admin Lead Creator','Manual: No Outreach - Bam Request','Directly Enrolled: CAM Add to Account','Aquisition Evolution-OD','Manual: Onboarding Evolution','') then 1 else 0 end),0) as "phone_enrollments"
        , nvl(sum(case when A.enrollment_method in ('Manual: Email') then 1 else 0 end),0) as "email_enrollments"
        , nvl(sum(case when A.enrollment_method in ('Manual: No Outreach','Directly Enrolled: EIN Provided','Directly Enrolled: Bulk Lead Creator','Manual: Waychat') then 1 else 0 end),0) as "no_outreach_enrollments"
        , nvl(cast(count(case when A.ib_ob = 'IB' then A.enrollment_date else null end) as int),0) as "ib_enrollments"
        , nvl(sum(case when A.ib_ob = 'IB' and A.enrollment_method LIKE '%Live Transfer%' then 1 else 0 end),0) as "ib_live_transfers"
		
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
        , 0 as "prospects"
        , 0 as ib_calls
        , 0 as ib_contacts
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
        , nvl(sum(case when A.enrollment_method LIKE '%Waychat%' then 1 else 0 end),0) as "waychat_enrollments"
        
        --Reliability
        , 0 as "work_hours"
        , 0 as "scheduled_hours" 
        
        from client_wayfair.wayfair_enrollments A
        left join client_wayfair.sites B
        on A.enrolled_by_role = B.enrolled_by_role
        and date(convert_timezone('PST','EST',A.enrollment_date)) between B.start_date and B.end_date
        left join client_wayfair.wayfair_demographics C
        on A.enrolled_by = C.salesforce_username
        and date(convert_timezone('PST','EST',A.enrollment_date)) between C.start_date and c.end_date
        WHERE date(convert_timezone('PST','EST',A.enrollment_date)) >= trunc(getdate())-2 --start_dt 
		AND date(convert_timezone('PST','EST',A.enrollment_date)) < trunc(getdate())+1 --end_dt + 1
        --and A.enrolled_by not in ('Joyce Molina', 'Chris Medina', 'Raul Manalac', 'Joselito GO') --Exclude Test Data
        and (date(convert_timezone('PST','EST',A.enrollment_date)),a.enrolled_by) not in 
           (
		   select entry_date, agent_name from {{ ref('stg_agent_attr') }} 
           )
        group by 1,2,3,4,5,6,7,8,9,10
--End of Enrollment Data

