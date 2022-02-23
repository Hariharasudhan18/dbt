{{ config(materialized='table') }}

--Start of Reliability Data
        select 
        --Dimensions
        B.entry_date
        , -1 as "enrollment_hour"
        , null as teams
        , null as sites
        , B.agent_name 
        , null as production_status
        , null as agent_status
        , null as wave
        , CURRENT_DATE as production_date
        , null as supervisor_name
               
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
        , 0 as "prospects"
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
        , 0 as "qa_score_internal"      
        , 0 as "quality_score_external"      
        , 0 as "quality_evaluations_internal"
        , 0 as "quality_evaluations_external" 
		
		--Enrollment
		, 0 as "waychat_enrollments" 
	
        --Reliability
        , sum(b.work_hours) as "work_hours"
        , sum(b.scheduled_hours) as "scheduled_hours" 
          
        from client_wayfair.wayfair_agent_reliability B  
        WHERE B.entry_date >= trunc(getdate())-2 --start_dt 
		AND B.entry_date < trunc(getdate())+1 --end_dt + 1
        GROUP BY 1,2,3,4,5,6,7,8,9,10
		 --End of Reliability Data
