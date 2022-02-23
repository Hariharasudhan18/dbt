{{ config(materialized='table') }}

--QA Score Data                
        select 
        --Dimensions
        B.enrollment_date
        , -1 as "enrollment_hour"
        , null as teams
        , null as sites
        , B.agents 
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
        , sum(CASE WHEN B.qa_type = 'internal' THEN B.qa_score ELSE 0 END) as "qa_score_internal"
        , sum(CASE WHEN B.qa_type = 'external' THEN B.qa_score ELSE 0 END) as "qa_score_external"
        , count(CASE WHEN B.qa_type = 'internal' AND  isnull(B.baid,'') <> '' THEN B.baid  ELSE NULL END)    as "qa_evaluations_internal"
        , count(CASE WHEN B.qa_type = 'external' AND  isnull(B.baid,'') <> '' THEN B.baid ELSE NULL END)    as "qa_evaluations_external"
		
		--Enrollment
		, 0 as "waychat_enrollments"
	
        --Reliability
        , 0 as "work_hours"
        , 0 as "scheduled_hours" 
        
        from 
        (
                select a.entry_date as  enrollment_date,
                a.agent_name as agents,
                a.overall_quality_scores as qa_score,
                a.qa_type,
                a.baid
                from (select 
                        date(nvl(audit_date,review_date)) as entry_date,
                        agent_name,
                        overall_quality_scores ,
                        qa_type,
                        a.reviewed_by,
                        a.cuid as baid
                        from client_wayfair.wayfair_qa_survey a
                        union ALL
                        select
                        date(nvl(b.audit_date,b.entry_date)) as entry_date,
                        b.agent_name,
                        b.score as overall_quality_scores,
                        b.qa_type,
                        b.reviewed_by,
                        b.baid_cuid as baid
                        from client_wayfair.wayfair_qa_survey_new b) a
                where reviewed_by not LIKE '%Test%'
                and a.entry_date >= trunc(getdate())-2 --start_dt 
				AND a.entry_date  < trunc(getdate())+1 --end_dt + 1
        ) B
        GROUP BY 1,2,3,4,5,6,7,8,9,10
--End of QA Score Data
