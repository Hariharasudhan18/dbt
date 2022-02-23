{{ config(materialized='table') }}

--Start of Activation Data
        select 
        --Dimensions
        A.enrollment_date
        , 0 as "enrollment_hour"
        , A.teams
        , A.sites
        , A.agents
        , A.production_status
        , A.agent_status
        , A.wave
        , A.production_date
        , A.supervisor_name
               
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
        , sum(B.activations) as "activations"
        , sum(B.enrollments) as "activations_enrollment"
        
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
        , 0 as "work_hours"
        , 0 as "scheduled_hours" 
          
        from 
                (
                SELECT DISTINCT 
                enrollment_date
                , teams
                , sites
                , agents
                , production_status
                , agent_status
                , wave
                , production_date
                , supervisor_name
                FROM
                        (
                                --Enrollment Data Dimensions
                           select * from {{ ref('stg_enrol_attr') }}
                           
                           UNION ALL
                           
                           --Call Data Dimensions
                           select * from {{ ref('stg_call_attr') }}
						   
                        ) as Combined_Enrollment_and_Call_Data
                ) A --List of Agents with Enrollment/Call Data
        INNER JOIN client_wayfair.wayfair_activation_rate B     
        ON A.agents = B.employee_name
        AND A.enrollment_date = B.entry_date
        WHERE A.enrollment_date >= trunc(getdate())-2 --start_dt 
		AND A.enrollment_date < trunc(getdate())+1 --end_dt + 1
        GROUP BY 1,2,3,4,5,6,7,8,9,10
 
 --End of Activation Data
 