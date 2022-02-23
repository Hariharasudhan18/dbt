{{ config(materialized='table') }}

--Aggregate All Data       
 SELECT 
        --Dimensions
        enrollment_date
        , enrollment_hour
        , teams
        , sites
        , agents
        , production_status
        , nvl(agent_status,'A') as "agent_status"
        , wave
        , production_date
        , supervisor_name
        
        --Enrollment
        , sum(nvl(enrollments,0)) as enrollments
        , sum(nvl(live_transfers,0)) as live_transfers
        , sum(nvl(dead,0)) as dead
        , sum(nvl(same_day_activations,0)) as "same_day_activations"
        , sum(nvl(phone_enrollments,0)) as phone_enrollments
	      , sum(nvl(email_enrollments,0)) as email_enrollments
	      , sum(nvl(no_outreach_enrollments,0)) as no_outreach_enrollments
	      , sum(nvl(ib_enrollments,0)) as ib_enrollments
	      , sum(nvl(ib_live_transfers,0)) as ib_live_transfers 
        
        --Call
        , sum(nvl(calls,0)) as calls        
        , sum(nvl(Call_COMPLETED,0)) as Call_COMPLETED
        , sum(nvl(Call_Could_not_connect,0)) as Call_Could_not_connect
        , sum(nvl(Call_Spoke_to_a_Decisionmaker,0)) as Call_Spoke_to_a_Decisionmaker
        , sum(nvl(Call_Spoke_to_a_Gatekeeper,0)) as Call_Spoke_to_a_Gatekeeper
        , sum(nvl(Call_Number_not_valid,0)) as Call_Number_not_valid
        , sum(nvl(Call_Not_wrapped,0)) as Call_Not_wrapped
        , sum(nvl(Call_Left_VM,0)) as Call_Left_VM
        , sum(nvl(Call_total,0)) as Call_total
        , sum(nvl(call_duration_all,0)) as call_duration_all
        , sum(nvl(call_duration_completed,0)) as call_duration_completed
        , sum(nvl(call_duration_could_not_connect,0)) as call_duration_could_not_connect
        , sum(nvl(call_duration_spoke_to_a_decisionmaker,0)) as call_duration_spoke_to_a_decisionmaker
        , sum(nvl(call_duration_spoke_to_a_gatekeeper,0)) as call_duration_spoke_to_a_gatekeeper
        , sum(nvl(call_duration_number_not_valid,0)) as call_duration_number_not_valid
        , sum(nvl(call_duration_not_wrapped,0)) as call_duration_not_wrapped
        , sum(nvl(call_duration_left_vm,0)) as call_duration_left_vm
        , sum(nvl(prospects,0)) as prospects
        , sum(nvl(ib_calls,0)) as ib_calls
        , sum(nvl(ib_contacts,0)) as ib_contacts
        , sum(nvl(ib_call_completed,0)) as ib_call_completed
        , sum(nvl(ib_call_could_not_connect,0)) as ib_call_could_not_connect
        , sum(nvl(ib_call_spoke_to_a_decisionmaker,0)) as ib_call_spoke_to_a_decisionmaker
        , sum(nvl(ib_call_spoke_to_a_gatekeeper,0)) as ib_call_spoke_to_a_gatekeeper
        , sum(nvl(ib_call_number_not_valid,0)) as ib_call_number_not_valid
        , sum(nvl(ib_call_not_wrapped,0)) as ib_call_not_wrapped
        , sum(nvl(ib_call_left_vm,0)) as ib_call_left_vm
        , sum(nvl(ib_call_duration_completed,0)) as ib_call_duration_completed
        , sum(nvl(ib_call_duration_could_not_connect,0)) as ib_call_duration_could_not_connect
        , sum(nvl(ib_call_duration_spoke_to_a_decisionmaker,0)) as ib_call_duration_spoke_to_a_decisionmaker
        , sum(nvl(ib_call_duration_spoke_to_a_gatekeeper,0)) as ib_call_duration_spoke_to_a_gatekeeper
        , sum(nvl(ib_call_duration_number_not_valid,0)) as ib_call_duration_number_not_valid
        , sum(nvl(ib_call_duration_not_wrapped,0)) as ib_call_duration_not_wrapped
        , sum(nvl(ib_call_duration_left_vm,0)) as ib_call_duration_left_vm
        
         --Productivity from ActiveTrack
        , sum(nvl(productive_time,0)) as productive_time
        , sum(nvl(unproductive_time,0)) as unproductive_time
        , sum(nvl(undefined_time,0)) as undefined_time
        
        --Activations      
        , sum(nvl(activations,0)) as activations
        , sum(nvl(activations_enrollment,0)) as activations_enrollment
        
        --Paid Time from Sprout
        , sum(nvl(paid_time,0)) as paid_time       

        --QA Score          
        , sum(nvl(quality_score_internal,0)) as quality_score_internal
        , sum(nvl(quality_score_external,0)) as quality_score_external
        , sum(nvl(quality_evaluations_internal,0)) as quality_evaluations_internal
        , sum(nvl(quality_evaluations_external,0)) as quality_evaluations_external
		
	--Enrollment
	, sum(nvl(waychat_enrollments,0)) as waychat_enrollments 
        
        --Reliability
	, sum(nvl(work_hours,0)) as work_hours 
	, sum(nvl(scheduled_hours,0)) as scheduled_hours 
FROM (
select * from {{ ref('stg_wayfair_enrollment') }}
union all
select * from {{ ref('stg_wayfair_call') }}
union all
select * from {{ ref('stg_wayfair_prospects') }}
union all
select * from {{ ref('stg_wayfair_productivity') }}
union all
select * from {{ ref('stg_wayfair_conversion') }}
union all
select * from {{ ref('stg_wayfair_qa_score') }}
union all
select * from {{ ref('stg_wayfair_reliability') }}
)
GROUP BY 1,2,3,4,5,6,7,8,9,10