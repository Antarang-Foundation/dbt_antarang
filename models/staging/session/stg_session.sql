with
    t0 as (
        select * from {{ source('salesforce', 'Session__c') }} where IsDeleted = false
    ),
    t1 as (
        select
            Id as session_id,
            Batch__c as session_batch_id,
            Assigned_Facilitator__c as session_facilitator_id,
            Session_Code__c as session_code,
            Name as session_name,
            Session_Type__c as session_type,
            SessionDate__c as session_date,
            Session_Start_Time__c as session_start_time,
            Session_Grade__c as session_grade,
            Session_Number__c as session_no,
            OMR_required__c as 	omr_required,
            OMR_s_received_for_session__c as omrs_received,
            Total_Student_Present__c as total_student_present,
            Total_Parent_Present__c as 	total_parent_present,
            Log_Reason__c as log_reason,
            Attendance_Submitted__c as attendance_submitted,
            Present_Count__c as present_count,        --- present check on all session type. Use for individual session attednace
            Attendance_Count__c as attendance_count,  -- present + absent
            Payment_Status__c as payment_status,
            Deferred_Reason__c as deferred_reason,
            Invoice_Date__c as invoice_date,
            Session_Amount__c as session_amount,
            Number_of_Sessions_No_of_Units__c as no_of_sessions_no_of_units,
            Total_Amount__c as total_amount,
            Guardian_Parent_Count__c as parent_present_count,
            Session_Mode__c as session_mode
        from t0
        
    ),

    t2 as (select *, 
    
    count(distinct session_code) OVER (PARTITION BY session_batch_id) `batch_expected_sessions`,
    count(distinct case when session_type = 'Student' then session_code end) OVER (partition by session_batch_id) `batch_expected_student_type_session`,
    count(distinct case when session_type = 'Parent' then session_code end) OVER (partition by session_batch_id) `batch_expected_parent_type_session`,
    count(distinct case when session_type = 'Flexible' then session_code end) OVER (partition by session_batch_id) `batch_expected_flexible_type_session`,
    count(distinct case when session_type = 'Counseling' then session_code end) OVER (partition by session_batch_id) `batch_expected_counseling_type_session`,
    

    count(distinct case when session_date is not null then session_code end) OVER (PARTITION BY session_batch_id) `batch_scheduled_sessions`,
    count(distinct case when session_type = 'Student' and session_date is not null then session_code end) OVER (PARTITION BY session_batch_id) `batch_student_type_scheduled_sessions`,
    count(distinct case when session_type = 'Parent' and session_date is not null then session_code end) OVER (PARTITION BY session_batch_id) `batch_parent_type_scheduled_sessions`,
    count(distinct case when session_type = 'Flexible' and session_date is not null then session_code end) OVER (PARTITION BY session_batch_id) `batch_flexible_type_scheduled_sessions`,
    count(distinct case when session_type = 'Counseling' and session_date is not null then session_code end) OVER (PARTITION BY session_batch_id) `batch_counseling_type_scheduled_sessions`,
    

    count(distinct case when session_date is not null and total_student_present > 0 then session_code end) OVER (PARTITION BY session_batch_id) `batch_completed_sessions`,
    count(distinct case when session_type = 'Student' and session_date is not null and total_student_present > 0 then session_code end) OVER (PARTITION BY session_batch_id) `batch_student_type_completed_sessions`,
    count(distinct case when session_type = 'Parent' and session_date is not null and total_student_present > 0 then session_code end) OVER (PARTITION BY session_batch_id) `batch_parent_type_completed_sessions`,
    count(distinct case when session_type = 'Flexible' and session_date is not null and total_student_present > 0 then session_code end) OVER (PARTITION BY session_batch_id) `batch_flexible_type_completed_sessions`,
    count(distinct case when session_type = 'Counseling' and session_date is not null and total_student_present > 0 then session_code end) OVER (PARTITION BY session_batch_id) `batch_Counseling_type_completed_sessions`,
    

    max(case when session_type = 'Student' then total_student_present end) OVER (PARTITION BY session_batch_id, session_type) `batch_max_student_session_attendance`,
    max(case when session_type = 'Parent' then total_parent_present end) OVER (PARTITION BY session_batch_id, session_type) `batch_max_session_parent_attendance`,
    max(case when session_type = 'Flexible' then total_student_present end) OVER (PARTITION BY session_batch_id, session_type) `batch_max_session_flexible_attendance`,
    max(case when session_type = 'Counseling' then present_count end) OVER (PARTITION BY session_batch_id, session_type) `batch_max_session_counseling_attendance`,
    
    
     --sum(case when session_type = 'Student' then present_count end) OVER (PARTITION BY session_batch_id) `batch_indi_stud_attendance`,
    max(case when session_type = 'Student' then present_count end) OVER (PARTITION BY session_batch_id, session_type) `batch_indi_stud_attendance`,
    max(case when session_type = 'Parent' then present_count end) OVER (PARTITION BY session_batch_id, session_type) `batch_indi_parent_attendance`,
    max(case when session_type = 'Flexible' then present_count end) OVER (PARTITION BY session_batch_id, session_type) `batch_indi_flexible_attendance`,
    max(case when session_type = 'Counseling' then present_count end) OVER (PARTITION BY session_batch_id, session_type) `batch_indi_counseling_attendance`,

    max(case 
             when session_type = 'Student' then total_student_present 
             when session_type = 'Flexible' then total_student_present 
             when session_type = 'Parent' then total_parent_present 
             when session_type = 'Counseling' then present_count 
        end) 
        OVER (PARTITION BY session_batch_id, session_type) `batch_session_type_based_avg_overall_attendance`,
        
    MAX(total_student_present) OVER (PARTITION BY session_batch_id) AS batch_max_overall_attendance,
    max(CASE WHEN session_no = 0 AND session_type = 'Parent' THEN parent_present_count end) OVER (PARTITION BY session_batch_id, session_type) `total_reached_parents`
    
    from t1  
    )

select * from t2 where session_name not like '%test%' or session_name not like '%Test%' 
order by session_batch_id, session_id

