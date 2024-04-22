with
    t0 as (
        select * from {{ source('salesforce', 'Session__c') }}
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
            Session_Grade__c as session_grade,
            Session_Number__c as session_no,
            /* Session_Delivery__c as session_delivery,
            Session_Mode__c as 	session_mode,
            Session_Start_Time__c as session_start_time, */
            OMR_required__c as 	omr_required,
            OMR_s_received_for_session__c as omrs_received,
            Total_Student_Present__c as total_student_present,
            Total_Parent_Present__c as 	total_parent_present,
            Log_Reason__c as log_reason,
            Attendance_Submitted__c as attendance_submitted,
            Present_Count__c as present_count,
            Attendance_Count__c as attendance_count,
            Payment_Status__c as payment_status,
            Deferred_Reason__c as deferred_reason,
            Invoice_Date__c as invoice_date,
            Session_Amount__c as session_amount,
            Number_of_Sessions_No_of_Units__c as no_of_sessions_no_of_units,
            Total_Amount__c as total_amount,
            /* TDS_Deduction__c as tds_deduction */


        from t0
    ),

    t2 as (select *, MAX(total_student_present) OVER (PARTITION BY session_batch_id) AS max_reach from t1)

select * from t2 
order by session_batch_id, session_id