with
    source as (
        select * from {{ source('salesforce', 'Session__c') }}
    ),
    renamed as (
        select
            id as sessions_id,
            session_code__c as sessions_code,
            name as sessions_name,
            session_type__c as sessions_type,
            sessiondate__c as sessions_date,
            session_grade__c as sessions_grade,
            session_number__c as sessions_number,
            assigned_facilitator__c as assigned_facilitator_id,
            batch__c as batches_id,
            Session_Delivery__c as Session_Delivery,
            Session_Mode__c as 	Session_Mode,
            Session_Start_Time__c as Session_Start_Time,
            OMR_required__c as 	OMR_required,
            OMR_s_received_for_session__c as OMR_s_received_for_session,
            Total_Student_Present__c as Total_Student_Present,
            Total_Parent_Present__c as 	Total_Parent_Present,
            Log_Reason__c as Log_Reason,
            Attendance_Submitted__c as Attendance_Submitted,
            Present_Count__c as Present_Count,
            Attendance_Count__c as Attendance_Count,
            Payment_Status__c as Payment_Status,
            Deferred_Reason__c as Deferred_Reason,
            Invoice_Date__c as Invoice_Date,
            Session_Amount__c as Session_Amount,
            Number_of_Sessions_No_of_Units__c as Number_of_Sessions_No_of_Units,
            Total_Amount__c as Total_Amount,
            TDS_Deduction__c as TDS_Deduction


        from source
    )

select * from renamed
where sessions_type = 'Student'