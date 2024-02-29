with
    account_batch as (select * from {{ ref('int_global') }}),
    sessions as (select * from {{ref ('stg_sessions') }}),
    trainers as (select contact_id, full_name from {{ ref('stg_trainer') }}),

    int_batch_schedule as (
       select batches_id, batch_no, batch_academic_year, batch_grade, 
       batch_language, Number_of_students_facilitated, facilitation_start, facilitation_end, Allocation_Email_Sent, 
       school, school_language, school_academic_year, Enrolled_Grade_9, Enrolled_Grade_10, Enrolled_Grade_11, Enrolled_Grade_12, 
       school_district, school_state, school_partner, taluka, ward, Tagged_for_Counselling, 
       full_name as batch_facilitator, trainers.full_name as assigned_facilitator, donor,
       sessions_id, sessions_code, sessions_name, sessions_type,
       sessions_date, sessions_grade, sessions_number, 
       Session_Delivery,
       Session_Mode, Session_Start_Time, OMR_required, OMR_s_received_for_session,
       Total_Student_Present, Total_Parent_Present, Log_Reason,
       Attendance_Submitted, Present_Count, Attendance_Count, Payment_Status,
       Deferred_Reason, Invoice_date, Session_Amount, 	
       Number_of_Sessions_No_of_Units, Total_Amount, TDS_Deduction
       from
       sessions
       left join account_batch on sessions.batches_id = account_batch.batch_id
       left join trainers on sessions.assigned_facilitator_id = trainers.contact_id
       
    )

select * from int_batch_schedule
          