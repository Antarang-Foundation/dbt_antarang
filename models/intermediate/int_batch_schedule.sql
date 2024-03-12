with
    sessions as (select * from {{ref ('stg_session') }}),
    batch_global as (select * from {{ ref('int_global') }}),
    trainers as (select contact_id, facilitator_name from {{ ref('stg_facilitator') }}),

    int_batch_schedule as (

       select 
       
       batch_id, session_batch_id, batch_no, batch_academic_year, batch_grade, batch_language, no_of_students_facilitated, fac_start_date, fac_end_date, 
       allocation_email_sent, 
       
       school, school_language, school_academic_year, enrolled_g9, enrolled_g10, enrolled_g11, enrolled_g12, tagged_for_counselling,
       taluka, ward, school_district, school_state, school_partner, donor, 
       
       trainers.facilitator_name as session_facilitator,
       
       session_id, session_code, session_name, session_type, session_date, session_grade, session_no, session_delivery, session_mode, session_start_time, 
       omr_required, omrs_received, total_student_present, total_parent_present, log_reason, attendance_submitted, present_count, attendance_count, 
       payment_status, deferred_reason, invoice_date, session_amount, no_of_sessions_no_of_units, total_amount, tds_deduction
       
       from
       
       sessions
       
       full outer join batch_global on sessions.session_batch_id = batch_global.batch_id
       full outer join trainers on sessions.session_facilitator_id = trainers.contact_id
       
    )

select * from int_batch_schedule order by batch_id, session_id
          