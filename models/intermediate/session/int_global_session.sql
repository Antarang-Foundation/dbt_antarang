/* 
with 
t1 as (select * from {{ref('int_global')}}),
t2 as (select * from {{ref('stg_session')}}),
t3 as (select * from t1 full outer join t2 on t1.batch_id = t2.session_batch_id order by batch_id, session_id)
select * from t3
*/

with 

t1 as (select * from {{ref('int_global')}}),
t2 as (select * from {{ref('stg_session')}}),
t3 as (select * from t1 full outer join t2 on t1.batch_id = t2.session_batch_id order by batch_id, session_id),
t4 AS (
    SELECT t3.*,
        (CASE
            WHEN t3.session_mode = 'Any' AND t3.school_district = 'RJ Model B' 
            THEN 'HW Session'
            ELSE t3.session_type
        END)updated_session_type
    FROM t3
)

SELECT 
batch_id, batch_no, batch_academic_year, batch_grade,batch_language, no_of_students_facilitated, fac_start_date, fac_end_date,
allocation_email_sent, batch_facilitator_id, facilitator_id, facilitator_name, facilitator_email, batch_school_id,school_id,
school_name, school_taluka, school_ward, school_district, school_state, school_academic_year, school_language,
enrolled_g9, enrolled_g10, enrolled_g11, enrolled_g12, tagged_for_counselling, school_partner,school_area,batch_donor_id,donor_id,batch_donor,
session_id,session_batch_id,session_facilitator_id,session_code,session_name,session_date,session_start_time,
session_grade,session_no,omr_required,omrs_received,total_student_present,total_parent_present,log_reason,attendance_submitted,
present_count,attendance_count,payment_status,deferred_reason,invoice_date,session_amount,no_of_sessions_no_of_units,total_amount,parent_present_count,
session_mode,batch_expected_sessions,batch_scheduled_sessions,batch_completed_sessions,batch_max_student_session_attendance,batch_max_session_parent_attendance,
batch_max_session_flexible_attendance,batch_max_session_counseling_attendance,batch_indi_stud_attendance,batch_indi_parent_attendance,batch_indi_flexible_attendance,
batch_indi_counseling_attendance,batch_session_type_based_avg_overall_attendance,batch_max_overall_attendance,total_reached_parents,
session_type as previous_session_type, updated_session_type as session_type
FROM t4

