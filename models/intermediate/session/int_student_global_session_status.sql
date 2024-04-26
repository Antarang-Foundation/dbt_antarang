with t1 as 

(select *,

count(distinct batch_no) OVER (PARTITION BY school_id) `school_no_of_batches`,
count(distinct case when batch_expected_sessions > 0 and batch_expected_sessions = batch_completed_sessions then batch_no end) OVER (PARTITION BY school_id) `school_batches_completing_program`,

from {{ref('int_student_global_session_combined')}}),

t2 as

(select student_barcode, student_name, batch_no, batch_donor, school_partner, school_state, school_district, school_ward, school_taluka, school_name, facilitator_name, batch_academic_year, batch_grade, session_grade, batch_language, 

max(school_batches_completing_program) `school_batches_completing_program`,
max(school_no_of_batches) `school_no_of_batches`,

max(batch_expected_sessions) `student_batch_expected_sessions`,
max(batch_expected_sessions) `student_batch_scheduled_sessions`,
max(batch_completed_sessions) `student_batch_completed_sessions`,
max(batch_max_session_attendance) `student_batch_max_session_attendance`,

count(distinct case when session_date is not null and total_student_present > 0 and attendance_status = 'Present' then session_code end) `student_present_sessions`,

FROM t1

group by student_barcode, student_name, batch_no, batch_donor, school_partner, school_state, school_district, school_ward, school_taluka, school_name, facilitator_name, batch_academic_year, batch_grade, session_grade, batch_language)

select * from t2 where student_barcode is not null