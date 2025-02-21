with t1 as 

(select *,

count(distinct batch_no) OVER (PARTITION BY school_id) `school_no_of_batches`,
count(distinct case when batch_expected_sessions > 0 and batch_expected_sessions = batch_completed_sessions then batch_no end) OVER (PARTITION BY school_id) `school_batches_completing_program`
from {{ref('int_student_global_session_combined')}}),

t2 as(
select student_barcode, student_name, batch_no, batch_donor, school_partner, school_state, school_district, school_taluka, school_name, school_area, facilitator_name, batch_academic_year, batch_grade, 
batch_language, 

max(school_batches_completing_program) `school_batches_completing_program`,
max(school_no_of_batches) `school_no_of_batches`,

max(batch_expected_sessions) `student_batch_expected_sessions`,
max(batch_expected_student_type_session) `expected_student_type_session`,
max(batch_expected_parent_type_session) `expected_parent_type_session`,
max(batch_expected_flexible_type_session) `expected_flexible_type_session`,
max(batch_expected_counseling_type_session) `expected_counseling_type_session`,

max(batch_scheduled_sessions) `student_batch_scheduled_sessions`,
max(batch_student_type_scheduled_sessions) `student_type_scheduled_sessions`,
max(batch_parent_type_scheduled_sessions) `parent_type_scheduled_sessions`,
max(batch_flexible_type_scheduled_sessions) `flexible_type_scheduled_sessions`,
max(batch_counseling_type_scheduled_sessions) `counseling_type_scheduled_sessions`,

max(batch_completed_sessions) `student_batch_completed_sessions`,
max(batch_student_type_completed_sessions) `student_type_completed_sessions`,
max(batch_parent_type_completed_sessions) `parent_type_completed_sessions`,
max(batch_flexible_type_completed_sessions) `flexible_type_completed_sessions`,
max(batch_Counseling_type_completed_sessions) `Counseling_type_completed_sessions`,

max(batch_max_student_session_attendance) `student_batch_max_session_attendance`,

count(distinct case when session_date is not null and total_student_present > 0 and attendance_status = 'Present' then session_code end) `student_present_sessions`,
count(distinct case when session_date is not null and total_student_present > 0 and attendance_status = 'Absent' then session_code end) `student_absent_sessions`,
count(distinct case when session_date is not null and total_student_present > 0 and attendance_status = 'Present' and session_type = 'Student' then session_code end) `stud_present_student_type_sessions`,
count(distinct case when session_date is not null and total_student_present > 0 and attendance_status = 'Present' and session_type = 'Parent' then session_code end) `stud_present_parent_type_sessions`,
count(distinct case when session_date is not null and total_student_present > 0 and attendance_status = 'Present' and session_type = 'Flexible' then session_code end) `stud_present_flexible_type_sessions`,
count(distinct case when session_date is not null and total_student_present > 0 and attendance_status = 'Present' and session_type = 'Counseling' then session_code end) `stud_present_counseling_type_sessions`

FROM t1
group by student_barcode, student_name, batch_no, batch_donor, school_partner, school_state, school_district, school_taluka, school_name, school_area, facilitator_name, batch_academic_year, batch_grade, session_grade, batch_language
)

select * from t2 




