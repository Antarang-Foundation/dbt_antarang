with t1 as (
    select student_id, student_barcode, gender, batch_no, total_stud_have_report
    from {{ref('fct_studentwise_demographic')}}
),

t2 as (select 
    session_batch_id, 
    batch_no as session_batch_no,
    batch_academic_year, batch_grade, batch_language, batch_donor, facilitator_id, facilitator_name,
    school_name, school_id, school_state, school_district, school_taluka, school_partner, school_area,
    session_id, session_type, session_no, attendance_submitted, 
    batch_expected_sessions, batch_expected_student_type_session, batch_scheduled_sessions, batch_completed_sessions
    from {{ref('int_global_session')}}
),

t3 as (
    select * from t1 Inner join t2 on t1.batch_no = t2.session_batch_no
),

t4 as (
    select attendance_student_id, attendance_session_id, attendance_no, 
    attendance_status, guardian_attendance
    from {{ref('stg_attendance')}}
),

t5 as (
    select * from t3 left join t4 on t3.student_id = t4.attendance_student_id and t3.session_id = t4.attendance_session_id
),

t6 AS (
    SELECT *,
        COUNT(CASE WHEN attendance_status = 'Present' THEN 1 END) OVER (PARTITION BY student_barcode, batch_no) AS `total_student_session_attended`,
        COUNT(CASE WHEN attendance_status = 'Present' AND session_type = 'Student' THEN 1 END) OVER (PARTITION BY student_barcode, batch_no) AS `total_attended_student_type_session`,
        COUNT(CASE WHEN attendance_status = 'Present' AND session_type = 'Parent' THEN 1 END) OVER (PARTITION BY student_barcode, batch_no) AS `total_attended_parent_type_session`,
        COUNT(CASE WHEN attendance_status = 'Present' AND session_type = 'Counseling' THEN 1 END) OVER (PARTITION BY student_barcode, batch_no) AS `total_attended_counseling_type_session`,
        COUNT(CASE WHEN guardian_attendance = 'Present' AND session_type = 'Parent' THEN 1 END) OVER (PARTITION BY student_barcode, batch_no) AS `total_parent_attended_parent_type_session`,
        COUNT(CASE WHEN guardian_attendance = 'Present' AND session_type = 'Counseling' THEN 1 END) OVER (PARTITION BY student_barcode, batch_no) AS `total_parent_attended_Counseling_type_session`
    FROM t5
),

t7 as (
    select *,
    Case when batch_expected_student_type_session = total_attended_student_type_session THEN 1 ELSE 0 end as student_completed_program,
    case when (total_attended_student_type_session / batch_expected_student_type_session) >=0 AND (total_attended_student_type_session/batch_expected_student_type_session)<0.26 then '0% to 25% Attended'
         when (total_attended_student_type_session / batch_expected_student_type_session)>0.25 AND (total_attended_student_type_session / batch_expected_student_type_session)<0.51 then '26% to 50% Attended'
         when (total_attended_student_type_session / batch_expected_student_type_session)>0.50 AND (total_attended_student_type_session / batch_expected_student_type_session)<0.76 then '51% to 75% Attended'
         when (total_attended_student_type_session / batch_expected_student_type_session)>0.75 AND (total_attended_student_type_session / batch_expected_student_type_session)<=1 then '75% to 100% Attended'
    END `category_of_percentage_of_session_attended`
    FROM t6
)

SELECT * FROM t7


