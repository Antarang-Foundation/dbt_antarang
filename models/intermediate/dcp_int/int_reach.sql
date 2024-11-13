WITH 
t1 AS (
    SELECT 
        student_id, first_barcode, student_grade, student_barcode, student_batch_id, gender, birth_year, 
        batch_no, batch_academic_year, batch_grade, batch_language, no_of_students_facilitated, fac_start_date, facilitator_name, facilitator_id,
        school_id, school_name, school_state, school_district, school_taluka, school_partner, school_area, batch_donor
    FROM {{ref('fct_student_global')}}
),

t2 AS (
    SELECT 
        batch_no AS session_batch, 
        batch_session_type_based_avg_overall_attendance as batch_max_overall_attendance,
        total_reached_parents
    FROM {{ref("fct_global_session")}}
),

t3 AS (
    --this is a join table contains student + session details
    SELECT * 
    FROM t1 
    FULL OUTER JOIN t2 
    ON t1.batch_no = t2.session_batch
),

t4 as (
    select stud_batch_no,
    student_barcode as student_barcode_att,
    total_student_session_att
    from  {{ref("int_student_attendace")}}  
), 


t5 as (
     SELECT * 
    FROM t3
    FULL OUTER JOIN t4
    ON t3.batch_no = t4.stud_batch_no
),

t6 AS (
    SELECT 
        t5.*,
        CASE
            WHEN batch_academic_year <= 2022 THEN no_of_students_facilitated
            WHEN batch_academic_year = 2023 THEN batch_max_overall_attendance
            WHEN batch_academic_year >= 2024 THEN total_student_session_att
            ELSE 0
        END AS total_reached_students
    FROM t5
)

SELECT * FROM t6

