WITH t1 AS (
    SELECT 
        student_id, 
        first_barcode, g9_barcode, g10_barcode, g11_barcode, g12_barcode

    FROM {{ref('stg_student')}}
),

t2 AS (
    SELECT 
        attendance_student_id, 
        attendance_status,
        guardian_attendance,
        session_att_grade
    FROM {{ref('stg_attendance')}} 
    WHERE attendance_status = 'Present'
),

t3 AS (
    SELECT * FROM  t1 INNER JOIN t2 ON t1.student_id = t2.attendance_student_id
),

t4 as (
    select student_id, attendance_student_id, session_att_grade, 
    count(*) as total_student_session_att
    from t3 
    group by student_id, attendance_student_id, session_att_grade
),


t5 as (
    select student_id as stud_id, 
    student_grade as stud_grade, 
    student_barcode, 
    batch_no as stud_batch_no
    from {{ref('int_student_global')}}
),

t6 as (
       select * from t4 inner join t5 on t4.student_id = t5.stud_id and t4.session_att_grade = t5.stud_grade
)

select * from t6

