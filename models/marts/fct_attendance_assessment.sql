with
    assessment as (select * from {{ ref('int_assessment') }}),
    attendance as (select * from {{ ref('int_attendance') }}),
    

attendance_assessment as (
    
    select *
    from 
        assessment
        left join attendance using (student_barcode)

)
select 
    student_barcode,
    student_name,
    q1_baseline,
    q1_endline,
    q2_baseline,
    q2_endline,
    q3_baseline,
    q3_endline,
    q4_baseline,
    q4_endline,
    total_baseline,
    total_endline,
    change_in_score,
    attendance_count,
    total_sessions,
    percentage_attendance,
    corr(IFNULL(change_in_score,0),IFNULL(percentage_attendance,0)) as correlation_coefficient
    
from attendance_assessment
--where student_barcode = '220042918'--WHERE correlation_coefficient IS NOT NULL
group by student_barcode,
    student_name,
    q1_baseline,
    q1_endline,
    q2_baseline,
    q2_endline,
    q3_baseline,
    q3_endline,
    q4_baseline,
    q4_endline,
    total_baseline,
    total_endline,
    change_in_score,
    attendance_count,
    total_sessions,
    percentage_attendance