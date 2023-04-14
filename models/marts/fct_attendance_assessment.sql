with
    assessment as (select * from {{ ref('int_cdm1_bl_el') }}),
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
    corr(change_in_score, percentage_attendance) as correlation_coefficient
    
from attendance_assessment
--WHERE correlation_coefficient IS NOT NULL
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