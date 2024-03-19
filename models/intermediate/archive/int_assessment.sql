with
    pivot as (select * from {{ ref('int_cdm1_pivot') }}),
    contacts as (select student_barcode, full_name as student_full_name from {{ ref('stg_student') }}),
    
int_assessment as (
    select *
    from 
        pivot
        left join contacts using (student_barcode)
)

select 
    student_barcode,
    student_full_name,
    q1_baseline,
    q2_baseline,
    q3_baseline,
    q4_baseline,
    total_cdm1_baseline,
    q1_endline,
    q2_endline,
    q3_endline,
    q4_endline,
    total_cdm1_endline,
    (total_cdm1_endline - total_cdm1_baseline) as change_in_score

from int_assessment
order by student_barcode