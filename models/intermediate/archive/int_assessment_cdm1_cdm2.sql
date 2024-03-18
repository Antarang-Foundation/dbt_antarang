with
    pivot as (select * from {{ ref('int_cdm1_cdm2') }}),
    contacts as (select student_barcode, full_name as student_full_name from {{ ref('stg_student') }}),
    int_assessment_2 as (
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
    q5_baseline,
    q6_baseline,
    (total_cdm1_baseline + total_cdm2_baseline) as total_baseline_cdm1_cdm2,
    q1_endline,
    q2_endline,
    q3_endline,
    q4_endline,
    q5_endline,
    q6_endline,
    (total_cdm1_endline + total_cdm2_endline) as total_endline_cdm1_cdm2,
    ((total_cdm1_endline + total_cdm2_endline) - (total_cdm1_baseline + total_cdm2_baseline)) as change_in_score
from int_assessment_2