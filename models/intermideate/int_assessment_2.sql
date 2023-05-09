with
    pivot as (select * from {{ ref('int_cdm1_cdm2') }}),
    contacts as (select student_barcode, full_name as student_full_name from {{ ref('stg_contacts') }}),
    
int_assessment_2 as (
   
    select *
    from 
        pivot
        left join contacts using (student_barcode)
        
)

select 
    student_barcode,
    student_full_name,
    q5_baseline,
    q6_baseline,
    total_cdm2_baseline,
    q5_endline,
    q6_endline,
    total_cdm2_endline,
    (total_cdm2_endline - total_cdm2_baseline) as change_in_score

from int_assessment_2
order by student_barcode