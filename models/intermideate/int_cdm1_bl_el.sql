with
    pivot as (select * from {{ ref('int_pivot_cdm1') }}),
    contacts as (select student_barcode, concat(first_name, " ", last_name) as student_name from {{ ref('stg_contacts') }}),
    
    

int_cdm1_bl_el as (
    
    select *
    from 
        pivot
        left join contacts using (student_barcode)

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
    (total_endline - total_baseline) as change_in_score

from int_cdm1_bl_el