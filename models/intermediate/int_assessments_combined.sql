
with
    cdm2 as (select * from {{ ref('int_pivot_cdm2_raw_latest') }}),
    cdm1 as (select * from {{ ref('int_pivot_cdm1_latest') }}),
    cs as (select * from {{ ref('int_pivot_cs_latest') }}),
    cp as (select * from {{ ref('int_pivot_cp_latest') }}),
    fp as (select * from {{ ref('int_pivot_fp_latest') }}),
    

int_assessments_combined as (
    
    select *
    from 
        cdm1
        left join cdm2 using (student_barcode, academic_year)
        left join cs using (student_barcode, academic_year)
        left join cp using (student_barcode, academic_year)
        left join fp using (student_barcode,academic_year)



)
select 
    student_barcode,
    academic_year,
    total_cdm1_Baseline,
    total_cdm1_Endline,
    cdm2_total_Baseline,
    cdm2_total_Endline,
    cs_total_Baseline,
    cs_total_Endline,
    cp_total_Baseline,
    cp_total_Endline,
    fp_total_Baseline,
    fp_total_Endline,
    7 as cdm1_max_marks,
    13 as cdm2_max_marks,
    10 as cp_max_marks,
    16 as cs_max_marks,
    8.5 as fp_max_marks
    
from int_assessments_combined
--where student_barcode = '2303214005'