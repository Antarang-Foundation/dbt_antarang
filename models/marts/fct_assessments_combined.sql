with
    assessment as (select * from {{ ref('int_assessments_combined') }}),
    student_database as (select * from {{ ref('int_student_database') }}),

joined as (
    select
        * 
    from student_database
    left join assessment using (student_barcode) 
),
add_columns as (
    select 
        *,
        (total_cdm1_Baseline/cdm1_max_marks) as cdm1_bl_percentage, 
        (total_cdm1_Endline/cdm1_max_marks) as cdm1_el_percentage,
        (cdm2_total_Baseline/cdm2_max_marks) as cdm2_bl_percentage, 
        (cdm2_total_Endline/cdm2_max_marks) as cdm2_el_percentage,
        (cs_total_Baseline/cs_max_marks) as cs_bl_percentage, 
        (cs_total_Endline/cs_max_marks) as cs_el_percentage,
        (cp_total_Baseline/cp_max_marks) as cp_bl_percentage, 
        (cp_total_Endline/cp_max_marks) as cp_el_percentage,
        (fp_total_Baseline/fp_max_marks) as fp_bl_percentage, 
        (fp_total_Endline/fp_max_marks) as fp_el_percentage,
        
    from joined
)
select * from add_columns
where student_barcode='2303214005'