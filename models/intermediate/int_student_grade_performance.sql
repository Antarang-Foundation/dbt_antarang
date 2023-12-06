with
    assessment as (select * from {{ ref('int_assessments_combined') }}),
    student_database as (select * from {{ ref('int_student_database') }}),

joined as (
    select
        * 
    from student_database
    left join assessment on student_database.barcode=assessment.student_barcode
),
add_totals as (
    select 
        *,
        CASE
        WHEN grade='Grade 9' AND total_cdm1_Baseline is not null AND  cdm2_total_Baseline is not null AND cp_total_Baseline is not null  THEN (total_cdm1_Baseline+cdm2_total_Baseline+cp_total_Baseline)
        WHEN grade='Grade 10' AND total_cdm1_Baseline is not null AND  cdm2_total_Baseline is not null AND cp_total_Baseline is not null  THEN (total_cdm1_Baseline+cdm2_total_Baseline+cp_total_Baseline)
        WHEN grade='Grade 11' AND total_cdm1_Baseline is not null AND  cs_total_Baseline is not null AND fp_total_Baseline is not null  THEN (total_cdm1_Baseline+cs_total_Baseline+fp_total_Baseline)
        WHEN grade='Grade 12' AND total_cdm1_Baseline is not null AND  cs_total_Baseline is not null AND fp_total_Baseline is not null  THEN (total_cdm1_Baseline+cs_total_Baseline+fp_total_Baseline)
        ELSE null
        END as baseline_total,
        CASE
        WHEN grade='Grade 9' AND total_cdm1_Endline is not null AND cdm2_total_Endline is not null AND cp_total_Endline is not null THEN (total_cdm1_Endline+cdm2_total_Endline+cp_total_Endline)
        WHEN grade='Grade 10' AND total_cdm1_Endline is not null AND cdm2_total_Endline is not null AND cp_total_Endline is not null THEN (total_cdm1_Endline+cdm2_total_Endline+cp_total_Endline)
        WHEN grade='Grade 11' AND total_cdm1_Endline is not null AND cs_total_Endline is not null AND fp_total_Endline is not null THEN (total_cdm1_Endline+cs_total_Endline+fp_total_Endline)
        WHEN grade='Grade 12' AND total_cdm1_Endline is not null AND cs_total_Endline is not null AND fp_total_Endline is not null THEN (total_cdm1_Endline+cs_total_Endline+fp_total_Endline)
        ELSE null
        END as endline_total,
        CASE
        WHEN grade='Grade 9' THEN (cdm1_max_marks+cdm2_max_marks+cp_max_marks)
        WHEN grade='Grade 10' THEN (cdm1_max_marks+cdm2_max_marks+cp_max_marks)
        WHEN grade='Grade 12' THEN (cdm1_max_marks+cs_max_marks+fp_max_marks)
        WHEN grade='Grade 11' THEN (cdm1_max_marks+cs_max_marks+fp_max_marks)
        ELSE null
        END as endline_maximum,
        CASE
        WHEN grade='Grade 9' THEN (cdm1_max_marks+cdm2_max_marks+cp_max_marks)
        WHEN grade='Grade 10' THEN (cdm1_max_marks+cdm2_max_marks+cp_max_marks)
        WHEN grade='Grade 11' THEN (cdm1_max_marks+cs_max_marks+fp_max_marks)
        WHEN grade='Grade 12' THEN (cdm1_max_marks+cs_max_marks+fp_max_marks)
        ELSE null
        END as baseline_maximum

        --(total_cdm1_Baseline/cdm1_max_marks) as cdm1_bl_percentage , 
        --(total_cdm1_Endline/cdm1_max_marks) as cdm1_el_percentage,
        --(cdm2_total_Baseline/cdm2_max_marks) as cdm2_bl_percentage, 
        --(cdm2_total_Endline/cdm2_max_marks) as cdm2_el_percentage,
        --(cs_total_Baseline/cs_max_marks) as cs_bl_percentage, 
        --(cs_total_Endline/cs_max_marks) as cs_el_percentage,
        --(cp_total_Baseline/cp_max_marks) as cp_bl_percentage, 
        --(cp_total_Endline/cp_max_marks) as cp_el_percentage,
        --(fp_total_Baseline/fp_max_marks) as fp_bl_percentage, 
        --(fp_total_Endline/fp_max_marks) as fp_el_percentage,
        
    from joined
),
add_percentages as (
    select 
        *,
        baseline_total/baseline_maximum as baseline_percentage,
        endline_total/endline_maximum as endline_percentage
        from add_totals
),
add_metrics as (
    select 
        *,
        (endline_percentage-baseline_percentage) as percentage_change,
        CASE 
        WHEN endline_percentage is null THEN null
        WHEN baseline_percentage is null THEN null
        WHEN (endline_percentage-baseline_percentage)>0 THEN "Yes"
        WHEN (endline_percentage-baseline_percentage)=0 THEN "No Improvement"
        ELSE "Decreased"
        END
        as showed_improvement,
        CASE 
        WHEN endline_percentage>=0.80 THEN "Mastered"
        WHEN endline_percentage is null THEN null
        ELSE "Not Mastered"
        END
        AS endline_mastered,
        FROM add_percentages
)
select * from add_metrics
--where student_barcode is not null
--where barcode='220047875'