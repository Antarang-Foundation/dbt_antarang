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
            AS showed_improvement,
            CASE 
                WHEN endline_percentage>=0.80 THEN "Mastered"
                WHEN endline_percentage is null THEN null
                ELSE "Not Mastered"
            END
            AS endline_mastered,
            CASE
                WHEN baseline_total IS NULL AND endline_total IS NULL THEN "None"
                WHEN baseline_total IS NULL AND endline_total IS NOT NULL THEN "Only Endline"
                WHEN baseline_total IS NOT NULL AND endline_total IS NULL THEN "Only Baseline"
                WHEN baseline_total IS NOT NULL AND endline_total IS NOT NULL THEN "Both"
                ELSE null
            END
            AS score_type,    
            FROM add_percentages
    )

select * from add_metrics
where student_barcode is not null