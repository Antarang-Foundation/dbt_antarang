with
    source as (select * from {{ ref("fct_attendance_assessment") }}),
    overall_correlation as (
        select
            CAST(1 as string) as id,
            count(student_barcode) as total_rows,
            count(distinct student_barcode) as total_students,
            countif(percentage_attendance is not null) as students_with_attendance_data,
            countif(total_cdm1_baseline is not null) as students_with_baseline_scores,
            countif(total_cdm1_endline is not null) as students_with_endline_scores,
            countif(change_in_score is not null) as students_with_change_in_score,
            countif(
                percentage_attendance is not null and change_in_score is not null
            ) as students_with_both_varibles,
            corr(change_in_score, percentage_attendance) as correlation_coefficient
        from source
    )
    
select *
from overall_correlation
