with
source as (
    select
        barcode,
        contact_id,
        full_name,
        grade,
        academic_year,
        baseline_total,
        baseline_percentage,
        endline_total,
        endline_percentage,
        percentage_change,
        showed_improvement,
        endline_mastered,
        gender,
        account_medium_language As school_medium_language,
        school_district,
        score_type
    from {{ ref('int_student_grade_performance') }}
)

select *
from source
