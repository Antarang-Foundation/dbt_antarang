{{ config(materialized="table") }}

With source as (
    Select 
        barcode, contact_id, full_name, grade, academic_year, baseline_total, baseline_percentage, endline_total, endline_percentage, percentage_change, showed_improvement, endline_mastered, gender, account_medium_language as school_medium_language, school_district
    From {{ ref('int_student_grade_performance') }}
)
select * from source
--where grade='Grade 12'
--where showed_improvement is not null
--where barcode='2301008570'
--where academic_year='2022'