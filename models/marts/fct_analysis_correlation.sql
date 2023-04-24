{{ config(materialized="table") }}
with 
source as (
  select * from {{ ref('fct_attendance_assessment') }}
),
remove_null_rows as (
    select * from source
    where change_in_score is not null AND percentage_attendance is not null
),
overall_correlation as (
    select 
        count(student_barcode) as count_of_rows,    
        corr(change_in_score,percentage_attendance) as correlation_coefficient
    from remove_null_rows
)
select * from overall_correlation