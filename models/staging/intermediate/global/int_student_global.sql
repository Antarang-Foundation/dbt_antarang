with

    t0 as (select * from {{ ref("int_student") }}),
    
    t1 as (select * from {{ ref("int_global") }}),

    t2 as (select * from t0 full outer join t1 on t0.student_batch_id = t1.batch_id),

    t3 as (select *,
    CASE
    WHEN student_details_2_grade IS NULL THEN 'N/A'
    WHEN REGEXP_CONTAINS(student_details_2_grade, r'(9|IX)')
         THEN 'Grade 9'
    WHEN REGEXP_CONTAINS(student_details_2_grade, r'(10|X)')
         THEN 'Grade 10'
    WHEN REGEXP_CONTAINS(student_details_2_grade, r'(11|XI)')
         THEN 'Grade 11'
    WHEN REGEXP_CONTAINS(student_details_2_grade, r'(12|XII)')
         THEN 'Grade 12'
    ELSE 'N/A'
END AS sd2_grade
from t2
),

t4 as (select *,
CASE 
    WHEN sd2_grade != 'N/A'
    THEN student_barcode
    ELSE NULL
END AS sd2_barcode
from t3
    where batch_academic_year>=2023 
    order by student_id, student_grade
)

select * from t4
