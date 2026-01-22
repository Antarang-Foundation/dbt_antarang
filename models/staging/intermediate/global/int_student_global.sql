with

    t0 as (select * from {{ ref("int_student") }}),
    
    t1 as (select * from {{ ref("int_global") }}),

    t2 as (select * from t0 full outer join t1 on t0.student_batch_id = t1.batch_id)

    select *,
    Case when(g9_batch_id is not null and student_details_2_grade like '%9%' and batch_grade = 'Grade 9') THEN 'Grade 9'
        when(g10_batch_id is not null and student_details_2_grade like '%10%' and batch_grade = 'Grade 10') THEN 'Grade 10'
        when(g11_batch_id is not null and student_details_2_grade like '%11%' and batch_grade = 'Grade 11') THEN 'Grade 11'
        when(g12_batch_id is not null and student_details_2_grade like '%12%' and batch_grade = 'Grade 12') THEN 'Grade 12' 
        ELSE 'N/A' END AS 
        sd2_grade, 
Case 
    When 
        (Case 
            When(g9_batch_id is not null and student_details_2_grade like '%9%' and batch_grade = 'Grade 9') Then 'Grade 9'
            When(g10_batch_id is not null and student_details_2_grade like '%10%' and batch_grade = 'Grade 10') Then 'Grade 10'
            When(g11_batch_id is not null and student_details_2_grade like '%11%' and batch_grade = 'Grade 11') Then 'Grade 11'
            When(g12_batch_id is not null and student_details_2_grade like '%12%' and batch_grade = 'Grade 12') Then 'Grade 12' 
            Else 'N/A' 
        end) != 'N/A'
    Then student_barcode
    Else null
end as sd2_student_barcode
from t2
    where batch_academic_year>=2023 
    order by student_id, student_grade

