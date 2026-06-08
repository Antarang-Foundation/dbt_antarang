with 

t0 as (select fp_id, assessment_barcode, record_type, created_on, created_from_form, fp_no, is_non_null, assessment_grade, 
assessment_academic_year, assessment_batch_id, q17, q17_marks, q18_1, q18_2, q18_3, q18_4, q18_5, q18_6, q18_7, q18_8, q18_9, 
q18_10, q18_11, q18_marks, q19, q19_marks, q20, q20_marks, q21, q21_marks, q22, q22_marks, fp_total_marks, f1, f2, f3, f4, f5, 
f6, f7, f8, f9, f10, f11, f12, error_status, data_cleanup, marks_recalculated, student_linked, q21_null, q21_bucket 
from {{ ref('stg_fp') }} where data_cleanup = true and marks_recalculated = true and student_linked = true),

t1 as (
  SELECT 
    *,
    ROW_NUMBER() OVER (
      PARTITION BY assessment_barcode, record_type 
      ORDER BY created_on DESC
    ) AS is_latest
  FROM t0
),

t2 as (
    
    select * except (error_status, data_cleanup, marks_recalculated, student_linked, is_latest) from t1 
    where is_latest = 1
    order by assessment_barcode, record_type)

select * from t2