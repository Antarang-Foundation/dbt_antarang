with 

t0 as (select cs_id, assessment_barcode, record_type, created_on, created_from_form, cs_no, is_non_null, assessment_grade,
assessment_academic_year, assessment_batch_id, q11_1, q11_2, q11_3, q11_4, q11_5, q11_6, q11_7, q11_8, q11_9, q11_marks,
q12_1, q12_2, q12_3, q12_4, q12_marks, q13, q13_marks, q14, q14_marks, q15_1, q15_2, q15_3, q15_4, q15_5, q15_6, q15_7,
q15_8, q15_9, q15_marks, q16, q16_marks, cs_total_marks, error_status, data_cleanup, marks_recalculated,
student_linked from {{ ref('stg_cs') }} where data_cleanup = true and marks_recalculated = true and student_linked = true),

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