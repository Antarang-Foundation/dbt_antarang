with 

t0 as (select cp_id, assessment_barcode, record_type, created_on, created_from_form, cp_no, is_non_null, assessment_grade, 
assessment_academic_year, assessment_batch_id, q7, q7_marks, q8, q8_marks, q9_1, q9_1_marks, q9_2, q9_2_marks, q9_3, q9_3_marks, 
q9_4, q9_4_marks, q9_5, q9_5_marks, q9_6, q9_6_marks, q9_7, q9_7_marks, q10, q10_marks, cp_total_marks, error_status, data_cleanup, 
marks_recalculated, student_linked, q8_null, q8_bucket from {{ ref('stg_cp') }} 
where data_cleanup = true and marks_recalculated = true and student_linked = true),

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
