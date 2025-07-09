with 

t0 as (select * from {{ ref('dev_stg_sar') }} where data_cleanup = true and marks_recalculated = true and student_linked = true),

t1 as (
  SELECT 
    t0.sar_id, t0.assessment_barcode, t0.record_type, t0.created_on, t0.created_from_form, t0.sar_no, t0.is_non_null, t0.sar_atleast_one_quiz, t0.sar_atleast_one_reality,
    t0.assessment_grade, t0.assessment_academic_year, t0.assessment_batch_id, t0.q2_submitted, t0.reality_submitted, t0.sar_q1, t0.sar_q1_marks, t0.sar_q2,
    t0.sar_q2_marks, t0.sar_q3, t0.sar_q3_marks, t0.sar_q4, t0.sar_q4_marks, t0.sar_q5, t0.sar_q5_marks, t0.r1s1, t0.r1s1_marks, t0.r2s2, t0.r2s2_marks, t0.r3s3, t0.r3s3_marks,
    t0.r4s4, t0.r4s4_marks, t0.r5f1, t0.r5f1_marks, t0.r6f2, t0.r6f2_marks, t0.r7f3, t0.r7f3_marks, t0.r8f4, t0.r8f4_marks,
    ROW_NUMBER() OVER (
      PARTITION BY assessment_barcode, record_type 
      ORDER BY created_on DESC
    ) AS is_latest
  FROM t0
)
   -- can we use row number() in above source 'check' for performance improvement
select * from t1 
where is_latest = 1
order by assessment_barcode, record_type
