with source as (
  SELECT 
        student_barcode,
        q7_marks,
        q8_marks,
        q9_1_marks,
        q9_2_marks,
        q9_3_marks,
        q9_4_marks,
        q9_5_marks,
        q9_6_marks,
        q9_7_marks,
        q10_marks,
        cp_total,
        record_type,
        academic_year
  FROM {{ ref('int_cp_latest') }}
),

pivot as (
  select *
  from source
  PIVOT (
  max(q7_marks) as q7, max(q8_marks) as q8,
  max(q9_1_marks) as q9_1,
  max(q9_2_marks) as q9_2,
  max(q9_3_marks) as q9_3,
  max(q9_4_marks) as q9_4,
  max(q9_5_marks) as q9_5,
  max(q9_6_marks) as q9_6,
  max(q9_7_marks) as q9_7,
  max(q10_marks) as q10,
  max(cp_total) as cp_total
  FOR record_type IN ('Baseline', 'Endline')
)
)
SELECT *
From pivot
--where student_barcode='2303214005'
