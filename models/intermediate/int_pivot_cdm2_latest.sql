SELECT *
From (
  SELECT 
        student_barcode,
        q5_marks,
        q6_marks,
        total_marks_cdm2 as total,
        record_type
  FROM {{ ref('int_cdm2_latest') }}
)
PIVOT (
  max(q5_marks) as q5, max(q6_marks) as q6, max(total) as total_cdm2   
  FOR record_type IN ('Baseline', 'Endline')
)
--where student_barcode = '220042918'