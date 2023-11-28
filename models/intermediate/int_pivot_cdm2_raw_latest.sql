with source as (
  SELECT 
        *
  FROM {{ ref('int_cdm2_raw_latest') }}
),

pivot as (
  select *
  from source
  PIVOT (
  max(q5_marks) as q5, max(q6) as q6   
  FOR record_type IN ('Baseline', 'Endline')
)
)
SELECT *
From pivot

--where student_barcode = '220042918'