with source as (
  SELECT 
        *
  FROM {{ ref('int_cs_latest') }}
),

pivot as (
  select *
  from source
  PIVOT (
  max(q11_marks) as q11, 
  max(q12_marks) as q12,
  max(q13_marks) as q13,
  max(q14_marks) as q14,
  max(q15_marks) as q15,
  max(q16_marks) as q16
  FOR record_type IN ('Baseline', 'Endline')
)
)
SELECT *
From pivot
--where q16_Baseline is not null and q11_Endline is not null