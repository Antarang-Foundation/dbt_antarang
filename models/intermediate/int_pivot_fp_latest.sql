with source as (
  SELECT 
        *
  FROM {{ ref('int_fp_latest') }}
),

pivot as (
  select *
  from source
  PIVOT (
  max(q17_marks) as q17, 
  max(q18_marks) as q18,
  max(q19_marks) as q19,
  max(q20_marks) as q20,
  max(q21_marks) as q21,
  max(q22_marks) as q22
  FOR record_type IN ('Baseline', 'Endline')
)
)
SELECT *
From pivot
--where q16_Baseline is not null and q11_Endline is not null