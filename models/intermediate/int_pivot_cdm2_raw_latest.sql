with source as (
  SELECT 
          student_barcode,
          q5_marks,
          q6,
          cdm2_total,
          record_type,
          academic_year
  FROM {{ ref('int_cdm2_raw_latest') }}
),

pivot as (
  select *
  from source
  PIVOT (
   max(q5_marks) as q5, max(q6) as q6, max(cdm2_total) as cdm2_total 
  FOR record_type IN ('Baseline', 'Endline')
)
)
SELECT *
From pivot

--where student_barcode = '2303007083'