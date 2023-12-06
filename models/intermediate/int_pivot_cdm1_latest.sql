with source as (
  SELECT 
        student_barcode,
        q1_career_plan_marks as q1,
        q2_interest_marks as q2,
        q3_aptitude_marks as q3,
        q4_career_choice_marks as q4,
        total_marks as total,
        record_type,
        academic_year
  FROM {{ ref('int_cdm1_latest') }}
),

pivot as (
  select *
  from source
  PIVOT (
  max(q1) as q1, max(q2) as q2, max(q3) as q3, max(q4) as q4, max(total) as total_cdm1   
  FOR record_type IN ('Baseline', 'Endline')
)
)
SELECT *
From pivot

--where student_barcode = '220018166'