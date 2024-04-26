with 

t0 as (select * from {{ ref('stg_sar') }} where data_cleanup = true and marks_recalculated = true and student_linked = true),

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