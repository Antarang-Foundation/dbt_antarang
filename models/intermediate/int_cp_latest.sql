with 

t0 as (select * from {{ ref('stg_cp') }} where error_status = 'No Error' and data_cleanup = true and marks_recalculated = true),

t1 as (
  SELECT 
    *,
    ROW_NUMBER() OVER (
      PARTITION BY barcode, record_type 
      ORDER BY created_on DESC
    ) AS is_latest
  FROM t0
),

t2 as (
    
    select * from t1 
    where is_latest = 1
    order by barcode, record_type)

select * from t2
