SELECT 
  *
FROM (
  SELECT 
    *,
    ROW_NUMBER() OVER (
      PARTITION BY student_barcode, record_type 
      ORDER BY created_on DESC
    ) AS latest_record
  FROM {{ ref('int_cdm1_recordtypes') }}
) t
WHERE latest_record = 1 --and student_barcode = '220042918'