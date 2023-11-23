SELECT 
  *
FROM (
  SELECT 
    *,
    ROW_NUMBER() OVER (
      PARTITION BY student_barcode, record_type 
      ORDER BY created_on DESC
    ) AS latest_record
  FROM {{ ref('int_fp_recordtypes') }}
) t
WHERE latest_record = 1