SELECT 
  *
FROM (
  SELECT 
    *,
    ROW_NUMBER() OVER (
      PARTITION BY student_barcode, record_type 
      ORDER BY created_on DESC
    ) AS latest_record
  FROM {{ ref('int_cdm2_raw_recordtypes') }}
) t
WHERE latest_record = 1