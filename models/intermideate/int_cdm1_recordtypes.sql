{{ config(materialized="view") }}
select 
    a.*,
    b.*
FROM 
    {{ ref('stg_cdm1') }} AS a
LEFT JOIN 
    {{ ref('stg_recordtypes') }} AS b
USING (record_type_id)