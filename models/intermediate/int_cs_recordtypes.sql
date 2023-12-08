with
    cs as (select * from {{ ref('stg_cs') }}),
    recordtypes as (select record_type_id,record_type from {{ ref('stg_recordtypes') }}),
    int_cdm1_recordtypes as (
        select *
        from 
            cs
            left join recordtypes using (record_type_id)
    )
    
select *
from int_cdm1_recordtypes