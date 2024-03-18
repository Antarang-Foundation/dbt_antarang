with
    cdm2 as (select * from {{ ref('stg_cdm2') }}),
    recordtypes as (select record_type_id,record_type from {{ ref('stg_recordtypes') }}),
    int_cdm1_recordtypes as (
        select *
        from 
            cdm2
            left join recordtypes using (record_type_id)
    )

select *
from int_cdm1_recordtypes