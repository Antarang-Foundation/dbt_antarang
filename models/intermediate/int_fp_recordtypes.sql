with
    fp as (select * from {{ ref('stg_fp') }}),
    recordtypes as (select record_type_id,record_type from {{ ref('stg_recordtypes') }}),
    int_cdm1_recordtypes as (
        select *
        from 
            fp
            left join recordtypes using (record_type_id)
    )

select *
from int_cdm1_recordtypes