with
    contacts as (select * from {{ ref('stg_facilitator') }}),
    recordtypes as (select record_type_id,record_type from {{ ref('stg_recordtypes') }}),
    int_facilitator as (
        select *
        from 
            contacts
            left join recordtypes using (record_type_id)
    )
    
select *
from int_facilitator
where record_type = 'CA Trainer' 