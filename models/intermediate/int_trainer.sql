with
    contacts as (select * from {{ ref('stg_trainer') }}),
    recordtypes as (select record_type_id,record_type from {{ ref('stg_recordtypes') }}),
    int_trainer as (
        select *
        from 
            contacts
            left join recordtypes using (record_type_id)
    )
    
select *
from int_trainer
where record_type = 'CA Trainer' 