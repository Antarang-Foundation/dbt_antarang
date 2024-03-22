with source as (
    select * from {{ source('salesforce', 'Account') }}
),

renamed as (
    select
        id as donor_id,
        name as donor_name,
        RecordTypeId as record_type_id

    from source
),

recordtypes as (select record_type_id, record_type from {{ ref('seed_recordtype') }}),
    
    stg_donor as (
        select * except (record_type_id)
        from 
            renamed
            left join recordtypes using (record_type_id) where record_type = 'Donor'
    )
    
select *
from stg_donor