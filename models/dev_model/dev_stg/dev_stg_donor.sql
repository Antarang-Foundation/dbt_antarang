with source as (
    select id as donor_id,
        name as donor_name,
        RecordTypeId as record_type_id
    from {{ source('salesforce', 'Account') }} 
    where IsDeleted = false and lower(name) not like '%test%' -- filters out test/test entries case-insensitively
),
    
recordtypes as (
    select record_type_id, record_type 
    from {{ ref('seed_recordtype') }} 
    where record_type = 'Donor'
),

dev_stg_donor as (
    select 
        a.donor_id, 
        a.donor_name, 
        b.record_type
    from source a
    inner join recordtypes b ON a.record_type_id = b.record_type_id 
)

select *
from dev_stg_donor