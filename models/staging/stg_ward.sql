with source as (
    select * from {{ source('salesforce', 'Ward_Master__c') }}
),

renamed as (
    select 
               Id as ward_id,
               Name as ward_name
    from source 

)

select * from renamed