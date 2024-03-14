with source as (
    select * from {{ source('salesforce', 'City_Master__c') }}
),

renamed as (
    select 
               Id as taluka_id,
               Name as taluka_name
    from source 

)

select * from renamed