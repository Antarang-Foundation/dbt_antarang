with source as (
    select * from {{ source('salesforce', 'District_Master__c') }}
),

renamed as (
    select 
               Id as district_id,
               Name as district_name
    from source 

)

select * from renamed