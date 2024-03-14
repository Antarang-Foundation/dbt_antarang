with source as (
    select * from {{ source('salesforce', 'State_Govt_Body_Level__c') }}
),

renamed as (
    select 
               Id as state_id,
               Name as state_name,
               State_Code__c as state_code 
    from source 

)

select * from renamed
