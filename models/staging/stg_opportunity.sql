with source as (
    select * from {{ source('salesforce', 'Opportunity__c') }}
),

t1 as (
   select
   id as opportunit_id,
   Name as oppprunity_name, 
   CreatedDate,
   Industry_name__c as industry_name
  from source where IsDeleted = false
)

select * from t1
