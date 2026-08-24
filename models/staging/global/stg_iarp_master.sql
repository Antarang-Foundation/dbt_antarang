with t1 as 
(
    select id, name as profession_name 
    from {{ source ('salesforce', 'IARP_Master__c') }} where IsDeleted = false and lower(name) not like '%test%'
)

select * from t1
