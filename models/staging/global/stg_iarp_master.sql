with t1 as 
(
    select id, name as profession_name 
    from {{ source ('salesforce', 'IARP_Master__c') }} where IsDeleted = false
)

select * from t1
