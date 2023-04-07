select 
    Id,
    FirstName,
    LastName,
    Bar_Code__c,
    Contact_Type__c
from {{ source('salesforce', 'src_Contact') }}