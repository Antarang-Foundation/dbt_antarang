select 
    * 
from {{ source('salesforce', 'src_Session__c') }}