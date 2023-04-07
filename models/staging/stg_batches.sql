select 
    * 
from {{ source('salesforce', 'src_Batch__c') }}