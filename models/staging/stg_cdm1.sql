select 
    * 
from {{ source('salesforce', 'src_OMR_Assessment__c') }}