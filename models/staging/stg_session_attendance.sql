select 
    * 
from {{ source('salesforce', 'src_Session_Attendance__c') }}