select 
    * 
from {{ source('salesforce', 'src_RecordType') }}