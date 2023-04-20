with source as (

    select * from {{ source('salesforce', 'src_Contact') }}

),

renamed as (

    select
    
        Id as contact_id,
        Full_Name__c as full_name,
        Current_OMR_Barcode__c as student_barcode,
        RecordTypeId

    from source

)

select * from renamed
--where student_barcode = '220069713'