with source as (

    select * from {{ source('salesforce', 'Contact') }}

),

renamed as (

    select
    
        Id as contact_id,
        Full_Name__c as full_name,
        Current_OMR_Barcode__c as student_barcode,
        RecordTypeId as record_type_id,
        Gender__c as gender,
        Batch_Code__c as g9_batch_code,
        G10_Batch_Code__c as g10_batch_code,
        G11_Batch_Code__c as g11_batch_code,
        G12_Batch_Code__c as g12_batch_code,
        Grade_9_Barcode__c as g9_barcode,
        Grade_10_Barcode__c as g10_barcode,
        Grade_11_Barcode__c as g11_barcode,
        --Grade_12_Barcode__c as g12_barcode

    from source

)

select * from renamed
where student_barcode = '2303214005'