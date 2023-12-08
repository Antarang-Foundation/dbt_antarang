with source as (
    select * from {{ source('salesforce', 'Contact') }}
),

renamed as (
    select
        Id as contact_id,
        Full_Name__c as full_name,
        Current_OMR_Barcode__c as student_barcode,
        RecordTypeId as record_type_id,
        CASE 
            WHEN Gender__c='Male' THEN 'Male'
            WHEN Gender__c='Female' THEN 'Female'
            WHEN Gender__c='FMALE' THEN 'Female'
            WHEN Gender__c='Transgender' THEN 'Transgender'
            WHEN Gender__c='Others' THEN 'Others'
            WHEN Gender__c='*' THEN '*'
            ELSE null
        END as gender,
        Batch_Code__c as g9_batch_code,
        G10_Batch_Code__c as g10_batch_code,
        G11_Batch_Code__c as g11_batch_code,
        G12_Batch_Code__c as g12_batch_code,
        Grade_9_Barcode__c as g9_barcode,
        Grade_10_Barcode__c as g10_barcode,
        Grade_11_Barcode__c as g11_barcode,
        Grade_12_Barcode__c as g12_barcode,
        Academic_Year__c as academic_year
    from source
)

select * from renamed
