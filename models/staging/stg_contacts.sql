with source as (
    select * from {{ source('salesforce', 'Contact') }}
),

renamed as (
    select
        Id as contact_id,
        Full_Name__c as full_name,
        Current_OMR_Barcode__c as current_barcode,
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
        Batch_Code__c as g9_contact_batch_id,
        G10_Batch_Code__c as g10_contact_batch_id,
        G11_Batch_Code__c as g11_contact_batch_id,
        G12_Batch_Code__c as g12_contact_batch_id,
        Grade_9_Barcode__c as g9_barcode,
        Grade_10_Barcode__c as g10_barcode,
        Grade_11_Barcode__c as g11_barcode,
        Grade_12_Barcode__c as g12_barcode,
        Academic_Year__c as contact_academic_year,
        Year_of_Birth__c as birth_year,
        Birthdate as birthdate,
        Currently_Studying_In__c as current_grade,
        Current_Batch_Number__c as current_batch_no,
        -- What_are_you_currently_studying__c as What_are_you_currently_studying, --
        /* Current_Batch_Name__c as Current_Batch_Name,
        Current_Whatsapp_Number__c as Current_Whatsapp_Number,
        Current_Batch_Grade__c as Current_Batch_Grade, */
        Bar_Code__c as uid,
        G9_Whatsapp_Number__c as g9_whatsapp_no,
        G10_Whatsapp_Number__c as g10_whatsapp_no,
        G11_Whatsapp_Number__c as g11_whatsapp_no
        --G12_Whatsapp_Number__c as G12_whatsapp_no,--
       





    from source
)

select * from renamed
