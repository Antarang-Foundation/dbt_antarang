with source as (
    select * from {{ source('salesforce', 'Contact') }} where IsDeleted = false
),

renamed as (
    select
        Id as student_id,
        Bar_Code__c as first_barcode,
        Full_Name__c as student_name,       
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

        Current_OMR_Barcode__c as current_barcode,
        Grade_9_Barcode__c as g9_barcode,
        Grade_10_Barcode__c as g10_barcode,
        Grade_11_Barcode__c as g11_barcode,
        Grade_12_Barcode__c as g12_barcode,

        Current_Batch_Number__c as current_batch_no,
        Batch_Code__c as g9_batch_id,
        G10_Batch_Code__c as g10_batch_id,
        G11_Batch_Code__c as g11_batch_id,
        G12_Batch_Code__c as g12_batch_id,

        Currently_Studying_In__c as current_grade1,
        Current_Batch_Grade__c as current_grade2,

        Year_of_Birth__c as birth_year,
        Birthdate as birth_date,
        What_are_you_currently_studying__c as currently_studying,
        G9_Whatsapp_Number__c as g9_whatsapp_no,
        G10_Whatsapp_Number__c as g10_whatsapp_no,
        G11_Whatsapp_Number__c as g11_whatsapp_no,
        G12_Whatsapp_Number__c as g12_whatsapp_no,
        G9_Alternate_Mobile_No__c as g9_alternate_no,
        G10_Alternate_Mobile_No__c as g10_alternate_no,
        G11_Alternate_Mobile_No__c as g11_alternate_no,
        G12_Alternate_Mobile_No__c as g12_alternate_no,
        Religion__c as religion,
        Caste_Certificate_present__c as caste,
        Father_Education__c as father_education,
        Father_Occupation__c as father_occupation,
        Mother_Education__c as mother_education,
        Mother_Occupation__c as mother_occupation


    from source

    where Full_Name__c not like '%test%' or Full_Name__c not like '%Test%' 
),

recordtypes as (select record_type_id, record_type from {{ ref('seed_recordtype') }}),
    
    stg_student as (
        select * except (record_type_id)
        from 
            renamed
            left join recordtypes using (record_type_id) where record_type = 'CA Student' and first_barcode is not null
            order by first_barcode
    )
    
select *
from stg_student


