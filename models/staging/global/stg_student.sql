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
        Mother_Occupation__c as mother_occupation,
        Reality_1__c as reality_1,
        Reality_2__c as reality_2,
        Reality_3__c as reality_3,
        Reality_5__c as reality_5,
        Reality_6__c as reality_6,
        Reality_4__c as reality_4,
        Reality_7__c as reality_7,
        Reality_8__c as reality_8,
        Aspiration_1__c as aspiration_1,
        Aspiration_2__c as aspiration_2,
        Aspiration_3__c as aspiration_3,
        Recommedation_Status__c as recommedation_status,
        Recommendation_Report_Status__c as  recommendation_report_status,
        Possible_Career_Report_Formula__c as possible_career_report,
        Career_Tracks__c as career_tracks,
        Clarity_Report__c as clarity_report,
        Current_Aspiration__c as current_aspiration,
        Possible_Careers_1__c as possible_careers_1,
        Possible_Careers_2__c as possible_careers_2,
        Possible_Careers_3__c as possible_careers_3,
        Followup1Aspiration__c as followup_1_aspiration,
        Followup2Aspiration__c as followup_2_aspiration

    from source

    where Full_Name__c not like '%test%' or Full_Name__c not like '%Test%' 
),

recordtypes as (select record_type_id, record_type from {{ ref('seed_recordtype') }}),
    
    stg_student as (
        select * except (record_type_id)
        from 
            renamed
            --left join recordtypes using (record_type_id) where record_type = 'CA Student' and first_barcode is not null
            INNER join recordtypes using (record_type_id) where record_type = 'CA Student' and first_barcode is not null
            order by first_barcode
    )
    
select *
from renamed


