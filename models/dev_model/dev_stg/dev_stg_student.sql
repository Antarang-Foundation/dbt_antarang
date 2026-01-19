with
    source as (
        select
            Id as student_id,
            Bar_Code__c as first_barcode,
            Full_Name__c as student_name,
            RecordTypeId as record_type_id,

            case 
                when Gender__c = 'Male' then 'Male'
                when Gender__c = 'Female' then 'Female'
                when Gender__c = 'FMALE' then 'Female'
                when Gender__c = 'Transgender' then 'Transgender'
                when Gender__c = 'Others' then 'Others'
                when Gender__c = '*' then '*'
                else null
            end as gender,

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
            Recommendation_Report_Status__c as recommendation_report_status,
            Possible_Career_Report_Formula__c as possible_career_report,
            Career_Tracks__c as career_tracks,
            Clarity_Report__c as clarity_report,
            Current_Aspiration__c as current_aspiration,
            Possible_Careers_1__c as possible_careers_1,
            Possible_Careers_2__c as possible_careers_2,
            Possible_Careers_3__c as possible_careers_3,
            Followup1Aspiration__c as followup_1_aspiration,
            Followup2Aspiration__c as followup_2_aspiration,
            Student_Details_2__c as student_details_2_submitted,
            SD2_Grade__c as student_details_2_grade
        from {{ source('salesforce', 'Contact') }} where IsDeleted = false and 
        (lower(Full_Name__c) not like '%test%' or Id in ('003OW00000NemiDYAR', '003OW00000S958SYAR'))

    ),

    recordtypes as (
        select record_type_id, record_type from {{ ref('seed_recordtype') }}
        where record_type = 'CA Student'
    ),
    
    dev_stg_student as (
        select s.student_id, s.student_name, s.first_barcode, s.gender, s.current_barcode, s.g9_barcode, s.g10_barcode, s.g11_barcode, 
        s.g12_barcode, s.current_batch_no, s.g9_batch_id, s.g10_batch_id, s.g11_batch_id, s.g12_batch_id, s.current_grade1, 
        s.current_grade2, s.birth_year, s.birth_date, s.currently_studying, s.g9_whatsapp_no, s.g10_whatsapp_no, 
        s.g11_whatsapp_no, s.g12_whatsapp_no, s.g9_alternate_no, s.g10_alternate_no, s.g11_alternate_no, s.g12_alternate_no, 
        s.religion, s.caste, s.father_education, s.father_occupation, s.mother_education, s.mother_occupation,
        s.reality_1, s.reality_2, s.reality_3, s.reality_4, s.reality_5, s.reality_6, s.reality_7, s.reality_8, s.aspiration_1, 
        s.aspiration_2, s.aspiration_3, s.recommedation_status, s.recommendation_report_status, s.possible_career_report, 
        s.career_tracks, s.clarity_report, s.current_aspiration, s.possible_careers_1, s.possible_careers_2, s.possible_careers_3, 
        s.followup_1_aspiration, s.followup_2_aspiration, s.student_details_2_submitted, s.student_details_2_grade,
        LENGTH(s.student_details_2_grade) - LENGTH(REPLACE(s.student_details_2_grade, ';', '')) + 1 AS Student_GRADE_COUNT,
        Case when(s.g9_batch_id is not null and s.student_details_2_grade like '%9%') then 1 Else 0 END G9_Batch_Student_Flag,
        Case when(s.g10_batch_id is not null and s.student_details_2_grade like '%10%') then 1 Else 0 END G10_Batch_Student_Flag,
        Case when(s.g11_batch_id is not null and s.student_details_2_grade like '%11%') then 1 Else 0 END G11_Batch_Student_Flag,
        Case when(s.g12_batch_id is not null and s.student_details_2_grade like '%12%') then 1 Else 0 END G12_Batch_Student_Flag
        from source s
        inner join recordtypes b ON s.record_type_id = b.record_type_id
        where s.first_barcode is not null
            order by s.first_barcode
    )

select *
from dev_stg_student
/*where first_barcode IN ('2503014777',
'2503009554')*/


