with t0 as (
    select * from {{ source('salesforce', 'CDM2__c') }} where IsDeleted = false
),

t1 as (
    select
            Id as cdm2_id,
            Barcode__c as assessment_barcode,
            RecordTypeId as record_type_id,
            CreatedDate as created_on,  
            Created_from_Form__c as created_from_form,                     
            Name as cdm2_no,

            Grade__c as assessment_grade,
            CAST(Academic_Year__c as STRING) as assessment_academic_year,
            Batch_Id__c as assessment_batch_id,

            Q5__c as q5,
            X5_Confident_about_chosen_career__c as q5_marks,

            Q6_1__c as q6_1, 
            Q6_1_Marks__c as q6_1_marks,
            Q6_2__c as q6_2, 
            Q6_2_Marks__c as q6_2_marks,
            Q6_3__c as q6_3, 
            Q6_3_Marks__c as q6_3_marks,
            Q6_4__c as q6_4, 
            Q6_4_Marks__c as q6_4_marks,
            Q6_5__c as q6_5, 
            Q6_5_Marks__c as q6_5_marks,
            Q6_6__c as q6_6, 
            Q6_6_Marks__c as q6_6_marks,
            Q6_7__c as q6_7, 
            Q6_7_Marks__c as q6_7_marks,
            Q6_8__c as q6_8, 
            Q6_8_Marks__c as q6_8_marks,
            Q6_9__c as q6_9,
            Q6_9_Marks__c as q6_9_marks,
            Q6_10__c as q6_10, 
            Q6_10_Marks__c as q6_10_marks,
            Q6_11__c as q6_11, 
            Q6_11_Marks__c as q6_11_marks,
            Q6_12__c as q6_12, 
            Q6_12_Marks__c as q6_12_marks,

            X6_Options_that_fit_into_Industry__c as q6_total_marks,

            (X5_Confident_about_chosen_career__c +  X6_Options_that_fit_into_Industry__c) as cdm2_total_marks,

            Error_Status__c as error_status, 
            Data_Clean_up__c as data_cleanup,
            Marks_Recalculated__c as marks_recalculated,
            Student_Linked__c as student_linked

    from t0 
),

t2 as (select record_type_id,record_type from {{ ref('seed_recordtype') }}),

t3 as (select cdm2_id, assessment_barcode, record_type, created_on, created_from_form, cdm2_no,

(case 

when cdm2_no is not null and (q5 is not null or q6_1 is not null or q6_2 is not null or q6_1 is not null or q6_2 is not null 
or q6_3 is not null or q6_4 is not null or q6_5 is not null or q6_6 is not null or q6_7 is not null or q6_8 is not null 
or q6_9 is not null or q6_10 is not null or q6_11 is not null or q6_12 is not null) then 1 

when cdm2_no is not null and (q5 is null and q6_1 is null and q6_2 is null and q6_3 is null and q6_4 is null and q6_5 is null and q6_6 is null and 
q6_7 is null and q6_8 is null and q6_9 is null and q6_10 is null and q6_11 is null and q6_12 is null) then 0 end) is_non_null,

t1.* except(cdm2_id, assessment_barcode, record_type_id, created_on, created_from_form, cdm2_no) 

from t1 left join t2 using (record_type_id) order by assessment_barcode, record_type
    )

select *
from t3