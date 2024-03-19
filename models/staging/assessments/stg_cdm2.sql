with t0 as (
    select * from {{ source('salesforce', 'CDM2__c') }}
),

t1 as (
    select
            Id as cdm2_id,
            Barcode__c as assessment_barcode,
            RecordTypeId as record_type_id,
            CreatedDate as created_on,                       
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
            Created_from_Form__c as created_from_form,
            Data_Clean_up__c as data_cleanup,
            Marks_Recalculated__c as marks_recalculated,
            Student_Linked__c as student_linked

    from t0 
),

t2 as (select record_type_id,record_type from {{ ref('stg_recordtypes') }}),
t3 as (
    select 
        cdm2_id,
        assessment_barcode,
        record_type,
        t1.* except(cdm2_id, assessment_barcode, record_type_id) 

    from 
        t1
        left join t2 using (record_type_id) order by assessment_barcode, record_type
    )

select *
from t3