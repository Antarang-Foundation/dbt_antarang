with t0 as (
    select * from {{ source('salesforce', 'Career_Planning__c') }}
),

t1 as (
    select
            Id as cp_id,
            Barcode__c as assessment_barcode,
            RecordTypeId as record_type_id,
            CreatedDate as created_on,                       
            Name as cp_no,

            Grade__c as assessment_grade,
            CAST(Academic_Year__c as STRING) as assessment_academic_year,
            Batch_Id__c as assessment_batch_id,

            Q_7__c as q7, 
            Q_7_Marks__c as q7_marks,
            Q_8__c as q8, 
            Q_8_Marks__c as q8_marks,
            Q_9_1__c as q9_1, 
            Q_9_1_Marks__c as q9_1_marks,
            Q_9_2__c as q9_2, 
            Q_9_2_Marks__c as q9_2_marks,
            Q_9_3__c as q9_3, 
            Q_9_3_Marks__c as q9_3_marks,
            Q_9_4__c as q9_4, 
            Q_9_4_Marks__c as q9_4_marks,
            Q_9_5__c as q9_5, 
            Q_9_5_Marks__c as q9_5_marks,
            Q_9_6__c as q9_6, 
            Q_9_6_Marks__c as q9_6_marks,
            Q_9_7__c as q9_7, 
            Q_9_7_Marks__c as q9_7_marks,
            Q_10__c as q10,
            Q_10_Marks__c as q10_marks,
            (Q_7_Marks__c + Q_8_Marks__c + Q_9_1_Marks__c + Q_9_2_Marks__c + Q_9_3_Marks__c + Q_9_4_Marks__c + Q_9_5_Marks__c + Q_9_6_Marks__c + 
            Q_9_7_Marks__c + Q_10_Marks__c) as cp_total_marks,

            Error_Status__c as error_status, 
            Created_from_Form__c as created_from_form,
            Data_Clean_up__c as data_cleanup,
            Marks_Recalculated__c as marks_recalculated,
            Student_Linked__c as student_linked, 

    from t0 
),

t2 as (select record_type_id,record_type from {{ ref('seed_recordtype') }}),
stg_cp as (
    select 
        cp_id,
        assessment_barcode,
        record_type,
        t1.* except(cp_id, assessment_barcode, record_type_id) 

    from 
        t1
        left join t2 using (record_type_id) order by assessment_barcode, record_type
    )

select *
from stg_cp
