with t0 as (
    select * from {{ source('salesforce', 'Career_Skill__c') }} where IsDeleted = false
),

t1 as (
    select
            Id as cs_id,
            Barcode__c as assessment_barcode,
            RecordTypeId as record_type_id,
            CreatedDate as created_on,  
            Created_from_Form__c as created_from_form,                     
            Name as cs_no,

            Grade__c as assessment_grade,
            CAST(Academic_Year__c as STRING) as assessment_academic_year,
            Batch_Id__c as assessment_batch_id,

            Q_11_1__c as q11_1, Q_11_2__c as q11_2, Q_11_3__c as q11_3, Q_11_4__c as q11_4, Q_11_5__c as q11_5, Q_11_6__c as q11_6, Q_11_7__c as q11_7,  
            Q_11_8__c as q11_8, Q_11_9__c as q11_9, Q_11_Ans__c as q11_marks,
        
            Q_12_1__c as q12_1, Q_12_2__c as q12_2, Q_12_3__c as q12_3, Q_12_4__c as q12_4, Q_12_Ans__c as q12_marks,
            
            Q_13__c as q13, Q_13_Ans__c as q13_marks,

            Q_14__c as q14, Q_14_Ans__c as q14_marks,
            
            Q_15_1__c as q15_1, Q_15_2__c as q15_2, Q_15_3__c as q15_3, Q_15_4__c as q15_4, Q_15_5__c as q15_5, Q_15_6__c as q15_6, 
            Q_15_7__c as q15_7, Q_15_8__c as q15_8, Q_15_9__c as q15_9, Q_15_Ans__c as q15_marks,
            
            Q_16__c as q16, Q_16_Ans__c as q16_marks,

            (Q_11_Ans__c + Q_12_Ans__c + Q_13_Ans__c + Q_14_Ans__c + Q_15_Ans__c + Q_16_Ans__c) as cs_total_marks,

            Error_Status__c as error_status, 
            Data_Clean_up__c as data_cleanup,
            Marks_Recalculated__c as marks_recalculated,
            Student_Linked__c as student_linked, 

    from t0 
),

t2 as (select record_type_id,record_type from {{ ref('seed_recordtype') }}),

t3 as (select cs_id, assessment_barcode, record_type, created_on, created_from_form, cs_no,

(case 

when cs_no is not null and (q11_1 is not null or q11_2 is not null or q11_3 is not null or q11_4 is not null or q11_5 is not null or q11_6 is not null or 
q11_7 is not null or q11_8 is not null or q11_9 is not null or q12_1 is not null or q12_2 is not null or q12_3 is not null or q12_4 is not null or 
q13 is not null or q14 is not null or q15_1 is not null or q15_2 is not null or q15_3 is not null or q15_4 is not null or q15_5 is not null or 
q15_6 is not null or q15_7 is not null or q15_8 is not null or q15_9 is not null or q16 is not null) then 1 

when cs_no is not null and (q11_1 is null and q11_2 is null and q11_3 is null and q11_4 is null and q11_5 is null and q11_6 is null and q11_7 is null 
and q11_8 is null and q11_9 is null and q12_1 is null and q12_2 is null and q12_3 is null and q12_4 is null and q13 is null and q14 is null and 
q15_1 is null and q15_2 is null and q15_3 is null and q15_4 is null and q15_5 is null and q15_6 is null and q15_7 is null and q15_8 is null and 
q15_9 is null and q16 is null) then 0 end) is_non_null,

t1.* except(cs_id, assessment_barcode, record_type_id, created_on, created_from_form, cs_no)

    from 
        t1
        left join t2 using (record_type_id) order by assessment_barcode, record_type
    )

select *
from t3