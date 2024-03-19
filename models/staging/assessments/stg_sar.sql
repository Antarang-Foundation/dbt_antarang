with t0 as (
    select * from {{ source('salesforce', 'Self_Awareness_Realities__c') }}
),

t1 as (
    select

        Id as sar_id,
        Barcode__c as assessment_barcode,
        RecordTypeId as record_type_id,
        CreatedDate as created_on,                       
        Name as sar_no,

        Grade__c as assessment_grade,
        CAST(Academic_Year__c as STRING) as assessment_academic_year,
        Batch_Id__c as assessment_batch_id,

        Quiz_2_Form_Submitted__c as q2_submitted, Reality_Form_Submitted__c as reality_submitted,

        Quiz_2_1__c as sar_q1, Quiz_2_1_Marks__c as sar_q1_marks, Quiz_2_2__c as sar_q2, Quiz_2_2_Marks__c as sar_q2_marks, 
        Quiz_2_3__c as sar_q3, Quiz_2_3_Marks__c as sar_q3_marks, Quiz_2_4__c as sar_q4, Quiz_2_4_Marks__c as sar_q4_marks, 
        Quiz_2_5__c as sar_q5, Quiz_2_5_Marks__c as sar_q5_marks,  
        
        Reality1_Self_1__c as r1_s1, Reality1_Self_1_Ans__c  as r1_s1_marks, 
        Reality_2_Self_2__c as r2_s2, Reality_2_Self_2_Ans__c as r2_s2_marks, 
        Reality_3_Self_3__c as r3_s3, Reality_3_Self_3_Ans__c  as r3_s3_marks, 
        Reality_4_Self_4__c as r4_s4, Reality_4_Self_4_Ans__c as r4_s4_marks, 
        Reality_5_Family_1__c as r5_f1, Reality_5_Family_1_Ans__c as r5_f1_marks, 
        Reality_6_Family_2__c as r6_f2, Reality_6_Family_2_Ans__c as r6_f2_marks, 
        Reality_7_Family_3__c as r7_f3, Reality_7_Family_3_Ans__c as r7_f3_marks, 
        Reality_8_Family_4__c as r8_f4, Reality_8_Family_4_Ans__c as r8_f4_marks,

        Error_Status__c as error_status, 
        Created_from_Form__c as created_from_form,
        Data_Clean_up__c as data_cleanup,
        Marks_Recalculated__c as marks_recalculated,
        Student_Linked__c as student_linked, 
        

    from t0 
),

t2 as (select record_type_id,record_type from {{ ref('stg_recordtypes') }}),
t3 as (
    select 
        sar_id,
        assessment_barcode,
        record_type,
        t1.* except(sar_id, assessment_barcode, record_type_id) 

    from 
        t1
        left join t2 using (record_type_id) order by assessment_barcode, record_type
    )

select *
from t3