with t0 as (
    select * from {{ source('salesforce', 'Self_Awareness_Realities__c') }}
),

t1 as (
    select

        Id as sar_id,
        Barcode__c as assessment_barcode,
        RecordTypeId as record_type_id,
        CreatedDate as created_on,  
        Created_from_Form__c as created_from_form,                     
        Name as sar_no,

        Grade__c as assessment_grade,
        CAST(Academic_Year__c as STRING) as assessment_academic_year,
        Batch_Id__c as assessment_batch_id,

        Quiz_2_Form_Submitted__c as q2_submitted, Reality_Form_Submitted__c as reality_submitted,

        Quiz_2_1__c as sar_q1, Quiz_2_1_Marks__c as sar_q1_marks, Quiz_2_2__c as sar_q2, Quiz_2_2_Marks__c as sar_q2_marks, 
        Quiz_2_3__c as sar_q3, Quiz_2_3_Marks__c as sar_q3_marks, Quiz_2_4__c as sar_q4, Quiz_2_4_Marks__c as sar_q4_marks, 
        Quiz_2_5__c as sar_q5, Quiz_2_5_Marks__c as sar_q5_marks,  
        
        Reality1_Self_1__c as r1s1, Reality1_Self_1_Ans__c  as r1s1_marks, 
        Reality_2_Self_2__c as r2s2, Reality_2_Self_2_Ans__c as r2s2_marks, 
        Reality_3_Self_3__c as r3s3, Reality_3_Self_3_Ans__c  as r3s3_marks, 
        Reality_4_Self_4__c as r4s4, Reality_4_Self_4_Ans__c as r4s4_marks, 
        Reality_5_Family_1__c as r5f1, Reality_5_Family_1_Ans__c as r5f1_marks, 
        Reality_6_Family_2__c as r6f2, Reality_6_Family_2_Ans__c as r6f2_marks, 
        Reality_7_Family_3__c as r7f3, Reality_7_Family_3_Ans__c as r7f3_marks, 
        Reality_8_Family_4__c as r8f4, Reality_8_Family_4_Ans__c as r8f4_marks,

        Error_Status__c as error_status, 
        Data_Clean_up__c as data_cleanup,
        Marks_Recalculated__c as marks_recalculated,
        Student_Linked__c as student_linked, 
        

    from t0 
),

t2 as (select record_type_id,record_type from {{ ref('seed_recordtype') }}),
t3 as (select sar_id, assessment_barcode, record_type, created_on, created_from_form, sar_no,

(case 

when sar_no is not null and (sar_q1 is not null or sar_q2 is not null or sar_q3 is not null or sar_q4 is not null or sar_q5 is not null or 
r1s1 is not null or r2s2 is not null or r3s3 is not null or r4s4 is not null or r5f1 is not null or r6f2 is not null or r7f3 is not null or 
r8f4 is not null) then 1 

when sar_no is not null and (sar_q1 is null and sar_q2 is null and sar_q3 is null and sar_q4 is null and sar_q5 is null and 
r1s1 is null and r2s2 is null and r3s3 is null and r4s4 is null and r5f1 is null and r6f2 is null and r7f3 is null and 
r8f4 is null) then 0 end) is_non_null,

(case 

when sar_no is not null and (sar_q1 is not null or sar_q2 is not null or sar_q3 is not null or sar_q4 is not null or sar_q5 is not null) then 1 
when sar_no is not null and (sar_q1 is null and sar_q2 is null and sar_q3 is null and sar_q4 is null and sar_q5 is null) 
then 0 end) sar_atleast_one_quiz,

(case 

when sar_no is not null and (r1s1 is not null or r2s2 is not null or r3s3 is not null or r4s4 is not null or r5f1 is not null or 
r6f2 is not null or r7f3 is not null or r8f4 is not null) then 1 

when sar_no is not null and (r1s1 is null and r2s2 is null and r3s3 is null and r4s4 is null and r5f1 is null and r6f2 is null and r7f3 is null and 
r8f4 is null) then 0 end) sar_atleast_one_reality,

t1.* except(sar_id, assessment_barcode, record_type_id, created_on, created_from_form, sar_no)

    from 
        t1
        left join t2 using (record_type_id) order by assessment_barcode, record_type
    )

select *
from t3