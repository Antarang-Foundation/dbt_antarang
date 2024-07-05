with t0 as (
    select * from {{ source('salesforce', 'Self_Awareness_and_Feedback__c') }} where IsDeleted = false
),

t1 as (
    select

        Id as saf_id,
        Barcode__c as assessment_barcode,
        RecordTypeId as record_type_id,
        CreatedDate as created_on,  
        Created_from_Form__c as created_from_form,                     
        Name as saf_no,

        Grade__c as assessment_grade,
        CAST(Academic_Year__c as STRING) as assessment_academic_year,
        Batch_Id__c as assessment_batch_id,

        Interest_Form_Submitted__c as interest_submitted,
        Apptitude_Form_Submitted__c as aptitude_submitted,
        Feedback_Form_Submitted__c as feedback_submitted,

        Interest_1__c as saf_i1, Interest_2__c as saf_i2, Interest_3__c as saf_i3, Aptitude_1__c as saf_a1, Aptitude_2__c as saf_a2, 
        Aptitude_3__c as saf_a3, 
        
        Quiz_1_1__c as saf_q1, Quiz_1_1_Marks__c as saf_q1_marks, Quiz_1_2__c as saf_q2, Quiz_1_2_Marks__c as saf_q2_marks, 
        Quiz_1_3__c as saf_q3, Quiz_1_3_Marks__c as saf_q3_marks, Quiz_1_4__c as saf_q4, Quiz_1_4_Marks__c as saf_q4_marks, 
        Quiz_1_5__c as saf_q5, Quiz_1_5_Marks__c as saf_q5_marks, 
        
        Feedback_1__c as saf_f1, Feedback_2__c as saf_f2, Feedback_3__c as saf_f3, Feedback_4__c as saf_f4, Feedback_5__c as saf_f5, 
        Feedback_6__c as saf_f6, Feedback_7__c as saf_f7, Feedback_8__c as saf_f8, Feedback_9__c as saf_f9, Feedback_10__c as saf_f10,
        Feedback_11__c as saf_f11, Feedback_12__c as saf_f12,

        Error_Status__c as error_status, 
        Data_Clean_up__c as data_cleanup,
        Marks_Recalculated__c as marks_recalculated,
        Student_Linked__c as student_linked, 
        

    from t0 
),

t2 as (select record_type_id,record_type from {{ ref('seed_recordtype') }}),
t3 as (select saf_id, assessment_barcode, record_type, created_on, created_from_form, saf_no,

(case 

when saf_no is not null and (saf_i1 is not null or saf_i2 is not null or saf_i3 is not null or saf_a1 is not null or saf_a2 is not null or 
saf_a3 is not null or saf_q1 is not null or saf_q2 is not null or saf_q3 is not null or saf_q4 is not null or saf_q5 is not null or saf_f1 is not null or 
saf_f2 is not null or saf_f3 is not null or saf_f4 is not null or saf_f5 is not null or saf_f6 is not null or saf_f7 is not null or saf_f8 is not null or 
saf_f9 is not null or saf_f10 is not null or saf_f11 is not null or saf_f12 is not null) then 1 

when saf_no is not null and (saf_i1 is null and saf_i2 is null and saf_i3 is null and saf_a1 is null and saf_a2 is null and 
saf_a3 is null and saf_q1 is null and saf_q2 is null and saf_q3 is null and saf_q4 is null and saf_q5 is null and saf_f1 is null and 
saf_f2 is null and saf_f3 is null and saf_f4 is null and saf_f5 is null and saf_f6 is null and saf_f7 is null and saf_f8 is null and 
saf_f9 is null and saf_f10 is null and saf_f11 is null and saf_f12 is null) then 0 end) is_non_null,

(case 

when saf_no is not null and (saf_i1 is not null or saf_i2 is not null or saf_i3 is not null) then 1 

when saf_no is not null and (saf_i1 is null and saf_i2 is null and saf_i3 is null) then 0 end) saf_atleast_one_interest,

(case 

when saf_no is not null and (saf_a1 is not null or saf_a2 is not null or saf_a3 is not null) then 1 

when saf_no is not null and (saf_a1 is null and saf_a2 is null and saf_a3 is null) then 0 end) saf_atleast_one_aptitude,

(case 

when saf_no is not null and (saf_q1 is not null or saf_q2 is not null or saf_q3 is not null or saf_q4 is not null or saf_q5 is not null) then 1 

when saf_no is not null and (saf_q1 is null and saf_q2 is null and saf_q3 is null and saf_q4 is null and saf_q5 is null) then 0 end) saf_atleast_one_quiz,

(case 

when saf_no is not null and (saf_f1 is not null or saf_f2 is not null or saf_f3 is not null or saf_f4 is not null or saf_f5 is not null or 
saf_f6 is not null or saf_f7 is not null or saf_f8 is not null or saf_f9 is not null or saf_f10 is not null or saf_f11 is not null or 
saf_f12 is not null) then 1 

when saf_no is not null and (saf_f1 is null and saf_f2 is null and saf_f3 is null and saf_f4 is null and saf_f5 is null and saf_f6 is null and saf_f7 is null and saf_f8 is null and 
saf_f9 is null and saf_f10 is null and saf_f11 is null and saf_f12 is null) then 0 end) saf_atleast_one_feedback,

t1.* except(saf_id, assessment_barcode, record_type_id, created_on, created_from_form, saf_no)

    from 
        t1
        left join t2 using (record_type_id) order by assessment_barcode, record_type
    )

select *
from t3