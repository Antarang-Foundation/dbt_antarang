with t1 as (
     select
        Id as sar_id,
        Barcode__c as assessment_barcode,
        RecordTypeId as record_type_id,
        CreatedDate as created_on,  
        Created_from_Form__c as created_from_form,                     
        Name as sar_no,
        Grade__c as assessment_grade,
        cast(Academic_Year__c as STRING) as assessment_academic_year,
        Batch_Id__c as assessment_batch_id,

        Quiz_2_Form_Submitted__c as q2_submitted, 
        Reality_Form_Submitted__c as reality_submitted,

        Quiz_2_1__c as sar_q1, Quiz_2_1_Marks__c as sar_q1_marks, 
        Quiz_2_2__c as sar_q2, Quiz_2_2_Marks__c as sar_q2_marks, 
        Quiz_2_3__c as sar_q3, Quiz_2_3_Marks__c as sar_q3_marks, 
        Quiz_2_4__c as sar_q4, Quiz_2_4_Marks__c as sar_q4_marks, 
        Quiz_2_5__c as sar_q5, Quiz_2_5_Marks__c as sar_q5_marks,  

        Reality1_Self_1__c as r1s1, Reality1_Self_1_Ans__c as r1s1_marks, 
        Reality_2_Self_2__c as r2s2, Reality_2_Self_2_Ans__c as r2s2_marks, 
        Reality_3_Self_3__c as r3s3, Reality_3_Self_3_Ans__c as r3s3_marks, 
        Reality_4_Self_4__c as r4s4, Reality_4_Self_4_Ans__c as r4s4_marks, 
        Reality_5_Family_1__c as r5f1, Reality_5_Family_1_Ans__c as r5f1_marks, 
        Reality_6_Family_2__c as r6f2, Reality_6_Family_2_Ans__c as r6f2_marks, 
        Reality_7_Family_3__c as r7f3, Reality_7_Family_3_Ans__c as r7f3_marks, 
        Reality_8_Family_4__c as r8f4, Reality_8_Family_4_Ans__c as r8f4_marks,

        Error_Status__c as error_status, 
        Data_Clean_up__c as data_cleanup,
        Marks_Recalculated__c as marks_recalculated,
        Student_Linked__c as student_linked,
       CASE WHEN (COALESCE(Quiz_2_1__c, Quiz_2_2__c, Quiz_2_3__c, Quiz_2_4__c, Quiz_2_5__c, Reality1_Self_1__c, 
       Reality_2_Self_2__c, Reality_3_Self_3__c, Reality_4_Self_4__c, Reality_5_Family_1__c, Reality_6_Family_2__c, 
        Reality_7_Family_3__c, Reality_8_Family_4__c) is not null) then 1 else 0 end AS quiz_reality_null_status,

        CASE WHEN (COALESCE(Quiz_2_1__c,Quiz_2_2__c, Quiz_2_3__c, Quiz_2_4__c, Quiz_2_5__c) is not null) then 1 else 0 end AS quiz_atleast_one,

        CASE WHEN (COALESCE(Reality1_Self_1__c, Reality_2_Self_2__c, Reality_3_Self_3__c, Reality_4_Self_4__c, Reality_5_Family_1__c, 
        Reality_6_Family_2__c, Reality_7_Family_3__c, Reality_8_Family_4__c) is not null) then 1 else 0 end AS reality_atleast_one
    from {{ source('salesforce', 'Self_Awareness_Realities__c') }} 
    where IsDeleted = false
),

t2 as (
    select record_type_id, record_type 
    from {{ ref('seed_recordtype') }}
),

t3 as (
    select 
        t1.sar_id,
        t1.assessment_barcode,
        t2.record_type,
        t1.created_on,
        t1.created_from_form,
        t1.sar_no,

        -- Flag 1: is_non_null
        /* 
        case
            when t1.sar_no is not null and (
                t1.sar_q1 is not null or t1.sar_q2 is not null or t1.sar_q3 is not null or t1.sar_q4 is not null or t1.sar_q5 is not null or 
                t1.r1s1 is not null or t1.r2s2 is not null or t1.r3s3 is not null or t1.r4s4 is not null or 
                t1.r5f1 is not null or t1.r6f2 is not null or t1.r7f3 is not null or t1.r8f4 is not null
            ) then 1
            when t1.sar_no is not null then 0
            else null
        end as is_non_null, */-- Replaced by sar_no_quiz_reality_status
        
        case
            when t1.sar_no is not null and quiz_reality_null_status = 1
            then 1
            when t1.sar_no is not null then 0
            else null
        end as is_non_null, --sar_no_quiz_reality_status

        /*-- Flag 2: at least one quiz
        case
            when t1.sar_no is not null and (
                t1.sar_q1 is not null or t1.sar_q2 is not null or t1.sar_q3 is not null or t1.sar_q4 is not null or t1.sar_q5 is not null
            ) then 1
            when t1.sar_no is not null then 0
            else null

        end as sar_atleast_one_quiz, */ -- Replaced by quiz_atleast_one

        case
            when t1.sar_no is not null and quiz_atleast_one = 1
            then 1
            when t1.sar_no is not null then 0
            else null
        end as sar_atleast_one_quiz, --sar_no_quiz_atleast_one

        /*-- Flag 3: at least one reality
        case
            when t1.sar_no is not null and (
                t1.r1s1 is not null or t1.r2s2 is not null or t1.r3s3 is not null or t1.r4s4 is not null or 
                t1.r5f1 is not null or t1.r6f2 is not null or t1.r7f3 is not null or t1.r8f4 is not null
            ) then 1
            when t1.sar_no is not null then 0
            else null
        end as sar_atleast_one_reality, */ -- Replaced by reality_atleast_one

        case
            when t1.sar_no is not null and reality_atleast_one = 1
            then 1
            when t1.sar_no is not null then 0
            else null
        end as sar_atleast_one_reality, --sar_no_reality_atleast_one,

        -- Rest of the fields
        t1.assessment_grade, t1.assessment_academic_year, t1.assessment_batch_id, t1.q2_submitted, t1.reality_submitted, 
        t1.sar_q1, t1.sar_q1_marks, t1.sar_q2, t1.sar_q2_marks, t1.sar_q3, t1.sar_q3_marks, t1.sar_q4, t1.sar_q4_marks, 
        t1.sar_q5, t1.sar_q5_marks, t1.r1s1, t1.r1s1_marks, t1.r2s2, t1.r2s2_marks, t1.r3s3, t1.r3s3_marks, t1.r4s4, 
        t1.r4s4_marks, t1.r5f1, t1.r5f1_marks, t1.r6f2, t1.r6f2_marks, t1.r7f3, t1.r7f3_marks, t1.r8f4, t1.r8f4_marks, 
        t1.error_status, t1.data_cleanup, t1.marks_recalculated, t1.student_linked

    from t1
    left join t2 on t1.record_type_id = t2.record_type_id
)

select * 
from t3