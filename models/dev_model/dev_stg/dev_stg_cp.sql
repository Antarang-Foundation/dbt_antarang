with source as (
    select
        Id as cp_id,
        Barcode__c as assessment_barcode,
        RecordTypeId as record_type_id,
        CreatedDate as created_on, 
        Created_from_Form__c as created_from_form,                      
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
        Data_Clean_up__c as data_cleanup,
        Marks_Recalculated__c as marks_recalculated,
        Student_Linked__c as student_linked,
        Q1_C1__c as q1_c1,
            Q1_C1_Ans__c as q1_c1_ans,
            Q1_C1_Faci__c as q1_c1_faci,
            Q1_C2__c as q1_c2,
            Q1_C2_Ans__c as q1_c2_ans,
            Q1_C2_Faci__c as q1_c2_faci,
            A_Q2_C1__c as a_q2_c1,
            A_Q2_C2__c as a_q2_c2,
            A_Q3_C1__c as a_q3_c1,
            A_Q3_C1_Ans__c as a_q3_c1_ans,
            A_Q3_C2__c as a_q3_c2,
            A_Q3_C2_Ans__c as a_q3_c2_ans,
            A_Q4_C1__c as a_q4_c1,
            A_Q4_C1_Ans__c as a_q4_c1_ans,
            A_Q4_C2__c as a_q4_c2,
            A_Q4_C2_Ans__c as a_q4_c2_ans,
            A_Q5_C1__c as a_q5_c1,
            A_Q5_C2__c as a_q5_c2,
            A_Q6_C1__c as a_q6_c1,
            A_Q6_C1_Ans__c as a_q6_c1_ans,
            A_Q6_C2__c as a_q6_c2,
            A_Q6_C2_Ans__c as a_q6_c2_ans,
            A_Q7_C1__c as a_q7_c1,
            A_Q7_C1_Ans__c as a_q7_c1_ans,
            A_Q7_C2__c as a_q7_c2,
            A_Q7_C2_Ans__c as a_q7_c2_ans,
            A_Q8_C1__c as a_q8_c1,
            A_Q8_C1_Ans__c as a_q8_c1_ans,
            A_Q8_C2__c as a_q8_c2,
            A_Q8_C2_Ans__c as a_q8_c2_ans,
            B_Q2_C1__c as b_q2_c1,
            B_Q2_C1_Ans__c as b_q2_c1_ans,
            B_Q2_C2__c as b_q2_c2,
            B_Q2_C2_Ans__c as b_q2_c2_ans,
            B_Q3_C1__c as b_q3_c1,
            B_Q3_C2__c as b_q3_c2,
            B_Q4_C1__c as b_q4_c1,
            B_Q4_C1_Ans__c as b_q4_c1_ans,
            B_Q4_C2__c as b_q4_c2,
            B_Q4_C2_Ans__c as b_q4_c2_ans,
            B_Q5_C1__c as b_q5_c1,
            B_Q5_C1_Ans__c as b_q5_c1_ans,
            B_Q5_C2__c as b_q5_c2,
            B_Q5_C2_Ans__c as b_q5_c2_ans,
            B_Q6_C1__c as b_q6_c1,
            B_Q6_C1_Ans__c as b_q6_c1_ans,
            B_Q6_C2__c as b_q6_c2,
            B_Q6_C2_Ans__c as b_q6_c2_ans,
            B_Q7_C1__c as b_q7_c1,
            B_Q7_C2__c as b_q7_c2,
            Q2__c as q2,
            Q2_Other__c as q2_other,
            Q3__c as q3,
            Q4__c as q4,
            Q4_Ans__c as q4_ans
    from {{ source('salesforce', 'Career_Planning__c') }}
    where IsDeleted = false
),

record_type as (
    select 
        record_type_id,
        record_type 
    from {{ ref('seed_recordtype') }}
),

joined_source as (
    select 
        s.*,
        rt.record_type
    from source s
    inner join record_type rt 
        on s.record_type_id = rt.record_type_id
),

t2 as (
    select 
        cp_id,
        assessment_barcode,
        record_type,
        created_on,
        created_from_form,
        cp_no,
        CASE
    WHEN cp_no IS NOT NULL
AND (
       q7 IS NOT NULL
    OR q8 IS NOT NULL
    OR q9_1 IS NOT NULL
    OR q9_2 IS NOT NULL
    OR q9_3 IS NOT NULL
    OR q9_4 IS NOT NULL
    OR q9_5 IS NOT NULL
    OR q9_6 IS NOT NULL
    OR q9_7 IS NOT NULL
    OR q10 IS NOT NULL

    OR q1_c1 IS NOT NULL
    OR q1_c1_ans IS NOT NULL
    OR q1_c1_faci IS NOT NULL
    OR q1_c2 IS NOT NULL
    OR q1_c2_ans IS NOT NULL
    OR q1_c2_faci IS NOT NULL

    OR a_q2_c1 IS NOT NULL
    OR a_q2_c2 IS NOT NULL

    OR a_q3_c1 IS NOT NULL
    OR a_q3_c1_ans IS NOT NULL
    OR a_q3_c2 IS NOT NULL
    OR a_q3_c2_ans IS NOT NULL

    OR a_q4_c1 IS NOT NULL
    OR a_q4_c1_ans IS NOT NULL
    OR a_q4_c2 IS NOT NULL
    OR a_q4_c2_ans IS NOT NULL

    OR a_q5_c1 IS NOT NULL
    OR a_q5_c2 IS NOT NULL

    OR b_q2_c1 IS NOT NULL
    OR b_q2_c1_ans IS NOT NULL
    OR b_q2_c2 IS NOT NULL
    OR b_q2_c2_ans IS NOT NULL

    OR b_q3_c1 IS NOT NULL
    OR b_q3_c2 IS NOT NULL

    OR b_q4_c1 IS NOT NULL
    OR b_q4_c1_ans IS NOT NULL
    OR b_q4_c2 IS NOT NULL
    OR b_q4_c2_ans IS NOT NULL

    OR b_q5_c1 IS NOT NULL
    OR b_q5_c1_ans IS NOT NULL
    OR b_q5_c2 IS NOT NULL
    OR b_q5_c2_ans IS NOT NULL

    OR a_q6_c1 IS NOT NULL
    OR a_q6_c1_ans IS NOT NULL
    OR a_q6_c2 IS NOT NULL
    OR a_q6_c2_ans IS NOT NULL

    OR b_q6_c1 IS NOT NULL
    OR b_q6_c1_ans IS NOT NULL
    OR b_q6_c2 IS NOT NULL
    OR b_q6_c2_ans IS NOT NULL

    OR a_q7_c1 IS NOT NULL
    OR a_q7_c1_ans IS NOT NULL
    OR a_q7_c2 IS NOT NULL
    OR a_q7_c2_ans IS NOT NULL

    OR b_q7_c1 IS NOT NULL
    OR b_q7_c2 IS NOT NULL

    OR a_q8_c1 IS NOT NULL
    OR a_q8_c1_ans IS NOT NULL
    OR a_q8_c2 IS NOT NULL
    OR a_q8_c2_ans IS NOT NULL

    OR q2 IS NOT NULL
    OR q2_other IS NOT NULL
    OR q3 IS NOT NULL
    OR q4 IS NOT NULL
    OR q4_ans IS NOT NULL
)
THEN 1

WHEN cp_no IS NOT NULL
AND (
       q7 IS NOT NULL
    OR q8 IS NOT NULL
    OR q9_1 IS NOT NULL
    OR q9_2 IS NOT NULL
    OR q9_3 IS NOT NULL
    OR q9_4 IS NOT NULL
    OR q9_5 IS NOT NULL
    OR q9_6 IS NOT NULL
    OR q9_7 IS NOT NULL
    OR q10 IS NOT NULL

    OR q1_c1 IS NOT NULL
    OR q1_c1_ans IS NOT NULL
    OR q1_c1_faci IS NOT NULL
    OR q1_c2 IS NOT NULL
    OR q1_c2_ans IS NOT NULL
    OR q1_c2_faci IS NOT NULL

    OR a_q2_c1 IS NOT NULL
    OR a_q2_c2 IS NOT NULL

    OR a_q3_c1 IS NOT NULL
    OR a_q3_c1_ans IS NOT NULL
    OR a_q3_c2 IS NOT NULL
    OR a_q3_c2_ans IS NOT NULL

    OR a_q4_c1 IS NOT NULL
    OR a_q4_c1_ans IS NOT NULL
    OR a_q4_c2 IS NOT NULL
    OR a_q4_c2_ans IS NOT NULL

    OR a_q5_c1 IS NOT NULL
    OR a_q5_c2 IS NOT NULL

    OR b_q2_c1 IS NOT NULL
    OR b_q2_c1_ans IS NOT NULL
    OR b_q2_c2 IS NOT NULL
    OR b_q2_c2_ans IS NOT NULL

    OR b_q3_c1 IS NOT NULL
    OR b_q3_c2 IS NOT NULL

    OR b_q4_c1 IS NOT NULL
    OR b_q4_c1_ans IS NOT NULL
    OR b_q4_c2 IS NOT NULL
    OR b_q4_c2_ans IS NOT NULL

    OR b_q5_c1 IS NOT NULL
    OR b_q5_c1_ans IS NOT NULL
    OR b_q5_c2 IS NOT NULL
    OR b_q5_c2_ans IS NOT NULL

    OR a_q6_c1 IS NOT NULL
    OR a_q6_c1_ans IS NOT NULL
    OR a_q6_c2 IS NOT NULL
    OR a_q6_c2_ans IS NOT NULL

    OR b_q6_c1 IS NOT NULL
    OR b_q6_c1_ans IS NOT NULL
    OR b_q6_c2 IS NOT NULL
    OR b_q6_c2_ans IS NOT NULL

    OR a_q7_c1 IS NOT NULL
    OR a_q7_c1_ans IS NOT NULL
    OR a_q7_c2 IS NOT NULL
    OR a_q7_c2_ans IS NOT NULL

    OR b_q7_c1 IS NOT NULL
    OR b_q7_c2 IS NOT NULL

    OR a_q8_c1 IS NOT NULL
    OR a_q8_c1_ans IS NOT NULL
    OR a_q8_c2 IS NOT NULL
    OR a_q8_c2_ans IS NOT NULL

    OR q2 IS NOT NULL
    OR q2_other IS NOT NULL
    OR q3 IS NOT NULL
    OR q4 IS NOT NULL
    OR q4_ans IS NOT NULL
)
THEN 0 
END AS is_non_null,
        q7, q7_marks, q8, q8_marks, q9_1, q9_1_marks, q9_2, q9_2_marks,
        q9_3, q9_3_marks, q9_4, q9_4_marks, q9_5, q9_5_marks, q9_6, q9_6_marks,
        q9_7, q9_7_marks, q10, q10_marks, cp_total_marks, data_cleanup, marks_recalculated, student_linked
    from joined_source
),

t3 as (
    select 
        t2.*,
        case 
            when q8 is not null and q8 != '*' then q8_marks
            else 3
        end as q8_null,
        case 
            when (case when q8 is not null and q8 != '*' then q8_marks else 3 end) = 1 then '1. Yes'
            when (case when q8 is not null and q8 != '*' then q8_marks else 3 end) = 0.5 then '3. Other'
            when (case when q8 is not null and q8 != '*' then q8_marks else 3 end) = 0 then '2. No'
            else '4. DNA'
        end as q8_bucket
    from t2
)

select * from t3
