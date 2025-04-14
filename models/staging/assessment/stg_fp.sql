with t0 as (
    select * from {{ source('salesforce', 'Future_Planning__c') }} where IsDeleted = false
),

t1 as (
    select

        Id as fp_id,
        Barcode__c as assessment_barcode,
        RecordTypeId as record_type_id,
        CreatedDate as created_on, 
        Created_from_Form__c as created_from_form,                      
        Name as fp_no,

        Grade__c as assessment_grade,
        CAST(Academic_Year__c as STRING) as assessment_academic_year,
        Batch_Id__c as assessment_batch_id,

        Q_17__c as q17, Q_17_Ans__c as q17_marks,
        
        Q_18_1__c as q18_1, Q_18_2__c as q18_2, Q_18_3__c as q18_3, Q_18_4__c as q18_4, Q_18_5__c as q18_5, Q_18_6__c as q18_6, Q_18_7__c as q18_7, 
        Q_18_8__c as q18_8, Q_18_9__c as q18_9, Q_18_10__c as q18_10, Q_18_11__c as q18_11, Q_18_Ans__c as q18_marks,
        
        Q_19__c as q19, Q_19_Ans__c as q19_marks,
        Q_20__c as q20, Q_20_Ans__c as q20_marks,
        Q_21__c as q21, Q_21_Ans__c as q21_marks,
        Q_22__c as q22, Q_22_Ans__c as q22_marks,

        (Q_17_Ans__c + Q_18_Ans__c + Q_19_Ans__c + Q_20_Ans__c + Q_21_Ans__c + Q_22_Ans__c) as fp_total_marks,
        
        F_1__c as f1, F_2__c as f2, F_3__c as f3, F_4__c as f4, F_5__c as f5, F_6__c as f6, F_7__c as f7, F_8__c as f8, F_9__c as f9, F_10__c as f10, 
        F_11__c as f11, F_12__c as f12,

        Error_Status__c as error_status, 
        Data_Clean_up__c as data_cleanup,
        Marks_Recalculated__c as marks_recalculated,
        Student_Linked__c as student_linked
        

    from t0 
),

t2 as (select record_type_id,record_type from {{ ref('seed_recordtype') }}),
t3 as (select fp_id, assessment_barcode, record_type, created_on, created_from_form, fp_no,

(case 

when fp_no is not null and (q17 is not null or q18_1 is not null or q18_2 is not null or q18_3 is not null or q18_4 is not null or q18_5 is not null or 
q18_6 is not null or q18_7 is not null or q18_8 is not null or q18_9 is not null or q18_10 is not null or q18_11 is not null or q19 is not null or 
q20 is not null or q21 is not null or q22 is not null or f1 is not null or f2 is not null or f3 is not null or f4 is not null or f5 is not null or 
f6 is not null or f7 is not null or f8 is not null or f9 is not null or f10 is not null or f11 is not null or f12 is not null) then 1 

when fp_no is not null and (q17 is null and q18_1 is null and q18_2 is null and q18_3 is null and q18_4 is null and q18_5 is null and 
q18_6 is null and q18_7 is null and q18_8 is null and q18_9 is null and q18_10 is null and q18_11 is null and q19 is null and 
q20 is null and q21 is null and q22 is null and f1 is null and f2 is null and f3 is null and f4 is null and f5 is null and 
f6 is null and f7 is null and f8 is null and f9 is null and f10 is null and f11 is null and f12 is null) then 0 end) is_non_null,

t1.* except(fp_id, assessment_barcode, record_type_id, created_on, created_from_form, fp_no) 

    from 
        t1
        left join t2 using (record_type_id) order by assessment_barcode, record_type
),

t4 AS (
    SELECT 
        t3.*,
        CASE 
            WHEN q21 IS NOT NULL AND q21 != '*' THEN q21_marks
            ELSE 3
        END AS q21_null,
        
        CASE
            WHEN 
                CASE WHEN q21 IS NOT NULL AND q21 != '*' THEN q21_marks ELSE 3 
                END = 1 THEN '1. Yes'
            WHEN 
                CASE WHEN q21 IS NOT NULL AND q21 != '*' THEN q21_marks 
                    ELSE 3 
                END = 0.5 THEN '3. Other'
            WHEN 
                CASE 
                    WHEN q21 IS NOT NULL AND q21 != '*' THEN q21_marks 
                    ELSE 3 
                END = 0 THEN '2. No'
            ELSE '4. DNA'
        END AS q21_bucket
from t3
)

select *
from t4