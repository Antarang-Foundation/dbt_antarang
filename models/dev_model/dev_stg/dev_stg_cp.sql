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
        Student_Linked__c as student_linked
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
        case 
            when cp_no is not null and (
                q7 is not null or q8 is not null or q9_1 is not null or q9_2 is not null 
                or q9_3 is not null or q9_4 is not null or q9_5 is not null or q9_6 is not null 
                or q9_7 is not null or q10 is not null
            ) then 1
            when cp_no is not null and (
                q7 is null and q8 is null and q9_1 is null and q9_2 is null 
                and q9_3 is null and q9_4 is null and q9_5 is null and q9_6 is null 
                and q9_7 is null and q10 is null
            ) then 0
        end as is_non_null,
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
