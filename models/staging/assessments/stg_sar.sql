with t0 as (
    select * from {{ source('salesforce', 'Self_Awareness_Realities__c') }}
),

t1 as (
    select

        Id as sar_id,
        Barcode__c as barcode,
        RecordTypeId as record_type_id,
        CreatedDate as created_on,                       
        Name as sar_no,

        Quiz_2_1__c as sar_q1, Quiz_2_1_Marks__c as sar_q1_marks, Quiz_2_2__c as sar_q2, Quiz_2_2_Marks__c as sar_q2_marks, 
        Quiz_2_3__c as sar_q3, Quiz_2_3_Marks__c as sar_q3_marks, Quiz_2_4__c as sar_q4, Quiz_2_4_Marks__c as sar_q4_marks, 
        Quiz_2_5__c as sar_q5, Quiz_2_5_Marks__c as sar_q5_marks,  
        
        Reality1_Self_1__c as reality1_self1, Reality1_Self_1_Ans__c  as reality1_self1_marks, 
        Reality_2_Self_2__c as reality2_self2, Reality_2_Self_2_Ans__c as reality2_self2_marks, 
        Reality_3_Self_3__c as reality3_self3, Reality_3_Self_3_Ans__c  as reality3_self3_marks, 
        Reality_4_Self_4__c as reality4_self4, Reality_4_Self_4_Ans__c as reality4_self4_marks, 
        Reality_5_Family_1__c as reality5_family1, Reality_5_Family_1_Ans__c as reality5_family1_marks, 
        Reality_6_Family_2__c as reality6_family2, Reality_6_Family_2_Ans__c as reality6_family2_marks, 
        Reality_7_Family_3__c as reality7_family3, Reality_7_Family_3_Ans__c as reality7_family3_marks, 
        Reality_8_Family_4__c as reality8_family4, Reality_8_Family_4_Ans__c as reality8_family4_marks,
        
        Quiz_2_Form_Submitted__c as q2_form_submitted, Reality_Form_Submitted__c as reality_form_submitted,

        Grade__c as assessment_grade,
        CAST(Academic_Year__c as STRING) as assessment_academic_year,
        Batch_Id__c as assessment_batch_id,

        Error_Status__c as error_status, 
        Created_from_Form__c as created_from_form,
        Data_Clean_up__c as data_cleanup,
        Marks_Recalculated__c as marks_recalculated,
        Student_Linked__c as student_linked, 
        

    from t0 
),

t2 as (select record_type_id,record_type from {{ ref('stg_recordtypes') }}),
stg_sar as (
    select 
        sar_id,
        barcode,
        record_type,
        t1.* except(sar_id, barcode, record_type_id) 

    from 
        t1
        left join t2 using (record_type_id) order by barcode, record_type
    )

select *
from stg_sar