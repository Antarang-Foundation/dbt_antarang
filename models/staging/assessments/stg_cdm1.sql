with 

t0 as (
    select * from {{ source('salesforce', 'OMR_Assessment__c') }}
),

t1 as (
    select
            Id as cdm1_id,
            Barcode__c as assessment_barcode,
            RecordTypeId as record_type_id,
            CreatedDate as created_on,
            Created_from_Form__c as created_from_form,                       
            Name as cdm1_no,

            Grade__c as assessment_grade,
            CAST(Academic_Year__c as STRING) as assessment_academic_year,
            Batch_Id__c as assessment_batch_id,

            Q_1__c as q1,
            X1_A_good_career_plan_has_the_following__c as q1_marks,

            Interest_1__c as q2_1, 
            Interest_2__c as q2_2,  
            Interest_Marks__c as q2_marks,

            Aptitude_1__c as q3_1, 
            Aptitude_2__c as q3_2,
            Aptitude_Marks__c as q3_marks,

            Career_Choice_1__c as q4_1,
            Career_Choice_1_Marks__c as q4_1_marks,
            Career_Choice_2__c as q4_2,
            Career_Choice_2_Marks__c as q4_2_marks,
            Career_Choice_Total_Marks__c as q4_marks,


            (X1_A_good_career_plan_has_the_following__c + Interest_Marks__c + Aptitude_Marks__c + Career_Choice_Total_Marks__c) as cdm1_total_marks,

            Error_Status__c as error_status, 
            Data_Clean_up__c as data_cleanup,
            Marks_Recalculated__c as marks_recalculated,
            Student_Linked__c as student_linked 

    from t0 
),

t2 as (select record_type_id, record_type from {{ ref('seed_recordtype') }}),

t3 as (select cdm1_id, assessment_barcode, record_type, created_on, created_from_form, cdm1_no,

(case 

when cdm1_no is not null and (q1 is not null or q2_1 is not null or q2_2 is not null or q3_1 is not null or q3_2 is not null or q4_1 is not null 
or q4_2 is not null) then 1 
when cdm1_no is not null and (q1 is null and q2_1 is null and q2_2 is null and q3_1 is null and q3_2 is null and q4_1 is null and q4_2 is null) 
then 0 end) is_non_null,

t1.* except(cdm1_id, assessment_barcode, record_type_id, created_on, created_from_form, cdm1_no) 

from t1 left join t2 using (record_type_id) order by assessment_barcode, record_type)

select * from t3 