with source as (
    select * from {{ source('salesforce', 'OMR_Assessment__c') }}
),

renamed as (
    select
            Id as cdm1_id,
            Barcode__c as student_barcode,
            CreatedDate as created_on,
            RecordTypeId as record_type_id,            
            Name as cdm1_no,

            Q_1__c as q1
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

            Grade__c as grade,
            CAST(Academic_Year__c as STRING) as academic_year,
            Batch_Id__c as batch_id,

            Error_Status__c as error_status, 
            Data_Clean_up__c as data_cleanup,
            Marks_Recalculated__c as marks_recalculated,
            Student_Linked__c as student_linked, 
            Created_from_Form__c as created_from_form,

    from source
)

select * from renamed order by student_barcode, record_type_id