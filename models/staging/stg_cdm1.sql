with source as (
    select * from {{ source('salesforce', 'OMR_Assessment__c') }}
),

renamed as (
    select
            Id as cdm1_id,
            Barcode__c as student_barcode,
            RecordTypeId as record_type_id,
            CreatedDate as created_on,
            X1_A_good_career_plan_has_the_following__c as q1_career_plan_marks,
            Interest_Marks__c as q2_interest_marks,
            Aptitude_Marks__c as q3_aptitude_marks,
            Career_Choice_Total_Marks__c as q4_career_choice_marks,
            (X1_A_good_career_plan_has_the_following__c + Interest_Marks__c + Aptitude_Marks__c + Career_Choice_Total_Marks__c) as total_marks,
            Grade__c as grade,
            CAST(Academic_Year__c as STRING) as academic_year,
            Error_Status__c as error_status
    from source
)

select * from renamed
where error_status="No Error"