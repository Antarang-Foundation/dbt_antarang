with source as (
    select * from {{ source('salesforce', 'CDM2__c') }}
),

renamed as (
    select
            Id as cdm2_id,
            Barcode__c as student_barcode,
            RecordTypeId as record_type_id,
            CreatedDate as created_on,
            X5_Confident_about_chosen_career__c as q5_marks,
            X6_Options_that_fit_into_Industry__c as q6,
            Grade__c as grade,
            CAST(Academic_Year__c as STRING) as academic_year,
            Error_Status__c as error_status,
            (X5_Confident_about_chosen_career__c +  X6_Options_that_fit_into_Industry__c) as cdm2_total
    from source
)

select * from renamed
where error_status="No Error"