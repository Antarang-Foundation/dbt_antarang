

with source as (

    select * from {{ source('salesforce', 'Career_Skill__c') }}

),

renamed as (

    select
        Q_11_Ans__c as q11_marks,
        Q_12_Ans__c as q12_marks,
        Q_13_Ans__c as q13_marks,
        Q_14_Ans__c as q14_marks,
        Q_15_Ans__c as q15_marks,
        Q_16_Ans__c as q16_marks,
        RecordTypeID as record_type_id,
        CAST(Barcode__c as STRING) as student_barcode,
        Grade__c as grade,
        CAST(Academic_Year__c as STRING) as academic_year,
        CreatedDate as created_on,
        Error_Status__c as error_status,
        (
            Q_11_Ans__c
            +Q_12_Ans__c
            +Q_13_Ans__c
            +Q_14_Ans__c
            +Q_15_Ans__c
            +Q_16_Ans__c
        ) as cs_total
            
    from source
   
)

select * from renamed
where error_status="No Error"