with source as (
    select * from {{ source('salesforce', 'Future_Planning__c') }}
),

renamed as (
    select
        Id as fp_id,
        Q_17_Ans__c as q17_marks,
        Q_18_Ans__c as q18_marks,
        Q_19_Ans__c as q19_marks,
        Q_20_Ans__c as q20_marks,
        Q_21_Ans__c as q21_marks,
        Q_22_Ans__c as q22_marks,
        RecordTypeID as record_type_id,
        CAST(Barcode__c as STRING) as student_barcode,
        Grade__c as grade,
        CAST(Academic_Year__c as STRING) as academic_year,
        CreatedDate as created_on,
        Error_Status__c as error_status,
        (
            Q_17_Ans__c
            + Q_18_Ans__c
            + Q_19_Ans__c
            + Q_20_Ans__c
            + Q_21_Ans__c
            + Q_22_Ans__c
        ) as fp_total
    from source
)

select * from renamed
where error_status="No Error"