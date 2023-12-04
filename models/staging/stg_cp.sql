

with source as (

    select * from {{ source('salesforce', 'Career_Planning__c') }}

),

renamed as (

    select

        Q_7_Marks__c as q7_marks,
        Q_8_Marks__c as q8_marks,
        Q_9_1_Marks__c as q9_1_marks,
        Q_9_2_Marks__c as q9_2_marks,
        Q_9_3_Marks__c as q9_3_marks,
        Q_9_4_Marks__c as q9_4_marks,
        Q_9_5_Marks__c as q9_5_marks,
        Q_9_6_Marks__c as q9_6_marks,
        Q_9_7_Marks__c as q9_7_marks,
        Q_10_Marks__c as q10_marks,
        RecordTypeID as record_type_id,
        CAST(Barcode__c as STRING) as student_barcode,
        Grade__c as grade,
        CAST(Academic_Year__c as STRING) as academic_year,
        CreatedDate as created_on,
        Error_Status__c as error_status,
        (Q_7_Marks__c
        +Q_8_Marks__c
        +Q_9_1_Marks__c
        +Q_9_2_Marks__c
        +Q_9_3_Marks__c
        +Q_9_4_Marks__c
        +Q_9_5_Marks__c
        +Q_9_6_Marks__c
        +Q_9_7_Marks__c
        +Q_10_Marks__c) as cp_total
            
    from source
   
)

select * from renamed
where error_status="No Error"