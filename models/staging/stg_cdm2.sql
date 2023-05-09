

with source as (

    select * from {{ source('salesforce', 'src_CDM2__c') }}

),

renamed as (

    select
    
            Id as cdm2_id,
            Barcode__c as student_barcode,
            RecordTypeId as record_type_id,
            CreatedDate as created_on,
            Q6_1_Marks__c as q6_1,
            Q6_2_Marks__c as q6_2,
            Q6_3_Marks__c as q6_3,
            Q6_4_Marks__c as q6_4,
            Q6_5_Marks__c as q6_5,
            Q6_6_Marks__c as q6_6,
            Q6_7_Marks__c as q6_7,
            Q6_8_Marks__c as q6_8,
            Q6_9_Marks__c as q6_9,
            Q6_10_Marks__c as q6_10,
            Q6_11_Marks__c as q6_11,
            Q6_12_Marks__c as q6_12,
            X5_Confident_about_chosen_career__c as q5_marks,
            --X6_Options_that_fit_into_Industry__c as temp,
            ((Q6_1_Marks__c + Q6_2_Marks__c + Q6_3_Marks__c + Q6_4_Marks__c + Q6_5_Marks__c + Q6_6_Marks__c + Q6_7_Marks__c + Q6_8_Marks__c + Q6_9_Marks__c + Q6_10_Marks__c + Q6_11_Marks__c + Q6_12_Marks__c) / 12) as q6_marks

    from source
   
)

select * from renamed