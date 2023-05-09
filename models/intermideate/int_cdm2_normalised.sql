with source as (

    select * from {{ ref('stg_cdm2') }}

),

renamed as (

    select
    
            cdm2_id,
            student_barcode,
            record_type_id,
            created_on,
            q6_1,
            q6_2,
            q6_3,
            q6_4,
            q6_5,
            q6_6,
            q6_7,
            q6_8,
            q6_9,
            q6_10,
            q6_11,
            q6_12,
            q5_marks,
            --X6_Options_that_fit_into_Industry__c as temp,
            ((q6_1 + q6_2 + q6_3 + q6_4 + q6_5 + q6_6 + q6_7 + q6_8 + q6_9 + q6_10 + q6_11 + q6_12)/12) as q6_marks,
            (q5_marks + q6_marks) as total_marks_cdm2

    from source
   
)

select * from renamed