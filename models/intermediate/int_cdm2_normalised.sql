with source as (

    select * from {{ ref('stg_cdm2') }}

),

renamed as (

    select
    
            cdm2_id,
            student_barcode,
            record_type_id,
            created_on,
            q5_marks,
            ((q6)/12) as q6_marks,
            (q5_marks + ((q6)/12)) as total_marks_cdm2

    from source
   
)

select * from renamed