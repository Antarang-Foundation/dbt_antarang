with source as (

    select * from {{ source('salesforce', 'Account') }}

),

renamed as (

    select
    
        *
    from source

)

select * from renamed
--where student_barcode = '220069713'