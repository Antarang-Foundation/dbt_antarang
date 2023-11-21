

with source as (

    select * from {{ source('salesforce', 'RecordType') }}

),

renamed as (

    select
    
        id as record_type_id,
        name as record_type

    from source

)

select * from renamed
where record_type in ('Baseline','Endline','Baseline Assessment','Endline Assessment')