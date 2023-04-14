with source as (

    select * from {{ source('salesforce', 'src_Contact') }}

),

renamed as (

    select
        Id as contact_id,
        FirstName as first_name,
        LastName as last_name,
        Current_OMR_Barcode__c as student_barcode,
        RecordTypeId

    from source

)

select * from renamed