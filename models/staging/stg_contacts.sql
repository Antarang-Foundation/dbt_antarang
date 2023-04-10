with source as (

    select * from {{ source('salesforce', 'src_Contact') }}

),

renamed as (

    select
        Id as contact_id,
        FirstName as first_name,
        LastName as last_name,
        Bar_Code__c as student_barcode,
        Contact_Type__c as contact_type

    from source

)

select * from renamed
where contact_type = 'Student'