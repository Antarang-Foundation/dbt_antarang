

with source as (

    select * from {{ source('salesforce', 'src_Session_Attendance__c') }}

),

renamed as (

    select
        id as session_attendance_id,
        session__c as session_id,
        name as session_name,
        date__c as session_atendance_date,
        attendance__c as attendance_status,
        contact__c as contact_id
      

    from source

)

select * from renamed
