with source as (
    select * from {{ source('salesforce', 'Session_Attendance__c') }}
),

renamed as (
    select
        id as session_attendance_id,
        session__c as sessions_id,
        date__c as session_atendance_date,
        attendance__c as attendance_status,
        contact__c as contact_id
    from source
)

select * from renamed
where attendance_status is not null
