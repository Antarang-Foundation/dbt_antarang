with source as (
    select * from {{ source('salesforce', 'Session_Attendance__c') }}
),

renamed as (
    select
        id as session_attendance_id,
        session__c as sessions_id,
        contact__c as contact_id,
        Name as SA_Number,
        attendance__c as attendance_status,
        date__c as session_atendance_date,
        Time__c as session_atendance_time,
        Status__c as Status,
        Guardian_Parent_Attendance__c as Guardian_Parent_Attendance,
        Career_Aspiration__c as Career_Aspiration,
        Reason__c as Reason,
        First_step_after_Class_10__c as First_step_after_Class

    from source
)

select * from renamed
where attendance_status is not null
