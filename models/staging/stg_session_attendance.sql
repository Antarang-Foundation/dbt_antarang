with source as (
    select * from {{ source('salesforce', 'Session_Attendance__c') }}
),

renamed as (
    select
        id as session_attendance_id,
        session__c as sessions_id,
        contact__c as contact_id,
        Name as sa_number,
        attendance__c as attendance_status,
        date__c as session_atendance_date,
        Time__c as session_atendance_time,
        Status__c as status,
        Guardian_Parent_Attendance__c as guardian_parent_Attendance,
        Career_Aspiration__c as career_aspiration,
        Reason__c as reason,
        First_step_after_Class_10__c as first_step_after_class_10

    from source
)

select * from renamed
where attendance_status is not null
