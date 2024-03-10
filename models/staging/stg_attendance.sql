with source as (
    select * from {{ source('salesforce', 'Session_Attendance__c') }}
),

renamed as (
    select
        id as attendance_id,
        session__c as session_id,
        contact__c as contact_id,
        Name as attendance_no,
        attendance__c as attendance_status,
        date__c as attendance_date,
        Time__c as attendance_time,
        Status__c as status,
        Guardian_Parent_Attendance__c as guardian_attendance,
        Career_Aspiration__c as career_aspiration,
        Reason__c as reason,
        First_step_after_Class_10__c as first_step_after_class_10

    from source
)

select * from renamed
where attendance_status is not null
order by attendance_id, session_id, contact_id
