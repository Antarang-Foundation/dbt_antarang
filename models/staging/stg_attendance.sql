with source as (
    select * from {{ source('salesforce', 'Session_Attendance__c') }}
),

renamed as (
    select
        id as attendance_id,
        contact__c as attendance_student_id,
        session__c as attendance_session_id,
        Name as attendance_no,
        attendance__c as attendance_status,
        /* Status__c as status, */
        date__c as attendance_date,
        Time__c as attendance_time,
        Guardian_Parent_Attendance__c as guardian_attendance,
        /* Career_Aspiration__c as career_aspiration,
        Reason__c as reason,
        First_step_after_Class_10__c as first_step_after_class_10 */

    from source
)

select * from renamed
where attendance_status is not null
order by attendance_student_id, attendance_session_id
