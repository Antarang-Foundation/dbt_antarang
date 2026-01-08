with source as (
    select * from {{ source('salesforce', 'Session_Attendance__c') }} where IsDeleted = false
),

a as (
    select
        id as attendance_id,
        contact__c as attendance_student_id,
        session__c as attendance_session_id,
        Name as attendance_no,
        attendance__c as attendance_status,
        date__c as attendance_date,
        Time__c as attendance_time,
        Guardian_Parent_Attendance__c as guardian_attendance,
        Session_Grade__c as session_att_grade,
        Wrong_Batch__c as wrong_batch
    from source
)

select * from a
order by attendance_student_id, attendance_session_id