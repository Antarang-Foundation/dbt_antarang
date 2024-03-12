with
    int_sessions as (
        select 
            contact_id, full_name as student_name, contact_batch_id, session_batch_id, contact_barcode, 
            count(distinct case when attendance_status = 'Present' then session_id else null end) as attendance_count
        from {{ ref('int_session')}} 
        group by contact_id, full_name, contact_batch_id, session_batch_id, contact_barcode
    ),
    int_batch_sessions_conducted as (
        select * from {{ ref('int_batch_sessions_conducted') }}
    ),
    int_attendance as (
        select *
        from int_sessions
            full outer join int_batch_sessions_conducted using (session_batch_id)
    ),
    select_columns as (
        select
            contact_id,
            contact_barcode,
            student_name,
            batch_grade,
            session_batch_id,
            total_sessions,
            attendance_count,
            round(100*safe_divide(attendance_count, total_sessions), 1) as percent_attendance
        from int_attendance
    )

select *
from select_columns