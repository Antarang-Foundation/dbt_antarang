with
    int_sessions as (
        select 
            contact_id, 
            full_name, 
            count(distinct case when attendance_status = 'Present' then sessions_id else null end) as attendance_count, 
            batches_id, 
            student_barcode 
        from {{ ref('int_sessions')}} 
        group by contact_id, batches_id, student_barcode, full_name
    ),
    int_batches_sessions_conducted as (
        select * from {{ ref('int_batches_sessions_conducted') }}
    ),
    int_attendance as (
        select *
        from int_sessions
            left join int_batches_sessions_conducted using (batches_id)
    ),
    select_columns as (
        select
            contact_id,
            student_barcode,
            full_name,
            batches_grade,
            attendance_count,
            total_sessions,
            safe_divide(attendance_count, total_sessions) as percentage_attendance
        from int_attendance
    )

select *
from select_columns