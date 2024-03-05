with
    session_attendance as (select * from {{ ref('stg_session_attendance') }}),
    sessions as (select * from {{ ref('stg_sessions') }}),
    contacts as (select * from {{ ref('stg_students') }}),
    int_sessions as (
        select *
        from 
            session_attendance
            left join sessions using (sessions_id)
            left join contacts using (contact_id)
    )

select *
from int_sessions 
where sessions_type = 'Student'