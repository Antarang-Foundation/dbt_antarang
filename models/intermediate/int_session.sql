with
    attendance as (select * from {{ ref('stg_attendance') }}),
    sessions as (select * from {{ ref('stg_session') }}),
    students as (select * from {{ ref('int_student') }}),
    int_session as (
        select *
        from 
            attendance
            full outer join sessions using (session_id)
            full outer join students using (contact_id)
    )

select *
from int_session
where session_type = 'Student'