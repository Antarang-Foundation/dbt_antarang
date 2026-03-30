with ranked as (

    select
        *,
        row_number() over (
            partition by attendance_student_id, attendance_session_id
            order by attendance_date desc, attendance_time desc
        ) as rn
    from {{ ref('stg_attendance') }}

)

select *
from ranked
where rn = 1