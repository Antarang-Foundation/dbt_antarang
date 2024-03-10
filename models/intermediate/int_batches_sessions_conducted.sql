with
    batches as (select * from {{ ref('stg_batch') }}),
    sessions as (select * from {{ ref('stg_session') }}),
    batch_sessions as (
        select *
        from batches
        left join sessions using (batch_id)
    ),
    int_batch_sessions_conducted as (
        select
            batch_id, batch_no, batch_grade,
            count(session_no) as total_sessions
        from batch_sessions
        where session_type = 'Student'
        group by batch_id, batch_no, batch_grade
        order by batch_id, batch_no, batch_grade

    )

select *
from int_batch_sessions_conducted
