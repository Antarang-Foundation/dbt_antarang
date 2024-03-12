with
    batches as (select * from {{ ref('stg_batch') }}),
    sessions as (select * from {{ ref('stg_session') }}),
    batch_sessions as (
        select *
        from batches
        full outer join sessions on batches.batch_id = sessions.session_batch_id
    ),
    int_batch_sessions_conducted as (
        select
            batch_grade, batch_id, session_batch_id, batch_no, facilitator_id as batch_facilitator_id, session_facilitator_id,  
            count(session_no) as total_sessions
        from batch_sessions
        where session_type = 'Student'
        group by batch_grade, batch_id, session_batch_id, batch_no, facilitator_id, session_facilitator_id
        order by batch_grade, batch_id, session_batch_id, batch_no, facilitator_id, session_facilitator_id

    )

select *
from int_batch_sessions_conducted
