with
    batches as (select * from {{ ref('stg_batch') }}),
    sessions as (select * from {{ ref('stg_sessions') }}),
    int_batches_sessions_conducted as (
        select *
        from batches
        left join sessions using (batches_id)
    ),
    get_total_sessions_by_batch_and_grade as (
        select
            batches_id,
            batches_grade,
            count(sessions_number) as total_sessions
        from int_batches_sessions_conducted
        where sessions_type = 'Student'
        group by batches_id, batches_grade
    )

select *
from get_total_sessions_by_batch_and_grade
