with
    batches as (select * from {{ ref('stg_batches') }}),
    sessions as (select * from {{ ref('stg_sessions') }}),

int_batches_sessions_conducted as (

    select *
    from batches
    left join sessions using (batches_id)
    
)
select batches_id, Count(sessions_number) as total_sessions
from int_batches_sessions_conducted
where sessions_type = 'Student'
group by batches_id
