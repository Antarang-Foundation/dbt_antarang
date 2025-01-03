with t1 as (
    select * from {{ref('stg_bot_flow_results')}}
)

select * from t1