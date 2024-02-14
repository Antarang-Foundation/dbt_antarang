with
    batches as (select * from {{ ref('stg_batches') }}),
    accounts as (select * from {{ ref('stg_accounts') }}),
    int_account_batch as (
        select *
        from 
            batches
            left join accounts using (school_id)
    )
    
select *
from int_account_batch

