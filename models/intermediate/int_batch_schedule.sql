with
    account_batch as (select * from {{ ref('int_global') }}),
    sessions as (select * from {{ref ('stg_sessions') }}),
    trainers as (select * from {{ ref('stg_trainer') }}),

    int_batch_schedule as (
         select * from sessions, 
         select * from account_batch,
         full_name as facilitator

        from 
            account_batch
              left join sessions on account_batch.batches_id = sessions.batches_id
              left join trainers on account_batch.assigned_facilitator_id = trainers.contact_id

    )