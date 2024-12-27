with bot as (
    select phone,
    last_message_at, inserted_at, updated_at
    from {{source ('salesforce', 'contacts')}}
)

select * from bot

