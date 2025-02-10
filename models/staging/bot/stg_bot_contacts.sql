with bot as (
    select phone, last_message_at, inserted_at, updated_at
    from {{source ('chatbot', 'contacts')}}
)

select * from bot

