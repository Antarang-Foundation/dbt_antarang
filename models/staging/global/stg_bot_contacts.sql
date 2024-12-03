with bot as (
    select id, phone
    from {{source ('salesforce', 'contacts')}}
)

select * from bot

