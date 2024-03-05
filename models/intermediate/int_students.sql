with
    contacts as (select * from {{ ref('stg_students') }}),
    recordtypes as (select record_type_id,record_type from {{ ref('stg_recordtypes') }}),
    int_students as (
        select *
        from 
            contacts
            left join recordtypes using (record_type_id)
    )
    
select *
from int_students
where record_type='CA Student'