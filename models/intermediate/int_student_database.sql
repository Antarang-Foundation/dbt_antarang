with
    students as (select * from {{ ref('int_students') }}),
    batch as (select * from {{ ref('stg_batches') }}),
    account as (select * from {{ ref('stg_accounts') }}),
    
    

int_students as (
    
    SELECT *
    FROM students
    LEFT JOIN batch ON (
    batches_id = students.g9_batch_code OR
    batches_id = students.g10_batch_code OR
    batches_id = students.g11_batch_code OR
    batches_id = students.g12_batch_code
    )

),
int_student_database as (
    select
        *
    from int_students
    Left Join account on int_students.school_id=account.account_id
)
select *
from int_student_database