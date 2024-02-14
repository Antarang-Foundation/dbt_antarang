with
    students as (select * except (academic_year, record_type_id) from {{ ref('int_students') }}),
    batch as (select batches_id, school_id, school_district from {{ ref('stg_batches') }}),
    account as (select * except (record_type_id, school_academic_year, account_district) from {{ ref('stg_accounts') }}),
    unpivot_barcode AS (
        SELECT 
            * except (student_barcode),
            (CASE 
            WHEN field='g9_barcode' THEN 'Grade 9'
            WHEN field='g10_barcode' THEN 'Grade 10'
            WHEN field='g11_barcode' THEN 'Grade 11'
            WHEN field='g12_barcode' THEN 'Grade 12'
            ELSE null END)
            as bar_grade
        FROM students
        UNPIVOT (barcode FOR field IN (g9_barcode, g10_barcode, g11_barcode, g12_barcode))
    ),    
    unpivot_batchcode AS (
        SELECT 
            contact_id as contact, batch_grade, batchcode, student_barcode,
            (CASE 
            WHEN batch_grade='g9_batch_code' THEN 'Grade 9'
            WHEN batch_grade='g10_batch_code' THEN 'Grade 10'
            WHEN batch_grade='g11_batch_code' THEN 'Grade 11'
            WHEN batch_grade='g12_batch_code' THEN 'Grade 12'
            ELSE null END)
            as grade
        FROM students
        UNPIVOT (batchcode FOR batch_grade IN (g9_batch_code, g10_batch_code, g11_batch_code, g12_batch_code)) as y
    ),
    joined AS (
        SELECT * except (student_barcode, bar_grade, contact, g9_batch_code, g10_batch_code, g11_batch_code, g12_batch_code, field, batch_grade)
        FROM unpivot_barcode
        LEFT JOIN unpivot_batchcode ON unpivot_barcode.contact_id=unpivot_batchcode.contact AND unpivot_barcode.bar_grade=unpivot_batchcode.grade
    ),
    join_batch as (
        
        SELECT *
        FROM joined
        LEFT JOIN batch ON joined.batchcode=batch.batches_id
    ),
    join_account as (
        select
            * except (school_id)
        from join_batch
        Left Join account on join_batch.school_id=account.school_id
    )

select *
from join_account