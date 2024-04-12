with t1 as (select * from {{ref('stg_student')}})

select * from t1 where g9_barcode = g10_barcode or g10_barcode = g11_barcode or g11_barcode = g12_barcode

order by student_id