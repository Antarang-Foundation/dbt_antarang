with t1 as (select * from {{ref('stg_student')}})

select * from t1 where g9_barcode is null and g10_barcode is null and g11_barcode is null and g12_barcode is null 

order by student_id