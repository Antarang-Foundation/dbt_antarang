with 

t1 as (select * from {{ref('stg_student')}}),
t2 as (select first_barcode from t1 group by first_barcode having count(*)>1),
t3 as (select * from t1 where first_barcode in (select * from t2) order by first_barcode)

select * from t3

