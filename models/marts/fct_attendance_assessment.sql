with
    assessment as (select * from {{ ref('int_assessment') }}),
    attendance as (select * from {{ ref('int_attendance') }}),
    

attendance_assessment as (
    
    select *
    from 
        assessment
        left join attendance using (student_barcode)

)
select *
from attendance_assessment