with
    cdm2 as (select * from {{ ref('int_cdm2_pivot') }}),
    cdm1 as (select * from {{ ref('int_pivot_cdm1_latest_normalised') }}),
    int_cdm1_cdm2 as (
        select *
        from 
            cdm1
            left join cdm2 using (student_barcode)
    )

select *
from int_cdm1_cdm2