
{{ config(materialized="view") }}

with
    cdm1 as (select * from {{ ref('stg_cdm1') }}),
    recordtypes as (select * from {{ ref('stg_recordtypes') }}),
    
    

int_cdm1_recordtypes as (
    
    select *
    from cdm1

    left join 
        recordtypes
    on 
        cdm1.record_type_id = recordtypes.record_type_id

)
select *
from int_cdm1_recordtypes
