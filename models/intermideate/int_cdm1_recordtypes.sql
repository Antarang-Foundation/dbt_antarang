
{{ config(materialized="view") }}

with
    cdm1 as (select * from {{ ref('stg_cdm1') }}),
    recordtypes as (select record_type_id,record_type from {{ ref('stg_recordtypes') }}),
    
    

int_cdm1_recordtypes as (
    
    select *
    from 
        cdm1
        left join recordtypes using (record_type_id)

)
select *
from int_cdm1_recordtypes
