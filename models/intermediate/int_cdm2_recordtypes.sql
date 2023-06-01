
{{ config(materialized="view") }}

with
    cdm2 as (select * from {{ ref('int_cdm2_normalised') }}),
    recordtypes as (select record_type_id,record_type from {{ ref('stg_recordtypes') }}),
    

int_cdm2_recordtypes as (
    
    select *
    from 
        cdm2
        left join recordtypes using (record_type_id)

)
select *
from int_cdm2_recordtypes
