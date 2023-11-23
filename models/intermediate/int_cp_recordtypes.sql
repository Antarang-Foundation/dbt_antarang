
{{ config(materialized="view") }}

with
    cp as (select * from {{ ref('stg_cp') }}),
    recordtypes as (select record_type_id,record_type from {{ ref('stg_recordtypes') }}),
    
    

int_cdm1_recordtypes as (
    
    select *
    from 
        cp
        left join recordtypes using (record_type_id)

)
select *
from int_cdm1_recordtypes