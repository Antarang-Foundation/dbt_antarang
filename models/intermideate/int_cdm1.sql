
{{ config(materialized="view") }}

with
    cdm1 as (select * from {{ ref("stg_cdm1") }}),
    
    

    
select *
from relevant_questions


with
    source2 as (select * from {{ref("stg_recordtypes")}}),

    record_type as (
        select
            Name as assessment_type
            id as record_type_id

    )
select *
from record_type



