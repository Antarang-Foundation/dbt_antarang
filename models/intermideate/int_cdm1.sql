
{{ config(materialized="view") }}

with
    cdm1_marks as (select * from {{ ref("stg_cdm1") }}),

    relevant_questions as (
        select
            X1_A_good_career_plan_has_the_following__c as q1_marks,
            Interest_Marks__c as q2_marks,
            Aptitude_Marks__c as q3_marks,
            Career_Choice_Total_Marks__c as q4_marks
        from source1
  
       
    )

    
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



