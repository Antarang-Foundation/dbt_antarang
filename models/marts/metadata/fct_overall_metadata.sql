select * from {{ref('fct_global_metadata')}}
union all
select * from {{ref('fct_cdm1_metadata')}}
union all 
select * from {{ref('fct_cdm2_metadata')}}
union all
select * from {{ref('fct_cp_metadata')}}
union all 
select * from {{ref('fct_cs_metadata')}}
union all
select * from {{ref('fct_fp_metadata')}}
union all 
select * from {{ref('fct_quiz_metadata')}}
union all 
select * from {{ref('fct_assessment_raw_metadata')}}
union all
select * from {{ref('fct_assessment_metadata')}}

order by table_name, column_name

