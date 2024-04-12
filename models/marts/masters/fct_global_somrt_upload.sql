with t1 as (select * from {{ref('int_global')}}),
t2 as(select * from {{ref('stg_somrt')}}),
t3 as (select * from t1 full outer join t2 on t1.batch_id = t2.somrt_batch_id order by batch_id, omr_type), 

t4 as (select batch_id, count(*) `total_students`, count(distinct bl_cdm1_no) `bl_cdm1`, count(distinct bl_cdm2_no) `bl_cdm2`, 
count(distinct bl_cp_no) `bl_cp`, count(distinct bl_cs_no) `bl_cs`, count(distinct bl_fp_no) `bl_fp`, count(distinct el_cdm1_no) `el_cdm1`, 
count(distinct el_cdm2_no) `el_cdm2`, count(distinct el_cp_no) `el_cp`, count(distinct el_cs_no) `el_cs`, count(distinct el_fp_no) `el_fp`, 
count(distinct saf_no) `saf`, count(distinct sar_no) `sar` from {{ref('fct_student_global_assessment_status')}} group by batch_id order by batch_id),

t5 as (
    
select t3.*,

(case 
when t3.omr_type = 'Self Awareness + Feedback' or t3.omr_type = 'Quiz + Feedback' then t4.saf
when t3.omr_type = 'Realities + Quiz' then t4.sar
when t3.omr_type = 'Career Decision Making- 1A' then t4.bl_cdm1
when t3.omr_type = 'Career Decision Making- 1B' then t4.el_cdm1
when t3.omr_type = 'Career Decision Making- 2A' then t4.bl_cdm2
when t3.omr_type = 'Career Decision Making- 2B' then t4.el_cdm2
when t3.omr_type = 'Career Planning A' then t4.bl_cp
when t3.omr_type = 'Career Planning B' then t4.el_cp
when t3.omr_type = 'Career Skills A' then t4.bl_cs
when t3.omr_type = 'Career Skills B' then t4.el_cs
when t3.omr_type = 'Planning for Future A' then t4.bl_fp
when t3.omr_type = 'Planning for Future B' then t4.el_fp
when t3.omr_type = 'Planning for Future B' then t4.el_fp
when t3.omr_type = 'Student Details' then t4.total_students
end) `omr_upload_count`


 from t3 full outer join t4 on t3.batch_id = t4.batch_id order by t3.batch_id, t3.omr_type)

 select * from t5