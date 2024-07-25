with 

t1 as (select * from {{ref('int_global')}}),

t2 as (select * from {{ref('stg_somrt')}}),

t3 as (select session_id, session_facilitator_id, omr_required, omrs_received, total_student_present, total_parent_present, attendance_count, 
present_count from {{ref('stg_session')}}),

t4 as (select * from t2 INNER JOIN t3 on t2.somrt_session_id = t3.session_id),
t5 as (select * from t1 INNER JOIN t4 on t1.batch_id = t4.somrt_batch_id order by batch_id, omr_type), 

/* t6 as (select batch_id, count(*) `total_students`, count(distinct bl_cdm1_no) `bl_cdm1`, count(distinct bl_cdm2_no) `bl_cdm2`, 
count(distinct bl_cp_no) `bl_cp`, count(distinct bl_cs_no) `bl_cs`, count(distinct bl_fp_no) `bl_fp`, count(distinct el_cdm1_no) `el_cdm1`, 
count(distinct el_cdm2_no) `el_cdm2`, count(distinct el_cp_no) `el_cp`, count(distinct el_cs_no) `el_cs`, count(distinct el_fp_no) `el_fp`, 
count(distinct saf_no) `saf`, count(distinct sar_no) `sar` from {{ref('int_student_global_assessment_status')}} group by batch_id order by batch_id), */


t6 as (select  batch_id , sum(distinct no_of_students_facilitated) as `total_students`, count(distinct bl_cdm1_no) `bl_cdm1`, count(distinct bl_cdm2_no) `bl_cdm2`, 
count(distinct bl_cp_no) `bl_cp`, count(distinct bl_cs_no) `bl_cs`, count(distinct bl_fp_no) `bl_fp`, count(distinct el_cdm1_no) `el_cdm1`, 
count(distinct el_cdm2_no) `el_cdm2`, count(distinct el_cp_no) `el_cp`, count(distinct el_cs_no) `el_cs`, count(distinct el_fp_no) `el_fp`, 
count(distinct saf_no) `saf`, count(distinct sar_no) `sar` from {{ref('int_student_global_assessment_status')}} group by batch_id order by batch_id),

t7 as (
    
select t5.*,

(case 
when t5.omr_type = 'Self Awareness + Feedback' or t5.omr_type = 'Quiz + Feedback' then t6.saf
when t5.omr_type = 'Realities + Quiz' then t6.sar
when t5.omr_type = 'Career Decision Making- 1A' then t6.bl_cdm1
when t5.omr_type = 'Career Decision Making- 1B' then t6.el_cdm1
when t5.omr_type = 'Career Decision Making- 2A' then t6.bl_cdm2
when t5.omr_type = 'Career Decision Making- 2B' then t6.el_cdm2
when t5.omr_type = 'Career Planning A' then t6.bl_cp
when t5.omr_type = 'Career Planning B' then t6.el_cp
when t5.omr_type = 'Career Skills A' then t6.bl_cs
when t5.omr_type = 'Career Skills B' then t6.el_cs
when t5.omr_type = 'Planning for Future A' then t6.bl_fp
when t5.omr_type = 'Planning for Future B' then t6.el_fp
/* when t5.omr_type = 'Planning for Future B' then t6.el_fp */
when t5.omr_type = 'Student Details' then t6.total_students
end) `omr_upload_count`


 /* from t5 full outer join t6 on t5.batch_id = t6.batch_id order by t5.batch_id, t5.omr_type) */
 from t5 INNER JOIN t6 on t5.batch_id = t6.batch_id order by t5.batch_id, t5.omr_type)

 select * from t7