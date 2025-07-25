with t1 as (select * from  
{{ref('fct_global_assessment_raw_uploads')}} ---total uploads
where school_district In ('Nagaland', 'Palghar', 'RJ Model B','RJ Model C', 'RJ Model A')), 

t2 as (select batch_no, batch_academic_year, batch_grade, batch_language, fac_start_date, facilitator_name, facilitator_email, school_name, school_academic_year, school_language, 
school_taluka, school_ward, school_district, school_state, school_partner,school_area, batch_donor,


count(distinct student_barcode) `combined_sd`, 
count(distinct assessment_barcode) `combined_barcodes`, 
count(distinct bl_cdm1_no) `bl_cdm1_correct`, 
count(distinct el_cdm1_no) `el_cdm1_correct`, count(distinct bl_cdm2_no) `bl_cdm2_correct`, count(distinct el_cdm2_no) `el_cdm2_correct`, 
count(distinct bl_cp_no) `bl_cp_correct`, count(distinct el_cp_no) `el_cp_correct`, count(distinct bl_cs_no) `bl_cs_correct`, 
count(distinct el_cs_no) `el_cs_correct`, count(distinct bl_fp_no) `bl_fp_correct`, count(distinct el_fp_no) `el_fp_correct`, 
count(distinct saf_no) `saf_correct`, count(distinct sar_no) `sar_correct` 
from {{ref('fct_student_global_assessment_status')}}  ----correct upload
WHERE school_district IN ('Nagaland', 'Palghar', 'RJ Model B','RJ Model C', 'RJ Model A')          
group by batch_no, batch_academic_year, batch_grade, batch_language, fac_start_date, facilitator_name, facilitator_email, school_name, school_academic_year, school_language, school_taluka, 
school_ward, school_district, school_state, school_partner,school_area, batch_donor),

t3 as (select 
coalesce(t1.batch_no, t2.batch_no) as batch_no,
coalesce(t1.batch_academic_year, t2.batch_academic_year) as batch_academic_year,
coalesce(t1.batch_grade, t2.batch_grade) as batch_grade,
coalesce(t1.batch_language, t2.batch_language) as batch_language,
coalesce(t1.fac_start_date, t2.fac_start_date) as fac_start_date,
coalesce(t1.facilitator_name, t2.facilitator_name) as facilitator_name,
coalesce(t1.facilitator_email, t2.facilitator_email) as facilitator_email,
coalesce(t1.school_name, t2.school_name) as school_name,
coalesce(t1.school_academic_year, t2.school_academic_year) as school_academic_year,
coalesce(t1.school_language, t2.school_language) as school_language,
coalesce(t1.school_taluka, t2.school_taluka) as school_taluka,
coalesce(t1.school_ward, t2.school_ward) as school_ward,
coalesce(t1.school_district, t2.school_district) as school_district,
coalesce(t1.school_state, t2.school_state) as school_state,
coalesce(t1.school_partner, t2.school_partner) as school_partner,
coalesce(t1.school_area, t2.school_area) as school_area,
coalesce(t1.batch_donor, t2.batch_donor) as batch_donor,

stg_cdm1_sd, stg_cdm1_barcodes, bl_cdm1_raw, bl_cdm1_correct, el_cdm1_raw, el_cdm1_correct, 
stg_cdm2_sd, stg_cdm2_barcodes, bl_cdm2_raw, bl_cdm2_correct, el_cdm2_raw, el_cdm2_correct,
stg_cp_sd, stg_cp_barcodes, bl_cp_raw, bl_cp_correct, el_cp_raw, el_cp_correct,
stg_cs_sd, stg_cs_barcodes, bl_cs_raw, bl_cs_correct, el_cs_raw, el_cs_correct,
stg_fp_sd, stg_fp_barcodes, bl_fp_raw, bl_fp_correct, el_fp_raw, el_fp_correct,
stg_saf_sd, stg_saf_barcodes, bl_saf_raw, el_saf_raw, saf_correct, 
stg_sar_sd, stg_sar_barcodes, bl_sar_raw, el_sar_raw, sar_correct,

combined_sd, combined_barcodes from t1 full outer join t2 on t1.batch_no = t2.batch_no),

t4 as (
    select 
        batch_no as session_batch_no,
        no_of_students_facilitated, --- this means total sd uploaded into batch by filling form on SF.
        total_student_present_s1,
        total_student_present_s2,
        total_student_present_s3,
        total_student_present_s4,
        total_student_present_s5,
        total_student_present_s6,
        total_student_present_s7,
        total_student_present_s8,
        total_student_present_s9,
        total_student_present_s10,
        total_student_present_s11,
        total_student_present_s12,
        total_student_present_s13,
        total_student_present_s14
        from {{ref('stg_overall_attendance')}}
),

t5 as (
    select * from t3 LEFT JOIN t4 on t3.batch_no = t4.session_batch_no
),

t6 as (
    SELECT 
        *, 
        total_student_present_s1 AS TSP_Baseline,

        CASE 
            WHEN school_district = 'Nagaland' AND batch_grade IN ('Grade 9', 'Grade 11') THEN total_student_present_s14
            WHEN school_district = 'Nagaland' AND batch_grade IN ('Grade 10', 'Grade 12') THEN total_student_present_s6
            WHEN (school_district LIKE '%Model B' OR school_district LIKE '%Model C') 
                 AND batch_grade IN ('Grade 10', 'Grade 11', 'Grade 12') THEN total_student_present_s4
            WHEN (school_district LIKE '%Model B' OR school_district LIKE '%Model C') 
                 AND batch_grade = 'Grade 9' THEN total_student_present_s10
        END AS TSP_Endline,

        CASE 
            WHEN school_district = 'Nagaland' AND batch_grade IN ('Grade 9', 'Grade 11') THEN total_student_present_s2
            WHEN (school_district LIKE '%Model B' OR school_district LIKE '%Model C') 
                 AND batch_grade = 'Grade 9' THEN total_student_present_s2
        END AS TSP_SAF_Interest,

        CASE 
            WHEN school_district = 'Nagaland' AND batch_grade IN ('Grade 9', 'Grade 11') THEN total_student_present_s4
            WHEN (school_district LIKE '%Model B' OR school_district LIKE '%Model C') 
                 AND batch_grade = 'Grade 9' THEN total_student_present_s4
        END AS TSP_SAF_Aptitude,

        CASE 
            WHEN school_district = 'Nagaland' AND batch_grade IN ('Grade 9', 'Grade 11') THEN total_student_present_s5
            WHEN school_district = 'Nagaland' AND batch_grade = 'Grade 10' THEN total_student_present_s3
            WHEN school_district = 'Nagaland' AND batch_grade = 'Grade 12' THEN total_student_present_s4
        END AS TSP_SAF_QF,

        CASE 
            WHEN school_district = 'Nagaland' AND batch_grade IN ('Grade 9', 'Grade 11') THEN total_student_present_s6
            WHEN (school_district LIKE '%Model B' OR school_district LIKE '%Model C') 
                 AND batch_grade = 'Grade 9' THEN total_student_present_s6
        END AS TSP_SAR_Reality,

        CASE 
            WHEN school_district = 'Nagaland' AND batch_grade IN ('Grade 9', 'Grade 11') THEN total_student_present_s11
        END AS TSP_SAR_Quiz2

    FROM t5 
    WHERE batch_academic_year >= 2023
)

SELECT * except (
total_student_present_s1, total_student_present_s2, total_student_present_s3, total_student_present_s4,
total_student_present_s5, total_student_present_s6, total_student_present_s7, total_student_present_s8,
total_student_present_s9, total_student_present_s10, total_student_present_s11,
total_student_present_s12, total_student_present_s13, total_student_present_s14)
FROM t6





--select * from t5



