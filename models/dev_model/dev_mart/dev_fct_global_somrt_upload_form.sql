with 
cdm1 as (select assessment_barcode as cdm1_assessment_barcode, cdm1_no, is_non_null, 
q1, q1_marks, q2_1, q2_2, q2_marks, q3_1, q3_2, q3_marks, q4_1, q4_1_marks, q4_2,
q4_2_marks,	 q4_marks, cdm1_total_marks, error_status, record_type as cdm1_record_type, 
created_from_form as cdm1_created_from_form, created_on as cdm1_created_on from {{ ref("dev_stg_cdm1") }}),

cdm2 as (select assessment_barcode as cdm2_assessment_barcode, cdm2_no, is_non_null, q5, q5_marks, q6_1, 
q6_1_marks, q6_2, q6_2_marks, q6_3, q6_3_marks, q6_4, q6_4_marks, q6_5, q6_5_marks, q6_6, q6_6_marks, q6_7, q6_7_marks, q6_8, 
q6_8_marks, q6_9, q6_9_marks, q6_10, q6_10_marks, q6_11, q6_11_marks, q6_12, q6_12_marks, q6_total_marks, cdm2_total_marks,
record_type as cdm2_record_type, created_from_form as cdm2_created_from_form, created_on as cdm2_created_on from {{ ref('dev_stg_cdm2') }}),

cp as (select assessment_barcode as cp_assessment_barcode, cp_no, is_non_null, q7, q7_marks, q8, q8_marks,
q9_1, q9_1_marks, q9_2, q9_2_marks, q9_3, q9_3_marks, q9_4, q9_4_marks, q9_5, q9_5_marks, q9_6, q9_6_marks, q9_7, q9_7_marks, q10, 
q10_marks, cp_total_marks, q8_null, q8_bucket, record_type as cp_record_type, created_from_form as cp_created_from_form, created_on as cp_created_on from {{ ref('dev_stg_cp') }}),

cs as (select assessment_barcode as cs_assessment_barcode, cs_no, is_non_null, q11_1, q11_2, q11_3, q11_4, q11_5, 
q11_6, q11_7, q11_8, q11_9, q11_marks, q12_1, q12_2, q12_3, q12_4, q12_marks, q13, q13_marks, q14, q14_marks, q15_1, q15_2, q15_3, 
q15_4, q15_5, q15_6, q15_7, q15_8, q15_9, q15_marks, q16,q16_marks, cs_total_marks, record_type as cs_record_type, 
created_from_form as cs_created_from_form, created_on as cs_created_on from {{ ref('dev_stg_cs') }}),

fp as (select assessment_barcode as fp_assessment_barcode, fp_no, is_non_null, q17, q17_marks, q18_1, q18_2,
q18_3, q18_4, q18_5, q18_6, q18_7, q18_8, q18_9, q18_10, q18_11, q18_marks, q19, q19_marks, q20, q20_marks, q21, q21_marks, q22,
q22_marks, fp_total_marks, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, q21_null, q21_bucket,
record_type as fp_record_type , created_from_form as fp_created_from_form, created_on as fp_created_on from {{ ref('dev_stg_fp') }}),

saf as (select saf_id, assessment_barcode as saf_assessment_barcode, saf_no, is_non_null, saf_atleast_one_interest,
saf_atleast_one_aptitude, saf_atleast_one_quiz, saf_atleast_one_feedback, interest_submitted, aptitude_submitted, feedback_submitted,
saf_i1, saf_i2, saf_i3, saf_a1, saf_a2, saf_a3, saf_q1, saf_q1_marks, saf_q2, saf_q2_marks, saf_q3, saf_q3_marks, saf_q4, saf_q4_marks,
saf_q5, saf_q5_marks, saf_f1, saf_f2, saf_f3, saf_f4, saf_f5, saf_f6, saf_f7, saf_f8, saf_f9, saf_f10, saf_f11, saf_f12,
record_type as saf_record_type, created_from_form as saf_created_from_form, created_on as saf_created_on from {{ ref('dev_stg_saf') }}),

sar as (select sar_id, assessment_barcode as sar_assessment_barcode, sar_no, is_non_null, sar_atleast_one_quiz,
sar_atleast_one_reality, q2_submitted, reality_submitted, sar_q1, sar_q1_marks, sar_q2, sar_q2_marks, sar_q3, sar_q3_marks, 
sar_q4, sar_q4_marks, sar_q5, sar_q5_marks, r1s1, r1s1_marks, r2s2, r2s2_marks, r3s3, r3s3_marks, r4s4, r4s4_marks, r5f1, 
r5f1_marks, r6f2, r6f2_marks, r7f3, r7f3_marks, r8f4, r8f4_marks, 
record_type as sar_record_type, created_from_form as sar_created_from_form, created_on as sar_created_on from {{ ref('dev_stg_sar') }}),

int_global_dcp as (select student_id, student_barcode, student_grade, student_batch_id, gender, batch_no, batch_academic_year, 
school_academic_year, batch_grade, batch_language, fac_start_date, school_language, facilitator_name, facilitator_email, 
school_name, school_taluka, school_ward, school_district, school_state, school_partner, school_area, batch_donor,  
Student_GRADE_COUNT, G9_Batch_Student_Flag, G10_Batch_Student_Flag, G11_Batch_Student_Flag, G12_Batch_Student_Flag 
from {{ ref("dev_int_global_dcp") }})
,

assessment as (
    SELECT a.batch_no, a.batch_academic_year, 
a.school_academic_year, a.batch_grade, a.batch_language, a.fac_start_date, a.school_language, a.facilitator_name, a.facilitator_email, 
a.school_name, a.school_taluka, a.school_ward, a.school_district, a.school_state, a.school_partner, a.school_area, a.batch_donor,

MIN(CASE WHEN cdm1_record_type = 'Baseline' THEN cdm1_created_on END) AS cdm1_baseline_created_on,
MIN(CASE WHEN cdm1_record_type = 'Endline' THEN cdm1_created_on END) AS cdm1_endline_created_on,
count(distinct case when cdm1_created_from_form = true then student_barcode end) as stg_cdm1_sd, 
count(distinct case when cdm1_created_from_form = true then cdm1_assessment_barcode end) as stg_cdm1_barcodes, 
count(distinct case when cdm1_record_type = 'Baseline' then cdm1_no end) as bl_cdm1_raw, 
count(distinct case when cdm1_record_type = 'Endline' then cdm1_no end) as el_cdm1_raw,


MIN(CASE WHEN cdm2_record_type = 'Baseline' THEN cdm2_created_on END) AS cdm2_baseline_created_on,
MIN(CASE WHEN cdm2_record_type = 'Endline' THEN cdm2_created_on END) AS cdm2_endline_created_on,
count(distinct case when cdm2_created_from_form = true then student_barcode end) as stg_cdm2_sd, 
count(distinct case when cdm2_created_from_form = true then cdm2_assessment_barcode end) as stg_cdm2_barcodes, 
count(distinct case when cdm2_record_type = 'Baseline' then cdm2_no end) as bl_cdm2_raw, 
count(distinct case when cdm2_record_type = 'Endline' then cdm2_no end) as el_cdm2_raw,


MIN(CASE WHEN cp_record_type = 'Baseline' THEN cp_created_on END) AS cp_baseline_created_on,
MIN(CASE WHEN cp_record_type = 'Endline' THEN cp_created_on END) AS cp_endline_created_on,
count(distinct case when cp_created_from_form = true then student_barcode end) as stg_cp_sd, 
count(distinct case when cp_created_from_form = true then cp_assessment_barcode end) as stg_cp_barcodes, 
count(distinct case when cp_record_type = 'Baseline' then cp_no end) as bl_cp_raw, 
count(distinct case when cp_record_type = 'Endline' then cp_no end) as el_cp_raw,


MIN(CASE WHEN cs_record_type = 'Baseline' THEN cs_created_on END) AS cs_baseline_created_on,
MIN(CASE WHEN cs_record_type = 'Endline' THEN cs_created_on END) AS cs_endline_created_on,
count(distinct case when cs_created_from_form = true then student_barcode end) as stg_cs_sd, 
count(distinct case when cs_created_from_form = true then cs_assessment_barcode end) as stg_cs_barcodes, 
count(distinct case when cs_record_type = 'Baseline' then cs_no end) as bl_cs_raw, 
count(distinct case when cs_record_type = 'Endline' then cs_no end) as el_cs_raw,


MIN(CASE WHEN fp_record_type = 'Baseline' THEN fp_created_on END) AS fp_baseline_created_on,
MIN(CASE WHEN fp_record_type = 'Endline' THEN fp_created_on END) AS fp_endline_created_on,
count(distinct case when fp_created_from_form = true then student_barcode end) as stg_fp_sd, 
count(distinct case when fp_created_from_form = true then fp_assessment_barcode end) as stg_fp_barcodes, 
count(distinct case when fp_record_type = 'Baseline' then fp_no end) as bl_fp_raw, 
count(distinct case when fp_record_type = 'Endline' then fp_no end) as el_fp_raw,


MIN(CASE WHEN saf_record_type = 'Baseline' THEN saf_created_on END) AS saf_baseline_created_on,
MIN(CASE WHEN saf_record_type = 'Endline' THEN saf_created_on END) AS saf_endline_created_on,
count(distinct case when saf_created_from_form = true then student_barcode end) as stg_saf_sd, 
count(distinct case when saf_created_from_form = true then saf_assessment_barcode end) as stg_saf_barcodes, 
count(distinct case when saf_record_type = 'Baseline' then saf_no end) as bl_saf_raw, 
count(distinct case when saf_record_type = 'Endline' then saf_no end) as el_saf_raw,
count(distinct case when saf_created_from_form = true and saf_record_type IN ('Baseline', 'Endline') then saf_no end) as saf_correct,


MIN(CASE WHEN sar_record_type = 'Baseline' THEN sar_created_on END) AS sar_baseline_created_on,
MIN(CASE WHEN sar_record_type = 'Endline' THEN sar_created_on END) AS sar_endline_created_on,
count(distinct case when sar_created_from_form = true then student_barcode end) as stg_sar_sd, 
count(distinct case when sar_created_from_form = true then sar_assessment_barcode end) as stg_sar_barcodes, 
count(distinct case when sar_record_type = 'Baseline' then sar_no end) as bl_sar_raw, 
count(distinct case when sar_record_type = 'Endline' then sar_no end) as el_sar_raw,
count(distinct case when sar_created_from_form = true and sar_record_type IN ('Baseline', 'Endline') then sar_no end) as sar_correct,

SUM(CASE WHEN batch_grade like '%9%' THEN G9_Batch_Student_Flag
         WHEN batch_grade like '%10%' THEN G10_Batch_Student_Flag
         WHEN batch_grade like '%11%' THEN G11_Batch_Student_Flag
         WHEN batch_grade like '%12%' THEN G12_Batch_Student_Flag END) AS Batch_Student_SD2_Count --Sd2 new column

from int_global_dcp a

LEFT JOIN cdm1 ON a.student_barcode = cdm1.cdm1_assessment_barcode
LEFT JOIN cdm2 ON a.student_barcode = cdm2.cdm2_assessment_barcode
LEFT JOIN cp ON a.student_barcode = cp.cp_assessment_barcode
LEFT JOIN cs ON a.student_barcode = cs.cs_assessment_barcode
LEFT JOIN fp ON a.student_barcode = fp.fp_assessment_barcode
LEFT JOIN saf ON a.student_barcode = saf.saf_assessment_barcode
LEFT JOIN sar ON a.student_barcode = sar.sar_assessment_barcode

WHERE a.school_partner NOT IN ('KMCT', 'Learning Links Foundation', 'Akanksha Foundation', 'Masoom', 'Educate Girls')
--and batch_no = '32416' 
group by a.batch_no, a.batch_academic_year, 
a.school_academic_year, a.batch_grade, a.batch_language, a.fac_start_date, a.school_language, a.facilitator_name, a.facilitator_email, 
a.school_name, a.school_taluka, a.school_ward, a.school_district, a.school_state, a.school_partner, a.school_area, a.batch_donor
),

assessment_status as (SELECT b.batch_no, b.batch_academic_year, 
b.school_academic_year, b.batch_grade, b.batch_language, b.fac_start_date, b.school_language, b.facilitator_name, b.facilitator_email, 
b.school_name, b.school_taluka, b.school_ward, b.school_district, b.school_state, b.school_partner, b.school_area, b.batch_donor, 
b.stg_cdm1_sd, b.stg_cdm1_barcodes, b.bl_cdm1_raw, b.el_cdm1_raw, b.cdm1_baseline_created_on, b.cdm1_endline_created_on, 
b.cdm2_baseline_created_on, b.cdm2_endline_created_on, b.cp_baseline_created_on, b.cp_endline_created_on,
b.cs_baseline_created_on, b.cs_endline_created_on, b.fp_baseline_created_on, b.fp_endline_created_on, b.saf_baseline_created_on, b.saf_endline_created_on, b.sar_baseline_created_on, b.sar_endline_created_on,

COUNT(DISTINCT CASE WHEN cdm1.cdm1_created_from_form = true and cdm1.cdm1_record_type = 'Baseline' THEN cdm1.cdm1_no END) AS bl_cdm1_correct,
COUNT(DISTINCT CASE WHEN cdm1.cdm1_created_from_form = true and cdm1.cdm1_record_type = 'Endline'  THEN cdm1.cdm1_no END) AS el_cdm1_correct,

b.stg_cdm2_sd, b.stg_cdm2_barcodes, b.bl_cdm2_raw, b.el_cdm2_raw,
COUNT(DISTINCT CASE WHEN cdm2.cdm2_created_from_form = true and cdm2.cdm2_record_type = 'Baseline' THEN cdm2.cdm2_no END) AS bl_cdm2_correct,
COUNT(DISTINCT CASE WHEN cdm2.cdm2_created_from_form = true and cdm2.cdm2_record_type = 'Endline'  THEN cdm2.cdm2_no END) AS el_cdm2_correct,

b.stg_cp_sd, b.stg_cp_barcodes, b.bl_cp_raw, b.el_cp_raw,
COUNT(DISTINCT CASE WHEN cp.cp_created_from_form = true and cp.cp_record_type = 'Baseline' THEN cp.cp_no END) AS bl_cp_correct,
COUNT(DISTINCT CASE WHEN cp.cp_created_from_form = true and cp.cp_record_type = 'Endline'  THEN cp.cp_no END) AS el_cp_correct,

b.stg_cs_sd, b.stg_cs_barcodes, b.bl_cs_raw, b.el_cs_raw,
COUNT(DISTINCT CASE WHEN cs.cs_created_from_form = true and cs.cs_record_type = 'Baseline' THEN cs.cs_no END) AS bl_cs_correct,
COUNT(DISTINCT CASE WHEN cs.cs_created_from_form = true and cs.cs_record_type = 'Endline'  THEN cs.cs_no END) AS el_cs_correct,

b.stg_fp_sd, b.stg_fp_barcodes, b.bl_fp_raw, b.el_fp_raw,
COUNT(DISTINCT CASE WHEN fp.fp_created_from_form = true and fp.fp_record_type = 'Baseline' THEN fp.fp_no END) AS bl_fp_correct,
COUNT(DISTINCT CASE WHEN fp.fp_created_from_form = true and fp.fp_record_type = 'Endline'  THEN fp.fp_no END) AS el_fp_correct,

b.stg_saf_sd, b.stg_saf_barcodes, b.bl_saf_raw, b.el_saf_raw, b.saf_correct,
COUNT(DISTINCT CASE WHEN saf.saf_created_from_form = true and saf.saf_record_type = 'Baseline' THEN saf.saf_no END) AS bl_saf_correct,
COUNT(DISTINCT CASE WHEN saf.saf_created_from_form = true and saf.saf_record_type = 'Endline'  THEN saf.saf_no END) AS el_saf_correct,

b.stg_sar_sd, b.stg_sar_barcodes, b.bl_sar_raw, b.el_sar_raw, b.sar_correct,
COUNT(DISTINCT CASE WHEN sar.sar_created_from_form = true and sar.sar_record_type = 'Baseline' THEN sar.sar_no END) AS bl_sar_correct,
COUNT(DISTINCT CASE WHEN sar.sar_created_from_form = true and sar.sar_record_type = 'Endline'  THEN sar.sar_no END) AS el_sar_correct,

MAX(Batch_Student_SD2_Count) AS Batch_Student_SD2_Count
FROM assessment b
LEFT JOIN int_global_dcp igd ON b.batch_no = igd.batch_no
LEFT JOIN cdm1 ON igd.student_barcode = cdm1.cdm1_assessment_barcode
LEFT JOIN cdm2 ON igd.student_barcode = cdm2.cdm2_assessment_barcode
LEFT JOIN cp ON igd.student_barcode = cp.cp_assessment_barcode
LEFT JOIN cs ON igd.student_barcode = cs.cs_assessment_barcode
LEFT JOIN fp ON igd.student_barcode = fp.fp_assessment_barcode
LEFT JOIN saf ON igd.student_barcode = saf.saf_assessment_barcode
LEFT JOIN sar ON igd.student_barcode = sar.sar_assessment_barcode

WHERE b.school_partner NOT IN ('KMCT', 'Learning Links Foundation', 'Akanksha Foundation', 'Masoom', 'Educate Girls') 
 
group by b.batch_no, b.batch_academic_year, 
b.school_academic_year, b.batch_grade, b.batch_language, b.fac_start_date, b.school_language, b.facilitator_name, b.facilitator_email, 
b.school_name, b.school_taluka, b.school_ward, b.school_district, b.school_state, b.school_partner, b.school_area, b.batch_donor,
b.stg_cdm1_sd, b.stg_cdm1_barcodes, b.bl_cdm1_raw, b.el_cdm1_raw, b.stg_cdm2_sd, b.stg_cdm2_barcodes, b.bl_cdm2_raw, b.el_cdm2_raw,
b.stg_cp_sd, b.stg_cp_barcodes, b.bl_cp_raw, b.el_cp_raw, b.stg_cs_sd, b.stg_cs_barcodes, b.bl_cs_raw, b.el_cs_raw,
b.stg_fp_sd, b.stg_fp_barcodes, b.bl_fp_raw, b.el_fp_raw, b.stg_saf_sd, b.stg_saf_barcodes, b.bl_saf_raw, b.el_saf_raw,
b.stg_sar_sd, b.stg_sar_barcodes, b.bl_sar_raw, b.el_sar_raw, b.cdm1_baseline_created_on, b.cdm1_endline_created_on, 
b.cdm2_baseline_created_on, b.cdm2_endline_created_on, b.cp_baseline_created_on, b.cp_endline_created_on,
b.cs_baseline_created_on, b.cs_endline_created_on, b.fp_baseline_created_on, b.fp_endline_created_on, b.saf_baseline_created_on, b.saf_endline_created_on, b.sar_baseline_created_on, b.sar_endline_created_on, b.saf_correct, b.sar_correct),

overall_attendance as (select 
batch_no as session_batch_no,
        MIN(session_date) as session_date,
        no_of_students_facilitated,  -- or SUM/AVG depending on meaning
        MAX(total_student_present_s1) as total_student_present_s1,
        MAX(total_student_present_s2) as total_student_present_s2,
        MAX(total_student_present_s3) as total_student_present_s3,
        MAX(total_student_present_s4) as total_student_present_s4,
        MAX(total_student_present_s5) as total_student_present_s5,
        MAX(total_student_present_s6) as total_student_present_s6,
        MAX(total_student_present_s7) as total_student_present_s7,
        MAX(total_student_present_s8) as total_student_present_s8,
        MAX(total_student_present_s9) as total_student_present_s9,
        MAX(total_student_present_s10) as total_student_present_s10,
        MAX(total_student_present_s11) as total_student_present_s11,
        MAX(total_student_present_s12) as total_student_present_s12,
        MAX(total_student_present_s13) as total_student_present_s13,
        MAX(total_student_present_s14) as total_student_present_s14,
        MAX(total_student_present_s15) as total_student_present_s15,
        MAX(total_student_present_s16) as total_student_present_s16
from {{ref('dev_stg_overall_attendance')}}
GROUP BY batch_no, no_of_students_facilitated
),

attendance_join as (select c.batch_no, c.batch_academic_year, 
c.school_academic_year, c.batch_grade, c.batch_language, c.fac_start_date, c.school_language, c.facilitator_name, c.facilitator_email, 
c.school_name, c.school_taluka, c.school_ward, c.school_district, c.school_state, c.school_partner, c.school_area, c.batch_donor, c.Batch_Student_SD2_Count,
c.stg_cdm1_sd, c.stg_cdm1_barcodes, c.bl_cdm1_raw, c.el_cdm1_raw, c.stg_cdm2_sd, c.stg_cdm2_barcodes, c.bl_cdm2_raw, c.el_cdm2_raw,
c.stg_cp_sd, c.stg_cp_barcodes, c.bl_cp_raw, c.el_cp_raw, c.stg_cs_sd, c.stg_cs_barcodes, c.bl_cs_raw, c.el_cs_raw,
c.stg_fp_sd, c.stg_fp_barcodes, c.bl_fp_raw, c.el_fp_raw, c.stg_saf_sd, c.stg_saf_barcodes, c.bl_saf_raw, c.el_saf_raw,
c.stg_sar_sd, c.stg_sar_barcodes, c.bl_sar_raw, c.el_sar_raw, c.bl_cdm1_correct, c.el_cdm1_correct, c.bl_cdm2_correct, c.el_cdm2_correct,
c.bl_cp_correct, c.el_cp_correct, c.bl_cs_correct, c.el_cs_correct, c.bl_fp_correct, c.el_fp_correct, c.bl_saf_correct, c.el_saf_correct,
c.cdm1_baseline_created_on, c.cdm1_endline_created_on, c.cdm2_baseline_created_on, c.cdm2_endline_created_on, c.cp_baseline_created_on, c.cp_endline_created_on, 
c.cs_baseline_created_on, c.cs_endline_created_on, c.fp_baseline_created_on, c.fp_endline_created_on, c.saf_baseline_created_on, c.saf_endline_created_on, c.sar_baseline_created_on, c.sar_endline_created_on, c.saf_correct, c.sar_correct,
c.bl_sar_correct, c.el_sar_correct, oa.session_batch_no, oa.no_of_students_facilitated,
oa.total_student_present_s1, oa.total_student_present_s2, oa.total_student_present_s3, oa.total_student_present_s4,
oa.total_student_present_s5, oa.total_student_present_s6, oa.total_student_present_s7, oa.total_student_present_s8,
oa.total_student_present_s9, oa.total_student_present_s10, oa.total_student_present_s11, oa.total_student_present_s12,
oa.total_student_present_s13, oa.total_student_present_s14, oa.total_student_present_s15, oa.total_student_present_s16,
session_date
 from assessment_status c
LEFT JOIN overall_attendance oa on c.batch_no = oa.session_batch_no
),

somrt as (
    SELECT 
        *, 
       CASE
    WHEN (d.school_district = 'Nagaland' 
          OR d.school_district LIKE '%Model B%' 
          OR d.school_district LIKE '%Model C%')
         AND d.batch_grade IN ('Grade 9','Grade 10','Grade 11','Grade 12')
    THEN d.total_student_present_s1

    WHEN d.school_district = 'Dungarpur' AND d.batch_grade = 'Grade 9' 
    THEN d.total_student_present_s3

    WHEN d.school_district = 'Dungarpur' AND d.batch_grade IN ('Grade 10','Grade 11','Grade 12') 
    THEN d.total_student_present_s1
END AS TSP_Baseline,


CASE
    WHEN d.school_district = 'Nagaland' AND d.batch_grade IN ('Grade 9','Grade 11') THEN d.total_student_present_s14
    WHEN d.school_district = 'Nagaland' AND d.batch_grade IN ('Grade 10','Grade 12') THEN d.total_student_present_s6
    WHEN d.school_district LIKE '%Model B%' AND d.batch_grade = 'Grade 12' THEN d.total_student_present_s6
    WHEN (d.school_district LIKE '%Model B%' OR d.school_district LIKE '%Model C%') AND d.batch_grade = 'Grade 10'
    THEN d.total_student_present_s11
    WHEN (d.school_district LIKE '%Model B%' OR d.school_district LIKE '%Model C%' OR d.school_district LIKE '%Model A%')
    AND d.batch_grade IN ('Grade 10','Grade 12') THEN d.total_student_present_s7
    WHEN (d.school_district LIKE '%Model B%' OR d.school_district LIKE '%Model C%' OR d.school_district LIKE '%Model A%')
         AND d.batch_grade = 'Grade 11' THEN d.total_student_present_s15
    WHEN (d.school_district LIKE '%Model B%' OR d.school_district LIKE '%Model C%' OR d.school_district LIKE '%Model A%' 
    OR d.school_district = 'Dungarpur') AND d.batch_grade IN ('Grade 9','Grade 11') THEN d.total_student_present_s16
    WHEN d.school_district = 'Dungarpur' AND d.batch_grade IN ('Grade 10','Grade 12') THEN d.total_student_present_s8
END AS TSP_Endline,

CASE 
    WHEN d.school_district = 'Nagaland' AND d.batch_grade IN ('Grade 9','Grade 11') THEN d.total_student_present_s2
    WHEN (d.school_district LIKE '%Model B%' OR d.school_district LIKE '%Model C%' OR d.school_district LIKE '%Model A%'
    OR d.school_district = 'Dungarpur') AND d.batch_grade = 'Grade 9' THEN d.total_student_present_s4
    WHEN (d.school_district LIKE '%Model B%' OR d.school_district LIKE '%Model C%') AND d.batch_grade = 'Grade 9' 
    THEN d.total_student_present_s5
END AS TSP_SAF_Interest,

CASE 
    WHEN d.school_district = 'Nagaland' AND d.batch_grade IN ('Grade 9','Grade 11')
        THEN d.total_student_present_s4
    WHEN (d.school_district LIKE '%Model B%' OR d.school_district LIKE '%Model C%' OR d.school_district = 'Dungarpur')
         AND d.batch_grade = 'Grade 9'
        THEN d.total_student_present_s5
    WHEN (d.school_district LIKE '%Model B%' OR d.school_district LIKE '%Model C%') AND d.batch_grade = 'Grade 9'
        THEN d.total_student_present_s7
END AS TSP_SAF_Aptitude,

CASE 
    WHEN d.school_district = 'Nagaland' AND d.batch_grade IN ('Grade 9','Grade 11')
        THEN d.total_student_present_s5
    WHEN d.school_district = 'Nagaland' AND d.batch_grade = 'Grade 10'
        THEN d.total_student_present_s3
    WHEN d.school_district = 'Nagaland' AND d.batch_grade = 'Grade 12'
        THEN d.total_student_present_s4
    WHEN d.school_district LIKE '%Model B%' AND d.batch_grade = 'Grade 12'
        THEN d.total_student_present_s4
END AS TSP_SAF_QF,

CASE 
    WHEN d.school_district = 'Nagaland' AND d.batch_grade IN ('Grade 9','Grade 11') THEN d.total_student_present_s6
    WHEN (d.school_district LIKE '%Model B%' OR d.school_district LIKE '%Model C%') AND d.batch_grade = 'Grade 9'
        THEN d.total_student_present_s9
    WHEN (d.school_district LIKE '%Model B%' OR d.school_district LIKE '%Model C%' OR d.school_district LIKE '%Model A%' OR
    d.school_district = 'Dungarpur') AND d.batch_grade = 'Grade 9' THEN d.total_student_present_s11
END AS TSP_SAR_Reality,

CASE 
    WHEN d.school_district = 'Nagaland' AND d.batch_grade IN ('Grade 9','Grade 11') THEN d.total_student_present_s11
END AS TSP_SAR_Quiz2,

-- TAT for CDM1
        CASE
  WHEN d.session_date IS NOT NULL 
   AND d.cdm1_baseline_created_on IS NOT NULL
  THEN (
    SELECT COUNT(*)
    FROM UNNEST(
      GENERATE_DATE_ARRAY(
        DATE_ADD(d.session_date, INTERVAL 1 DAY),   -- TAT starts from next day
        DATE(d.cdm1_baseline_created_on)
      )
    ) AS dt
    WHERE EXTRACT(DAYOFWEEK FROM dt) NOT IN (1, 7)   -- 1 = Sunday, 7 = Saturday
  )
END AS cdm1_baseline_TAT,

-- CDM1 Endline
CASE
  WHEN d.session_date IS NOT NULL AND d.cdm1_endline_created_on IS NOT NULL
  THEN (
    SELECT COUNT(*)
    FROM UNNEST(
      GENERATE_DATE_ARRAY(
        DATE_ADD(d.session_date, INTERVAL 1 DAY),
        DATE(d.cdm1_endline_created_on)
      )
    ) dt
    WHERE EXTRACT(DAYOFWEEK FROM dt) NOT IN (1,7)
  )
END AS cdm1_endline_TAT,

-- CDM2 Baseline
CASE
  WHEN d.session_date IS NOT NULL AND d.cdm2_baseline_created_on IS NOT NULL
  THEN (
    SELECT COUNT(*)
    FROM UNNEST(
      GENERATE_DATE_ARRAY(
        DATE_ADD(d.session_date, INTERVAL 1 DAY),
        DATE(d.cdm2_baseline_created_on)
      )
    ) dt
    WHERE EXTRACT(DAYOFWEEK FROM dt) NOT IN (1,7)
  )
END AS cdm2_baseline_TAT,

-- CDM2 Endline
CASE
  WHEN d.session_date IS NOT NULL AND d.cdm2_endline_created_on IS NOT NULL
  THEN (
    SELECT COUNT(*)
    FROM UNNEST(
      GENERATE_DATE_ARRAY(
        DATE_ADD(d.session_date, INTERVAL 1 DAY),
        DATE(d.cdm2_endline_created_on)
      )
    ) dt
    WHERE EXTRACT(DAYOFWEEK FROM dt) NOT IN (1,7)
  )
END AS cdm2_endline_TAT,

-- CP Baseline
CASE
  WHEN d.session_date IS NOT NULL AND d.cp_baseline_created_on IS NOT NULL
  THEN (
    SELECT COUNT(*)
    FROM UNNEST(
      GENERATE_DATE_ARRAY(
        DATE_ADD(d.session_date, INTERVAL 1 DAY),
        DATE(d.cp_baseline_created_on)
      )
    ) dt
    WHERE EXTRACT(DAYOFWEEK FROM dt) NOT IN (1,7)
  )
END AS cp_baseline_TAT,

-- CP Endline
CASE
  WHEN d.session_date IS NOT NULL AND d.cp_endline_created_on IS NOT NULL
  THEN (
    SELECT COUNT(*)
    FROM UNNEST(
      GENERATE_DATE_ARRAY(
        DATE_ADD(d.session_date, INTERVAL 1 DAY),
        DATE(d.cp_endline_created_on)
      )
    ) dt
    WHERE EXTRACT(DAYOFWEEK FROM dt) NOT IN (1,7)
  )
END AS cp_endline_TAT,

-- CS Baseline
CASE
  WHEN d.session_date IS NOT NULL AND d.cs_baseline_created_on IS NOT NULL
  THEN (
    SELECT COUNT(*)
    FROM UNNEST(
      GENERATE_DATE_ARRAY(
        DATE_ADD(d.session_date, INTERVAL 1 DAY),
        DATE(d.cs_baseline_created_on)
      )
    ) dt
    WHERE EXTRACT(DAYOFWEEK FROM dt) NOT IN (1,7)
  )
END AS cs_baseline_TAT,

-- CS Endline
CASE
  WHEN d.session_date IS NOT NULL AND d.cs_endline_created_on IS NOT NULL
  THEN (
    SELECT COUNT(*)
    FROM UNNEST(
      GENERATE_DATE_ARRAY(
        DATE_ADD(d.session_date, INTERVAL 1 DAY),
        DATE(d.cs_endline_created_on)
      )
    ) dt
    WHERE EXTRACT(DAYOFWEEK FROM dt) NOT IN (1,7)
  )
END AS cs_endline_TAT,

-- FP Baseline
CASE
  WHEN d.session_date IS NOT NULL AND d.fp_baseline_created_on IS NOT NULL
  THEN (
    SELECT COUNT(*)
    FROM UNNEST(
      GENERATE_DATE_ARRAY(
        DATE_ADD(d.session_date, INTERVAL 1 DAY),
        DATE(d.fp_baseline_created_on)
      )
    ) dt
    WHERE EXTRACT(DAYOFWEEK FROM dt) NOT IN (1,7)
  )
END AS fp_baseline_TAT,

-- FP Endline
CASE
  WHEN d.session_date IS NOT NULL AND d.fp_endline_created_on IS NOT NULL
  THEN (
    SELECT COUNT(*)
    FROM UNNEST(
      GENERATE_DATE_ARRAY(
        DATE_ADD(d.session_date, INTERVAL 1 DAY),
        DATE(d.fp_endline_created_on)
      )
    ) dt
    WHERE EXTRACT(DAYOFWEEK FROM dt) NOT IN (1,7)
  )
END AS fp_endline_TAT,

-- SAF Baseline
CASE
  WHEN d.session_date IS NOT NULL AND d.saf_baseline_created_on IS NOT NULL
  THEN (
    SELECT COUNT(*)
    FROM UNNEST(
      GENERATE_DATE_ARRAY(
        DATE_ADD(d.session_date, INTERVAL 1 DAY),
        DATE(d.saf_baseline_created_on)
      )
    ) dt
    WHERE EXTRACT(DAYOFWEEK FROM dt) NOT IN (1,7)
  )
END AS saf_baseline_TAT,

-- SAF Endline
CASE
  WHEN d.session_date IS NOT NULL AND d.saf_endline_created_on IS NOT NULL
  THEN (
    SELECT COUNT(*)
    FROM UNNEST(
      GENERATE_DATE_ARRAY(
        DATE_ADD(d.session_date, INTERVAL 1 DAY),
        DATE(d.saf_endline_created_on)
      )
    ) dt
    WHERE EXTRACT(DAYOFWEEK FROM dt) NOT IN (1,7)
  )
END AS saf_endline_TAT,

-- SAR Baseline
CASE
  WHEN d.session_date IS NOT NULL AND d.sar_baseline_created_on IS NOT NULL
  THEN (
    SELECT COUNT(*)
    FROM UNNEST(
      GENERATE_DATE_ARRAY(
        DATE_ADD(d.session_date, INTERVAL 1 DAY),
        DATE(d.sar_baseline_created_on)
      )
    ) dt
    WHERE EXTRACT(DAYOFWEEK FROM dt) NOT IN (1,7)
  )
END AS sar_baseline_TAT,

-- SAR Endline
CASE
  WHEN d.session_date IS NOT NULL AND d.sar_endline_created_on IS NOT NULL
  THEN (
    SELECT COUNT(*)
    FROM UNNEST(
      GENERATE_DATE_ARRAY(
        DATE_ADD(d.session_date, INTERVAL 1 DAY),
        DATE(d.sar_endline_created_on)
      )
    ) dt
    WHERE EXTRACT(DAYOFWEEK FROM dt) NOT IN (1,7)
  )
END AS sar_endline_TAT
FROM attendance_join d
),


final as (select e.batch_no, e.batch_academic_year, e.batch_grade, e.batch_language, e.fac_start_date, e.facilitator_name, e.facilitator_email,
e.school_name, e.school_taluka, e.school_ward, e.school_district, e.school_state, e.school_partner, e.school_area, e.batch_donor, e.Batch_Student_SD2_Count,
e.stg_cdm1_sd, e.stg_cdm1_barcodes, e.bl_cdm1_raw, e.bl_cdm1_correct, e.el_cdm1_raw, e.el_cdm1_correct, e.stg_cdm2_sd, e.stg_cdm2_barcodes,
e.bl_cdm2_raw, e.bl_cdm2_correct, e.el_cdm2_raw, e.el_cdm2_correct, e.stg_cp_sd, e.stg_cp_barcodes, e.bl_cp_raw, e.bl_cp_correct,
e.el_cp_raw, e.el_cp_correct, e.stg_cs_sd, e.stg_cs_barcodes, e.bl_cs_raw, e.bl_cs_correct, e.el_cs_raw, e.el_cs_correct,
e.stg_fp_sd, e.stg_fp_barcodes, e.bl_fp_raw, e.bl_fp_correct, e.el_fp_raw, e.el_fp_correct, e.stg_saf_sd, e.stg_saf_barcodes,
e.bl_saf_raw, e.saf_correct, e.stg_sar_sd, e.stg_sar_barcodes, e.bl_sar_raw, e.sar_correct, 
 e.no_of_students_facilitated, e.TSP_Baseline, e.TSP_Endline,
e.TSP_SAF_Interest, e.TSP_SAF_Aptitude, e.TSP_SAF_QF, e.TSP_SAR_Reality,
e.TSP_SAR_Quiz2, e.session_date, e.cdm1_baseline_created_on, e.cdm1_endline_created_on,
e.cdm1_baseline_TAT, e.cdm1_endline_TAT, e.cdm2_baseline_created_on, e.cdm2_endline_created_on, e.cdm2_baseline_TAT, 
e.cdm2_endline_TAT, e.cp_baseline_created_on, e.cp_endline_created_on, e.cp_baseline_TAT, e.cp_endline_TAT, 
e.cs_baseline_created_on, e.cs_endline_created_on, e.cs_baseline_TAT, e.cs_endline_TAT,
e.fp_baseline_created_on, e.fp_endline_created_on, e.fp_baseline_TAT, e.fp_endline_TAT, 
e.saf_baseline_created_on, e.saf_baseline_TAT, 
e.sar_baseline_created_on, e.sar_baseline_TAT
from somrt e
where batch_academic_year >= 2023 
)

select * from final


--where batch_no = '32416'
