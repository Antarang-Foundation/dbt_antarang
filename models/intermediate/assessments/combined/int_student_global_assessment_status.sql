with t0 as (select * from {{ref('int_student_global_assessment')}})

select

student_barcode, student_grade, gender, batch_id, batch_no, batch_academic_year, batch_grade, batch_language, facilitator_name, school_name, 
school_academic_year, school_language, school_taluka, school_ward, school_district, school_state, school_partner, batch_donor, assessment_barcode, 
assessment_grade, assessment_academic_year, created_from_form, 

(select regexp_replace(status_col, r'_no', '') from

(select (
  select string_agg(col, ', ') 
  from unnest(
    regexp_extract_all(
      regexp_replace(trim(to_json_string(t0), '{}'), r'"[^"]+":null,?', ''), 
      r'"([^"]+)":')) as col
  where col in ('bl_cdm1_no', 'el_cdm1_no', 'bl_cdm2_no', 'el_cdm2_no', 'bl_cp_no', 'el_cp_no', 'bl_cs_no', 'el_cs_no', 
  'bl_cp_no', 'el_cp_no', 'saf_no', 'sar_no')
  ) as status_col
)) submissions,

(case when bl_cdm1_no is not null or el_cdm1_no is not null or bl_cdm2_no is not null or el_cdm2_no is not null or bl_cp_no is not null or 
el_cp_no is not null or bl_cs_no is not null or el_cs_no is not null or bl_fp_no is not null or el_fp_no is not null or 
saf_no is not null or sar_no is not null then 1 else 0 end) atleast_one_submission,

(case when bl_cdm1_no is not null or el_cdm1_no is not null or bl_cdm2_no is not null or el_cdm2_no is not null or bl_cp_no is not null or 
el_cp_no is not null or bl_cs_no is not null or el_cs_no is not null or bl_fp_no is not null or el_fp_no is not null 
then 1 else 0 end) atleast_one_assessment_submission,

(case when saf_no is not null or sar_no is not null then 1 else 0 end) atleast_one_quiz_submission,

(case

        when (bl_cdm1_no is null and el_cdm1_no is null) then '1. Neither'
        when (bl_cdm1_no is not null and el_cdm1_no is null) then '2. Only_BL'
        when (bl_cdm1_no is null and el_cdm1_no is not null) then '3. Only_EL'
        when (bl_cdm1_no is not null and el_cdm1_no is not null) then '4. Both' end) cdm1_status,       

        (case

        when (bl_cdm2_no is null and el_cdm2_no is null) then '1. Neither'
        when (bl_cdm2_no is not null and el_cdm2_no is null) then '2. Only_BL'
        when (bl_cdm2_no is null and el_cdm2_no is not null) then '3. Only_EL'
        when (bl_cdm2_no is not null and el_cdm2_no is not null) then '4. Both' end) cdm2_status,

        (case

        when (bl_cp_no is null and el_cp_no is null) then '1. Neither'
        when (bl_cp_no is not null and el_cp_no is null) then '2. Only_BL'
        when (bl_cp_no is null and el_cp_no is not null) then '3. Only_EL'
        when (bl_cp_no is not null and el_cp_no is not null) then '4. Both' end) cp_status,

        (case

        when (bl_cs_no is null and el_cs_no is null) then '1. Neither'
        when (bl_cs_no is not null and el_cs_no is null) then '2. Only_BL'
        when (bl_cs_no is null and el_cs_no is not null) then '3. Only_EL'
        when (bl_cs_no is not null and el_cs_no is not null) then '4. Both' end) cs_status,

        (case

        when (bl_fp_no is null and el_fp_no is null) then '1. Neither'
        when (bl_fp_no is not null and el_fp_no is null) then '2. Only_BL'
        when (bl_fp_no is null and el_fp_no is not null) then '3. Only_EL'
        when (bl_fp_no is not null and el_fp_no is not null) then '4. Both' end) fp_status,

        (case

        when (saf_no is null and sar_no is null) then '1. Neither'
        when (saf_no is not null and sar_no is null) then '2. Only_SAF'
        when (saf_no is null and sar_no is not null) then '3. Only_SAR'
        when (saf_no is not null and sar_no is not null) then '4. Both' end) quiz_status,

bl_cdm1_no, el_cdm1_no, bl_cdm2_no, el_cdm2_no, bl_cp_no, el_cp_no, bl_cs_no, el_cs_no, bl_fp_no, el_fp_no, saf_no, sar_no,

cdm1_bl_created_on, cdm1_el_created_on, cdm2_bl_created_on, cdm2_el_created_on, cp_bl_created_on, cp_el_created_on, cs_bl_created_on, cs_el_created_on, 
fp_bl_created_on, fp_el_created_on, saf_created_on, sar_created_on,

cdm1_bl_is_non_null, cdm1_el_is_non_null, cdm2_bl_is_non_null, cdm2_el_is_non_null, cp_bl_is_non_null, cp_el_is_non_null, cs_bl_is_non_null, 
cs_el_is_non_null, fp_bl_is_non_null, fp_el_is_non_null, saf_is_non_null, sar_is_non_null, 

saf_atleast_one_interest, saf_atleast_one_aptitude, saf_atleast_one_quiz, saf_atleast_one_feedback, sar_atleast_one_quiz, sar_atleast_one_reality

from t0

