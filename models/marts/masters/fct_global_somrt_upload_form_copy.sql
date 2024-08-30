WITH t1 AS (
    SELECT 
        batch_no, 
        batch_academic_year, 
        batch_grade, 
        batch_language, 
        fac_start_date, 
        facilitator_name, 
        facilitator_email, 
        school_name, 
        school_academic_year, 
        school_language, 
        school_taluka, 
        school_ward, 
        school_district, 
        school_state, 
        school_partner,
        school_area, 
        batch_donor,
        COUNT(DISTINCT student_barcode) AS distinct_student_count,
        COUNT(DISTINCT assessment_barcode) AS distinct_assessment_count,
        COUNT(DISTINCT bl_cdm1_no) AS bl_cdm1_correct, 
        COUNT(DISTINCT el_cdm1_no) AS el_cdm1_correct, 
        COUNT(DISTINCT bl_cdm2_no) AS bl_cdm2_correct, 
        COUNT(DISTINCT el_cdm2_no) AS el_cdm2_correct, 
        COUNT(DISTINCT bl_cp_no) AS bl_cp_correct, 
        COUNT(DISTINCT el_cp_no) AS el_cp_correct, 
        COUNT(DISTINCT bl_cs_no) AS bl_cs_correct, 
        COUNT(DISTINCT el_cs_no) AS el_cs_correct, 
        COUNT(DISTINCT bl_fp_no) AS bl_fp_correct, 
        COUNT(DISTINCT el_fp_no) AS el_fp_correct, 
        COUNT(DISTINCT saf_no) AS saf_correct, 
        COUNT(DISTINCT sar_no) AS sar_correct
    FROM {{ref('fct_student_global_assessment_status')}}  
    --where created_from_form = True
    GROUP BY 
        batch_no, 
        batch_academic_year, 
        batch_grade, 
        batch_language, 
        fac_start_date, 
        facilitator_name, 
        facilitator_email, 
        school_name, 
        school_academic_year, 
        school_language, 
        school_taluka, 
        school_ward, 
        school_district, 
        school_state, 
        school_partner,
        school_area, 
        batch_donor
)
/*
t2 AS (
    SELECT 
        batch_no AS session_batch_no, 
        session_name,
        session_type, 
        total_student_present, 
        total_parent_present, 
        present_count, 
        attendance_count 
    FROM {{ref('fct_global_session')}}
),

t3 as (select * from t1 full outer join t2 on t1.batch_no = t2.session_batch_no)
*/
select * from t1

