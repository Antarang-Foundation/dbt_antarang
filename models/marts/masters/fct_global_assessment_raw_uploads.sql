with 

cdm1 as (select batch_no, batch_academic_year, batch_grade, batch_language, facilitator_name, school_name, school_academic_year, school_language, 
school_taluka, school_ward, school_district, school_state, school_partner, batch_donor, count(distinct student_barcode) `stg_cdm1_sd`, 
count(distinct assessment_barcode) `stg_cdm1_barcodes`, count(distinct case when record_type = 'Baseline' then cdm1_no end) `bl_cdm1_raw`, 
count(distinct case when record_type = 'Endline' then cdm1_no end) `el_cdm1_raw`
 from {{ref('int_student_global_stg_cdm1')}} where created_from_form=True 
 group by batch_no, batch_academic_year, batch_grade, batch_language, facilitator_name, school_name, school_academic_year, school_language, school_taluka, 
 school_ward, school_district, school_state, school_partner, batch_donor),

cdm2 as (select batch_no, batch_academic_year, batch_grade, batch_language, facilitator_name, school_name, school_academic_year, school_language, 
school_taluka, school_ward, school_district, school_state, school_partner, batch_donor, count(distinct student_barcode) `stg_cdm2_sd`, 
count(distinct assessment_barcode) `stg_cdm2_barcodes`, count(distinct case when record_type = 'Baseline' then cdm2_no end) `bl_cdm2_raw`, 
count(distinct case when record_type = 'Endline' then cdm2_no end) `el_cdm2_raw`
 from {{ref('int_student_global_stg_cdm2')}} where created_from_form=True 
 group by batch_no, batch_academic_year, batch_grade, batch_language, facilitator_name, school_name, school_academic_year, school_language, school_taluka, 
 school_ward, school_district, school_state, school_partner, batch_donor),

cp as (select batch_no, batch_academic_year, batch_grade, batch_language, facilitator_name, school_name, school_academic_year, school_language, 
school_taluka, school_ward, school_district, school_state, school_partner, batch_donor, count(distinct student_barcode) `stg_cp_sd`, 
count(distinct assessment_barcode) `stg_cp_barcodes`, count(distinct case when record_type = 'Baseline' then cp_no end) `bl_cp_raw`, 
count(distinct case when record_type = 'Endline' then cp_no end) `el_cp_raw`
 from {{ref('int_student_global_stg_cp')}} where created_from_form=True 
 group by batch_no, batch_academic_year, batch_grade, batch_language, facilitator_name, school_name, school_academic_year, school_language, school_taluka, 
 school_ward, school_district, school_state, school_partner, batch_donor),

cs as (select batch_no, batch_academic_year, batch_grade, batch_language, facilitator_name, school_name, school_academic_year, school_language, 
school_taluka, school_ward, school_district, school_state, school_partner, batch_donor, count(distinct student_barcode) `stg_cs_sd`, 
count(distinct assessment_barcode) `stg_cs_barcodes`, count(distinct case when record_type = 'Baseline' then cs_no end) `bl_cs_raw`, 
count(distinct case when record_type = 'Endline' then cs_no end) `el_cs_raw`
 from {{ref('int_student_global_stg_cs')}} where created_from_form=True 
 group by batch_no, batch_academic_year, batch_grade, batch_language, facilitator_name, school_name, school_academic_year, school_language, school_taluka, 
 school_ward, school_district, school_state, school_partner, batch_donor),

fp as (select batch_no, batch_academic_year, batch_grade, batch_language, facilitator_name, school_name, school_academic_year, school_language, 
school_taluka, school_ward, school_district, school_state, school_partner, batch_donor, count(distinct student_barcode) `stg_fp_sd`, 
count(distinct assessment_barcode) `stg_fp_barcodes`, count(distinct case when record_type = 'Baseline' then fp_no end) `bl_fp_raw`, 
count(distinct case when record_type = 'Endline' then fp_no end) `el_fp_raw`
 from {{ref('int_student_global_stg_fp')}} where created_from_form=True 
 group by batch_no, batch_academic_year, batch_grade, batch_language, facilitator_name, school_name, school_academic_year, school_language, school_taluka, 
 school_ward, school_district, school_state, school_partner, batch_donor),

saf as (select batch_no, batch_academic_year, batch_grade, batch_language, facilitator_name, school_name, school_academic_year, school_language, 
school_taluka, school_ward, school_district, school_state, school_partner, batch_donor, count(distinct student_barcode) `stg_saf_sd`, 
count(distinct assessment_barcode) `stg_saf_barcodes`, count(distinct case when record_type = 'Baseline' then saf_no end) `bl_saf_raw`, 
count(distinct case when record_type = 'Endline' then saf_no end) `el_saf_raw`
 from {{ref('int_student_global_stg_saf')}} where created_from_form=True 
 group by batch_no, batch_academic_year, batch_grade, batch_language, facilitator_name, school_name, school_academic_year, school_language, school_taluka, 
 school_ward, school_district, school_state, school_partner, batch_donor),

sar as (select batch_no, batch_academic_year, batch_grade, batch_language, facilitator_name, school_name, school_academic_year, school_language, 
school_taluka, school_ward, school_district, school_state, school_partner, batch_donor, count(distinct student_barcode) `stg_sar_sd`, 
count(distinct assessment_barcode) `stg_sar_barcodes`, count(distinct case when record_type = 'Baseline' then sar_no end) `bl_sar_raw`, 
count(distinct case when record_type = 'Endline' then sar_no end) `el_sar_raw`
 from {{ref('int_student_global_stg_sar')}} where created_from_form=True 
 group by batch_no, batch_academic_year, batch_grade, batch_language, facilitator_name, school_name, school_academic_year, school_language, school_taluka, 
 school_ward, school_district, school_state, school_partner, batch_donor),

fct_global_assessment_raw_uploads as 

(select 

coalesce(cdm1.batch_no, cdm2.batch_no, cp.batch_no, cs.batch_no, fp.batch_no, saf.batch_no, sar.batch_no) as batch_no,
coalesce(cdm1.batch_academic_year, cdm2.batch_academic_year, cp.batch_academic_year, cs.batch_academic_year, fp.batch_academic_year, saf.batch_academic_year, sar.batch_academic_year) as batch_academic_year,
coalesce(cdm1.batch_grade, cdm2.batch_grade, cp.batch_grade, cs.batch_grade, fp.batch_grade, saf.batch_grade, sar.batch_grade) as batch_grade,
coalesce(cdm1.batch_language, cdm2.batch_language, cp.batch_language, cs.batch_language, fp.batch_language, saf.batch_language, sar.batch_language) as batch_language,
coalesce(cdm1.facilitator_name, cdm2.facilitator_name, cp.facilitator_name, cs.facilitator_name, fp.facilitator_name, saf.facilitator_name, sar.facilitator_name) as facilitator_name,
coalesce(cdm1.school_name, cdm2.school_name, cp.school_name, cs.school_name, fp.school_name, saf.school_name, sar.school_name) as school_name,
coalesce(cdm1.school_academic_year, cdm2.school_academic_year, cp.school_academic_year, cs.school_academic_year, fp.school_academic_year, saf.school_academic_year, sar.school_academic_year) as school_academic_year,
coalesce(cdm1.school_language, cdm2.school_language, cp.school_language, cs.school_language, fp.school_language, saf.school_language, sar.school_language) as school_language,
coalesce(cdm1.school_taluka, cdm2.school_taluka, cp.school_taluka, cs.school_taluka, fp.school_taluka, saf.school_taluka, sar.school_taluka) as school_taluka,
coalesce(cdm1.school_ward, cdm2.school_ward, cp.school_ward, cs.school_ward, fp.school_ward, saf.school_ward, sar.school_ward) as school_ward,
coalesce(cdm1.school_district, cdm2.school_district, cp.school_district, cs.school_district, fp.school_district, saf.school_district, sar.school_district) as school_district,
coalesce(cdm1.school_state, cdm2.school_state, cp.school_state, cs.school_state, fp.school_state, saf.school_state, sar.school_state) as school_state,
coalesce(cdm1.school_partner, cdm2.school_partner, cp.school_partner, cs.school_partner, fp.school_partner, saf.school_partner, sar.school_partner) as school_partner,
coalesce(cdm1.batch_donor, cdm2.batch_donor, cp.batch_donor, cs.batch_donor, fp.batch_donor, saf.batch_donor, sar.batch_donor) as batch_donor,


cdm1.* except (batch_no, batch_academic_year, batch_grade, batch_language, facilitator_name, school_name, school_academic_year, school_language, school_taluka, school_ward, school_district, school_state, school_partner, batch_donor),
cdm2.* except (batch_no, batch_academic_year, batch_grade, batch_language, facilitator_name, school_name, school_academic_year, school_language, school_taluka, school_ward, school_district, school_state, school_partner, batch_donor),
cp.* except (batch_no, batch_academic_year, batch_grade, batch_language, facilitator_name, school_name, school_academic_year, school_language, school_taluka, school_ward, school_district, school_state, school_partner, batch_donor),
cs.* except (batch_no, batch_academic_year, batch_grade, batch_language, facilitator_name, school_name, school_academic_year, school_language, school_taluka, school_ward, school_district, school_state, school_partner, batch_donor),
fp.* except (batch_no, batch_academic_year, batch_grade, batch_language, facilitator_name, school_name, school_academic_year, school_language, school_taluka, school_ward, school_district, school_state, school_partner, batch_donor),
saf.* except (batch_no, batch_academic_year, batch_grade, batch_language, facilitator_name, school_name, school_academic_year, school_language, school_taluka, school_ward, school_district, school_state, school_partner, batch_donor),
sar.* except (batch_no, batch_academic_year, batch_grade, batch_language, facilitator_name, school_name, school_academic_year, school_language, school_taluka, school_ward, school_district, school_state, school_partner, batch_donor),

from 
            cdm1 

            full outer join cdm2 on cdm1.batch_no = cdm2.batch_no 
            full outer join cp on coalesce(cdm1.batch_no, cdm2.batch_no) =  cp.batch_no 
            full outer join cs on coalesce(cdm1.batch_no, cdm2.batch_no, cp.batch_no) =  cs.batch_no 
            full outer join fp on coalesce(cdm1.batch_no, cdm2.batch_no, cp.batch_no, 
            cs.batch_no) =  fp.batch_no 
            full outer join saf on coalesce(cdm1.batch_no, cdm2.batch_no, cp.batch_no, cs.batch_no, 
            fp.batch_no) = saf.batch_no
            full outer join sar on coalesce(cdm1.batch_no, cdm2.batch_no, cp.batch_no, cs.batch_no, 
            fp.batch_no, saf.batch_no) = sar.batch_no 


            order by batch_no
    )

select * from fct_global_assessment_raw_uploads


















