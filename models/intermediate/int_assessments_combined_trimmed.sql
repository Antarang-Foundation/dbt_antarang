with 

t0 as (select * from {{ ref('int_assessments_combined') }}), 

int_assessments_combined_trimmed as (select 

* except (cdm1_barcode, cdm2_barcode, cp_barcode, cs_barcode, fp_barcode, saf_barcode, sar_barcode, 

cdm1_batch_id_Baseline, cdm1_batch_id_Endline, cdm2_batch_id_Baseline, cdm2_batch_id_Endline, cp_batch_id_Baseline, cp_batch_id_Endline, cs_batch_id_Baseline, cs_batch_id_Endline, fp_batch_id_Baseline, fp_batch_id_Endline, saf_batch_id, sar_batch_id, 

cdm1_created_from_form_Baseline, cdm1_created_from_form_Endline, cdm2_created_from_form_Baseline, cdm2_created_from_form_Endline, cp_created_from_form_Baseline, cp_created_from_form_Endline, cs_created_from_form_Baseline, cs_created_from_form_Endline, fp_created_from_form_Baseline, fp_created_from_form_Endline, saf_created_from_form, sar_created_from_form, 

cdm1_grade_Baseline, cdm1_grade_Endline, cdm2_grade_Baseline, cdm2_grade_Endline, cp_grade_Baseline, cp_grade_Endline, cs_grade_Baseline, cs_grade_Endline, fp_grade_Baseline, fp_grade_Endline, saf_grade, sar_grade, 

cdm1_academic_year_Baseline, cdm1_academic_year_Endline, cdm2_academic_year_Baseline, cdm2_academic_year_Endline, cp_academic_year_Baseline, cp_academic_year_Endline, cs_academic_year_Baseline, cs_academic_year_Endline, fp_academic_year_Baseline, fp_academic_year_Endline, saf_academic_year, sar_academic_year)

from t0)

select * from int_assessments_combined_trimmed