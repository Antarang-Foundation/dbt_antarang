with

    t0 as (select * from {{ ref("int_assessments_combined") }}),

    int_assessments_combined_trimmed as (
        select

            * except (
                cdm1_barcode,
                cdm2_barcode,
                cp_barcode,
                cs_barcode,
                fp_barcode,
                saf_barcode,
                sar_barcode,

                cdm1_assessment_batch_id_baseline,
                cdm1_assessment_batch_id_endline,
                cdm2_assessment_batch_id_baseline,
                cdm2_assessment_batch_id_endline,
                cp_assessment_batch_id_baseline,
                cp_assessment_batch_id_endline,
                cs_assessment_batch_id_baseline,
                cs_assessment_batch_id_endline,
                fp_assessment_batch_id_baseline,
                fp_assessment_batch_id_endline,
                saf_assessment_batch_id,
                sar_assessment_batch_id,

                cdm1_created_from_form_baseline,
                cdm1_created_from_form_endline,
                cdm2_created_from_form_baseline,
                cdm2_created_from_form_endline,
                cp_created_from_form_baseline,
                cp_created_from_form_endline,
                cs_created_from_form_baseline,
                cs_created_from_form_endline,
                fp_created_from_form_baseline,
                fp_created_from_form_endline,
                saf_created_from_form,
                sar_created_from_form,

                cdm1_assessment_grade_baseline,
                cdm1_assessment_grade_endline,
                cdm2_assessment_grade_baseline,
                cdm2_assessment_grade_endline,
                cp_assessment_grade_baseline,
                cp_assessment_grade_endline,
                cs_assessment_grade_baseline,
                cs_assessment_grade_endline,
                fp_assessment_grade_baseline,
                fp_assessment_grade_endline,
                saf_assessment_grade,
                sar_assessment_grade,

                cdm1_assessment_academic_year_baseline,
                cdm1_assessment_academic_year_endline,
                cdm2_assessment_academic_year_baseline,
                cdm2_assessment_academic_year_endline,
                cp_assessment_academic_year_baseline,
                cp_assessment_academic_year_endline,
                cs_assessment_academic_year_baseline,
                cs_assessment_academic_year_endline,
                fp_assessment_academic_year_baseline,
                fp_assessment_academic_year_endline,
                saf_assessment_academic_year,
                sar_assessment_academic_year
            )

        from t0
    )

select *
from int_assessments_combined_trimmed
