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

                cdm1_bl_assessment_batch_id,
                cdm1_el_assessment_batch_id,
                cdm2_bl_assessment_batch_id,
                cdm2_el_assessment_batch_id,
                cp_bl_assessment_batch_id,
                cp_el_assessment_batch_id,
                cs_bl_assessment_batch_id,
                cs_el_assessment_batch_id,
                fp_bl_assessment_batch_id,
                fp_el_assessment_batch_id,
                saf_assessment_batch_id,
                sar_assessment_batch_id,

                cdm1_bl_created_from_form,
                cdm1_el_created_from_form,
                cdm2_bl_created_from_form,
                cdm2_el_created_from_form,
                cp_bl_created_from_form,
                cp_el_created_from_form,
                cs_bl_created_from_form,
                cs_el_created_from_form,
                fp_bl_created_from_form,
                fp_el_created_from_form,
                saf_created_from_form,
                sar_created_from_form,

                cdm1_bl_assessment_grade,
                cdm1_el_assessment_grade,
                cdm2_bl_assessment_grade,
                cdm2_el_assessment_grade,
                cp_bl_assessment_grade,
                cp_el_assessment_grade,
                cs_bl_assessment_grade,
                cs_el_assessment_grade,
                fp_bl_assessment_grade,
                fp_el_assessment_grade,
                saf_assessment_grade,
                sar_assessment_grade,

                cdm1_bl_assessment_academic_year,
                cdm1_el_assessment_academic_year,
                cdm2_bl_assessment_academic_year,
                cdm2_el_assessment_academic_year,
                cp_bl_assessment_academic_year,
                cp_el_assessment_academic_year,
                cs_bl_assessment_academic_year,
                cs_el_assessment_academic_year,
                fp_bl_assessment_academic_year,
                fp_el_assessment_academic_year,
                saf_assessment_academic_year,
                sar_assessment_academic_year
            )

        from t0
    )

select *
from int_assessments_combined_trimmed
