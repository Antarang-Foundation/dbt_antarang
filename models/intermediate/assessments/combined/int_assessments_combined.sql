with
    cdm1 as (select * from {{ ref('int_pivot_cdm1_latest') }}),
    cdm2 as (select * from {{ ref('int_pivot_cdm2_latest') }}),
    cp as (select * from {{ ref('int_pivot_cp_latest') }}),
    cs as (select * from {{ ref('int_pivot_cs_latest') }}),
    fp as (select * from {{ ref('int_pivot_fp_latest') }}),
    saf as (select * from {{ ref('int_saf_latest') }}),
    sar as (select * from {{ ref('int_sar_latest') }}),

    int_assessments_combined as (
        select 

        (case 
        when cdm1.barcode is not null then cdm1.barcode
        when cdm2.barcode is not null then cdm2.barcode
        when cp.barcode is not null then cp.barcode
        when cs.barcode is not null then cs.barcode
        when fp.barcode is not null then fp.barcode
        when saf.barcode is not null then saf.barcode
        when sar.barcode is not null then sar.barcode end) as barcode,

        (case

        when (bl_cdm1_no is null and el_cdm1_no is null) then 'Neither'
        when (bl_cdm1_no is not null and el_cdm1_no is null) then 'Only_BL'
        when (bl_cdm1_no is null and el_cdm1_no is not null) then 'Only_EL'
        when (bl_cdm1_no is not null and el_cdm1_no is not null) then 'Both' end) cdm1_status,

        (case

        when (bl_cdm2_no is null and el_cdm2_no is null) then 'Neither'
        when (bl_cdm2_no is not null and el_cdm2_no is null) then 'Only_BL'
        when (bl_cdm2_no is null and el_cdm2_no is not null) then 'Only_EL'
        when (bl_cdm2_no is not null and el_cdm2_no is not null) then 'Both' end) cdm2_status,

        (case

        when (bl_cp_no is null and el_cp_no is null) then 'Neither'
        when (bl_cp_no is not null and el_cp_no is null) then 'Only_BL'
        when (bl_cp_no is null and el_cp_no is not null) then 'Only_EL'
        when (bl_cp_no is not null and el_cp_no is not null) then 'Both' end) cp_status,

        (case

        when (bl_cs_no is null and el_cs_no is null) then 'Neither'
        when (bl_cs_no is not null and el_cs_no is null) then 'Only_BL'
        when (bl_cs_no is null and el_cs_no is not null) then 'Only_EL'
        when (bl_cs_no is not null and el_cs_no is not null) then 'Both' end) cs_status,

        (case

        when (bl_fp_no is null and el_fp_no is null) then 'Neither'
        when (bl_fp_no is not null and el_fp_no is null) then 'Only_BL'
        when (bl_fp_no is null and el_fp_no is not null) then 'Only_EL'
        when (bl_fp_no is not null and el_fp_no is not null) then 'Both' end) fp_status,

        (case when saf_no is not null then 'Submitted' else 'Not Submitted' end) saf_status,
        
        (case when sar_no is not null then 'Submitted' else 'Not Submitted' end) sar_status,

        (case 
        when cdm1.bl_assessment_batch_id is not null then cdm1.bl_assessment_batch_id
	    when cdm1.el_assessment_batch_id is not null then cdm1.el_assessment_batch_id
        when cdm2.bl_assessment_batch_id is not null then cdm2.bl_assessment_batch_id
        when cdm2.el_assessment_batch_id is not null then cdm2.el_assessment_batch_id
        when cp.bl_assessment_batch_id is not null then cp.bl_assessment_batch_id
        when cp.el_assessment_batch_id is not null then cp.el_assessment_batch_id
        when cs.bl_assessment_batch_id is not null then cs.bl_assessment_batch_id
        when cs.el_assessment_batch_id is not null then cs.el_assessment_batch_id
        when fp.bl_assessment_batch_id is not null then fp.bl_assessment_batch_id
        when fp.el_assessment_batch_id is not null then fp.el_assessment_batch_id
        when saf.assessment_batch_id is not null then saf.assessment_batch_id
        when sar.assessment_batch_id is not null then sar.assessment_batch_id end) as assessment_batch_id,

        (case 
        when cdm1.bl_created_from_form is not null then cdm1.bl_created_from_form
	    when cdm1.el_created_from_form is not null then cdm1.el_created_from_form
        when cdm2.bl_created_from_form is not null then cdm2.bl_created_from_form
        when cdm2.el_created_from_form is not null then cdm2.el_created_from_form
        when cp.bl_created_from_form is not null then cp.bl_created_from_form
        when cp.el_created_from_form is not null then cp.el_created_from_form
        when cs.bl_created_from_form is not null then cs.bl_created_from_form
        when cs.el_created_from_form is not null then cs.el_created_from_form
        when fp.bl_created_from_form is not null then fp.bl_created_from_form
        when fp.el_created_from_form is not null then fp.el_created_from_form
        when saf.created_from_form is not null then saf.created_from_form
        when sar.created_from_form is not null then sar.created_from_form end) as created_from_form,

        (case 
        when cdm1.bl_assessment_grade is not null then cdm1.bl_assessment_grade
	    when cdm1.el_assessment_grade is not null then cdm1.el_assessment_grade
        when cdm2.bl_assessment_grade is not null then cdm2.bl_assessment_grade
        when cdm2.el_assessment_grade is not null then cdm2.el_assessment_grade
        when cp.bl_assessment_grade is not null then cp.bl_assessment_grade
        when cp.el_assessment_grade is not null then cp.el_assessment_grade
        when cs.bl_assessment_grade is not null then cs.bl_assessment_grade
        when cs.el_assessment_grade is not null then cs.el_assessment_grade
        when fp.bl_assessment_grade is not null then fp.bl_assessment_grade
        when fp.el_assessment_grade is not null then fp.el_assessment_grade
        when saf.assessment_grade is not null then saf.assessment_grade
        when sar.assessment_grade is not null then sar.assessment_grade end) as assessment_grade,

        (case 
        when cdm1.bl_assessment_academic_year is not null then cdm1.bl_assessment_academic_year
	    when cdm1.el_assessment_academic_year is not null then cdm1.el_assessment_academic_year
        when cdm2.bl_assessment_academic_year is not null then cdm2.bl_assessment_academic_year
        when cdm2.el_assessment_academic_year is not null then cdm2.el_assessment_academic_year
        when cp.bl_assessment_academic_year is not null then cp.bl_assessment_academic_year
        when cp.el_assessment_academic_year is not null then cp.el_assessment_academic_year
        when cs.bl_assessment_academic_year is not null then cs.bl_assessment_academic_year
        when cs.el_assessment_academic_year is not null then cs.el_assessment_academic_year
        when fp.bl_assessment_academic_year is not null then fp.bl_assessment_academic_year
        when fp.el_assessment_academic_year is not null then fp.el_assessment_academic_year
        when saf.assessment_academic_year is not null then saf.assessment_academic_year
        when sar.assessment_academic_year is not null then sar.assessment_academic_year end) as assessment_academic_year,
        
        cdm1.barcode as cdm1_barcode, cdm2.barcode as cdm2_barcode, cp.barcode as cp_barcode, cs.barcode as cs_barcode, fp.barcode as fp_barcode, 
        saf.barcode as saf_barcode, sar.barcode as sar_barcode, 

        cdm1.bl_assessment_batch_id as cdm1_bl_assessment_batch_id, cdm1.el_assessment_batch_id as cdm1_el_assessment_batch_id, 
        cdm2.bl_assessment_batch_id as cdm2_bl_assessment_batch_id, cdm2.el_assessment_batch_id as cdm2_el_assessment_batch_id, 
        cp.bl_assessment_batch_id as cp_bl_assessment_batch_id, cp.el_assessment_batch_id as cp_el_assessment_batch_id, 
        cs.bl_assessment_batch_id as cs_bl_assessment_batch_id, cs.el_assessment_batch_id as cs_el_assessment_batch_id, 
        fp.bl_assessment_batch_id as fp_bl_assessment_batch_id, fp.el_assessment_batch_id as fp_el_assessment_batch_id, 
        saf.assessment_batch_id as saf_assessment_batch_id, sar.assessment_batch_id as sar_assessment_batch_id, 

        cdm1.bl_created_on as cdm1_bl_created_on, cdm1.el_created_on as cdm1_el_created_on, 
        cdm2.bl_created_on as cdm2_bl_created_on, cdm2.el_created_on as cdm2_el_created_on, 
        cp.bl_created_on as cp_bl_created_on, cp.el_created_on as cp_el_created_on, 
        cs.bl_created_on as cs_bl_created_on, cs.el_created_on as cs_el_created_on, 
        fp.bl_created_on as fp_bl_created_on, fp.el_created_on as fp_el_created_on, 
        saf.created_on as saf_created_on, sar.created_on as sar_created_on, 

        cdm1.bl_created_from_form as cdm1_bl_created_from_form, cdm1.el_created_from_form as cdm1_el_created_from_form, 
        cdm2.bl_created_from_form as cdm2_bl_created_from_form, cdm2.el_created_from_form as cdm2_el_created_from_form, 
        cp.bl_created_from_form as cp_bl_created_from_form, cp.el_created_from_form as cp_el_created_from_form, 
        cs.bl_created_from_form as cs_bl_created_from_form, cs.el_created_from_form as cs_el_created_from_form, 
        fp.bl_created_from_form as fp_bl_created_from_form, fp.el_created_from_form as fp_el_created_from_form, 
        saf.created_from_form as saf_created_from_form, sar.created_from_form as sar_created_from_form, 
        
        cdm1.bl_assessment_grade as cdm1_bl_assessment_grade, cdm1.el_assessment_grade as cdm1_el_assessment_grade, 
        cdm2.bl_assessment_grade as cdm2_bl_assessment_grade, cdm2.el_assessment_grade as cdm2_el_assessment_grade, 
        cp.bl_assessment_grade as cp_bl_assessment_grade, cp.el_assessment_grade as cp_el_assessment_grade, 
        cs.bl_assessment_grade as cs_bl_assessment_grade, cs.el_assessment_grade as cs_el_assessment_grade, 
        fp.bl_assessment_grade as fp_bl_assessment_grade, fp.el_assessment_grade as fp_el_assessment_grade, 
        saf.assessment_grade as saf_assessment_grade, sar.assessment_grade as sar_assessment_grade, 

        cdm1.bl_assessment_academic_year as cdm1_bl_assessment_academic_year, cdm1.el_assessment_academic_year as cdm1_el_assessment_academic_year,
        cdm2.bl_assessment_academic_year as cdm2_bl_assessment_academic_year, cdm2.el_assessment_academic_year as cdm2_el_assessment_academic_year, 
        cp.bl_assessment_academic_year as cp_bl_assessment_academic_year, cp.el_assessment_academic_year as cp_el_assessment_academic_year, 
        cs.bl_assessment_academic_year as cs_bl_assessment_academic_year, cs.el_assessment_academic_year as cs_el_assessment_academic_year, 
        fp.bl_assessment_academic_year as fp_bl_assessment_academic_year, fp.el_assessment_academic_year as fp_el_assessment_academic_year, 
        saf.assessment_academic_year as saf_assessment_academic_year, sar.assessment_academic_year as sar_assessment_academic_year,
        
        cdm1.* except (barcode, bl_assessment_batch_id, bl_created_on, bl_created_from_form, bl_assessment_grade, bl_assessment_academic_year, 
        el_assessment_batch_id, el_created_on, el_created_from_form, el_assessment_grade, el_assessment_academic_year),

        cdm2.* except (barcode, bl_assessment_batch_id, bl_created_on, bl_created_from_form, bl_assessment_grade, bl_assessment_academic_year, 
        el_assessment_batch_id, el_created_on, el_created_from_form, el_assessment_grade, el_assessment_academic_year),

        cp.* except (barcode, bl_assessment_batch_id, bl_created_on, bl_created_from_form, bl_assessment_grade, bl_assessment_academic_year, 
        el_assessment_batch_id, el_created_on, el_created_from_form, el_assessment_grade, el_assessment_academic_year),

        cs.* except (barcode, bl_assessment_batch_id, bl_created_on, bl_created_from_form, bl_assessment_grade, bl_assessment_academic_year, 
        el_assessment_batch_id, el_created_on, el_created_from_form, el_assessment_grade, el_assessment_academic_year),

        fp.* except (barcode, bl_assessment_batch_id, bl_created_on, bl_created_from_form, bl_assessment_grade, bl_assessment_academic_year, 
        el_assessment_batch_id, el_created_on, el_created_from_form, el_assessment_grade, el_assessment_academic_year),
        
        saf.* except (barcode, record_type, assessment_batch_id, created_on, created_from_form, assessment_grade, assessment_academic_year, 
        error_status, data_cleanup, marks_recalculated, student_linked, is_latest),

        sar.* except (barcode, record_type, assessment_batch_id, created_on, created_from_form, assessment_grade, assessment_academic_year, 
        error_status, data_cleanup, marks_recalculated, student_linked, is_latest)  

        from 
            cdm1 
            
            full outer join cdm2 on cdm1.barcode = cdm2.barcode and 
            (case when cdm1.bl_assessment_academic_year is not null then cdm1.bl_assessment_academic_year
            when cdm1.el_assessment_academic_year is not null then cdm1.el_assessment_academic_year end) =
            (case when cdm2.bl_assessment_academic_year is not null then cdm2.bl_assessment_academic_year
            when cdm2.el_assessment_academic_year is not null then cdm2.el_assessment_academic_year end)

            full outer join cp on cdm1.barcode = cp.barcode and 
            (case when cdm1.bl_assessment_academic_year is not null then cdm1.bl_assessment_academic_year
            when cdm1.el_assessment_academic_year is not null then cdm1.el_assessment_academic_year end) =
            (case when cp.bl_assessment_academic_year is not null then cp.bl_assessment_academic_year
            when cp.el_assessment_academic_year is not null then cp.el_assessment_academic_year end)

            full outer join cs on cdm1.barcode = cs.barcode and 
            (case when cdm1.bl_assessment_academic_year is not null then cdm1.bl_assessment_academic_year
            when cdm1.el_assessment_academic_year is not null then cdm1.el_assessment_academic_year end) =
            (case when cs.bl_assessment_academic_year is not null then cs.bl_assessment_academic_year
            when cs.el_assessment_academic_year is not null then cs.el_assessment_academic_year end)

            full outer join fp on cdm1.barcode = fp.barcode and 
            (case when cdm1.bl_assessment_academic_year is not null then cdm1.bl_assessment_academic_year
            when cdm1.el_assessment_academic_year is not null then cdm1.el_assessment_academic_year end) =
            (case when fp.bl_assessment_academic_year is not null then fp.bl_assessment_academic_year
            when fp.el_assessment_academic_year is not null then fp.el_assessment_academic_year end)

            full outer join saf on cdm1.barcode = saf.barcode and 
            (case when cdm1.bl_assessment_academic_year is not null then cdm1.bl_assessment_academic_year
            when cdm1.el_assessment_academic_year is not null then cdm1.el_assessment_academic_year end) = saf.assessment_academic_year

            full outer join sar on cdm1.barcode = sar.barcode and 
            (case when cdm1.bl_assessment_academic_year is not null then cdm1.bl_assessment_academic_year
            when cdm1.el_assessment_academic_year is not null then cdm1.el_assessment_academic_year end) = sar.assessment_academic_year

            order by barcode, assessment_academic_year
    )

    select * from int_assessments_combined 