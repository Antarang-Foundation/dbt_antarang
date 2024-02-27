with
    cdm1 as (select * from {{ ref('int_pivot_cdm1_latest') }}),
    cdm2 as (select * from {{ ref('int_pivot_cdm2_raw_latest') }}),
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

        when (cdm1_no_Baseline is null and cdm1_no_Endline is null) then 'Neither'
        when (cdm1_no_Baseline is not null and cdm1_no_Endline is null) then 'Only_BL'
        when (cdm1_no_Baseline is null and cdm1_no_Endline is not null) then 'Only_EL'
        when (cdm1_no_Baseline is not null and cdm1_no_Endline is not null) then 'Both' end) cdm1_status,

        (case

        when (cdm2_no_Baseline is null and cdm2_no_Endline is null) then 'Neither'
        when (cdm2_no_Baseline is not null and cdm2_no_Endline is null) then 'Only_BL'
        when (cdm2_no_Baseline is null and cdm2_no_Endline is not null) then 'Only_EL'
        when (cdm2_no_Baseline is not null and cdm2_no_Endline is not null) then 'Both' end) cdm2_status,

        (case

        when (cp_no_Baseline is null and cp_no_Endline is null) then 'Neither'
        when (cp_no_Baseline is not null and cp_no_Endline is null) then 'Only_BL'
        when (cp_no_Baseline is null and cp_no_Endline is not null) then 'Only_EL'
        when (cp_no_Baseline is not null and cp_no_Endline is not null) then 'Both' end) cp_status,

        (case

        when (cs_no_Baseline is null and cs_no_Endline is null) then 'Neither'
        when (cs_no_Baseline is not null and cs_no_Endline is null) then 'Only_BL'
        when (cs_no_Baseline is null and cs_no_Endline is not null) then 'Only_EL'
        when (cs_no_Baseline is not null and cs_no_Endline is not null) then 'Both' end) cs_status,

        (case

        when (fp_no_Baseline is null and fp_no_Endline is null) then 'Neither'
        when (fp_no_Baseline is not null and fp_no_Endline is null) then 'Only_BL'
        when (fp_no_Baseline is null and fp_no_Endline is not null) then 'Only_EL'
        when (fp_no_Baseline is not null and fp_no_Endline is not null) then 'Both' end) fp_status,

        (case when saf_no is not null then 'Submitted' else 'Not Submitted' end) saf_status,
        
        (case when sar_no is not null then 'Submitted' else 'Not Submitted' end) sar_status,

        (case 
        when cdm1.assessment_batch_id_Baseline is not null then cdm1.assessment_batch_id_Baseline
	    when cdm1.assessment_batch_id_Endline is not null then cdm1.assessment_batch_id_Endline
        when cdm2.assessment_batch_id_Baseline is not null then cdm2.assessment_batch_id_Baseline
        when cdm2.assessment_batch_id_Endline is not null then cdm2.assessment_batch_id_Endline
        when cp.assessment_batch_id_Baseline is not null then cp.assessment_batch_id_Baseline
        when cp.assessment_batch_id_Endline is not null then cp.assessment_batch_id_Endline
        when cs.assessment_batch_id_Baseline is not null then cs.assessment_batch_id_Baseline
        when cs.assessment_batch_id_Endline is not null then cs.assessment_batch_id_Endline
        when fp.assessment_batch_id_Baseline is not null then fp.assessment_batch_id_Baseline
        when fp.assessment_batch_id_Endline is not null then fp.assessment_batch_id_Endline
        when saf.assessment_batch_id is not null then saf.assessment_batch_id
        when sar.assessment_batch_id is not null then sar.assessment_batch_id end) as assessment_batch_id,

        (case 
        when cdm1.created_from_form_Baseline is not null then cdm1.created_from_form_Baseline
	    when cdm1.created_from_form_Endline is not null then cdm1.created_from_form_Endline
        when cdm2.created_from_form_Baseline is not null then cdm2.created_from_form_Baseline
        when cdm2.created_from_form_Endline is not null then cdm2.created_from_form_Endline
        when cp.created_from_form_Baseline is not null then cp.created_from_form_Baseline
        when cp.created_from_form_Endline is not null then cp.created_from_form_Endline
        when cs.created_from_form_Baseline is not null then cs.created_from_form_Baseline
        when cs.created_from_form_Endline is not null then cs.created_from_form_Endline
        when fp.created_from_form_Baseline is not null then fp.created_from_form_Baseline
        when fp.created_from_form_Endline is not null then fp.created_from_form_Endline
        when saf.created_from_form is not null then saf.created_from_form
        when sar.created_from_form is not null then sar.created_from_form end) as created_from_form,

        (case 
        when cdm1.assessment_grade_Baseline is not null then cdm1.assessment_grade_Baseline
	    when cdm1.assessment_grade_Endline is not null then cdm1.assessment_grade_Endline
        when cdm2.assessment_grade_Baseline is not null then cdm2.assessment_grade_Baseline
        when cdm2.assessment_grade_Endline is not null then cdm2.assessment_grade_Endline
        when cp.assessment_grade_Baseline is not null then cp.assessment_grade_Baseline
        when cp.assessment_grade_Endline is not null then cp.assessment_grade_Endline
        when cs.assessment_grade_Baseline is not null then cs.assessment_grade_Baseline
        when cs.assessment_grade_Endline is not null then cs.assessment_grade_Endline
        when fp.assessment_grade_Baseline is not null then fp.assessment_grade_Baseline
        when fp.assessment_grade_Endline is not null then fp.assessment_grade_Endline
        when saf.assessment_grade is not null then saf.assessment_grade
        when sar.assessment_grade is not null then sar.assessment_grade end) as assessment_grade,

        (case 
        when cdm1.assessment_academic_year_Baseline is not null then cdm1.assessment_academic_year_Baseline
	    when cdm1.assessment_academic_year_Endline is not null then cdm1.assessment_academic_year_Endline
        when cdm2.assessment_academic_year_Baseline is not null then cdm2.assessment_academic_year_Baseline
        when cdm2.assessment_academic_year_Endline is not null then cdm2.assessment_academic_year_Endline
        when cp.assessment_academic_year_Baseline is not null then cp.assessment_academic_year_Baseline
        when cp.assessment_academic_year_Endline is not null then cp.assessment_academic_year_Endline
        when cs.assessment_academic_year_Baseline is not null then cs.assessment_academic_year_Baseline
        when cs.assessment_academic_year_Endline is not null then cs.assessment_academic_year_Endline
        when fp.assessment_academic_year_Baseline is not null then fp.assessment_academic_year_Baseline
        when fp.assessment_academic_year_Endline is not null then fp.assessment_academic_year_Endline
        when saf.assessment_academic_year is not null then saf.assessment_academic_year
        when sar.assessment_academic_year is not null then sar.assessment_academic_year end) as assessment_academic_year,
        
        cdm1.barcode as cdm1_barcode, cdm2.barcode as cdm2_barcode, cp.barcode as cp_barcode, cs.barcode as cs_barcode, fp.barcode as fp_barcode, saf.barcode as saf_barcode, sar.barcode as sar_barcode, 

        cdm1.assessment_batch_id_Baseline as cdm1_assessment_batch_id_Baseline, cdm1.assessment_batch_id_Endline as cdm1_assessment_batch_id_Endline, cdm2.assessment_batch_id_Baseline as cdm2_assessment_batch_id_Baseline, cdm2.assessment_batch_id_Endline as cdm2_assessment_batch_id_Endline, cp.assessment_batch_id_Baseline as cp_assessment_batch_id_Baseline, cp.assessment_batch_id_Endline as cp_assessment_batch_id_Endline, cs.assessment_batch_id_Baseline as cs_assessment_batch_id_Baseline, cs.assessment_batch_id_Endline as cs_assessment_batch_id_Endline, fp.assessment_batch_id_Baseline as fp_assessment_batch_id_Baseline, fp.assessment_batch_id_Endline as fp_assessment_batch_id_Endline, saf.assessment_batch_id as saf_assessment_batch_id, sar.assessment_batch_id as sar_assessment_batch_id, 

        cdm1.created_on_Baseline as cdm1_created_on_Baseline, cdm1.created_on_Endline as cdm1_created_on_Endline, cdm2.created_on_Baseline as cdm2_created_on_Baseline, cdm2.created_on_Endline as cdm2_created_on_Endline, cp.created_on_Baseline as cp_created_on_Baseline, cp.created_on_Endline as cp_created_on_Endline, cs.created_on_Baseline as cs_created_on_Baseline, cs.created_on_Endline as cs_created_on_Endline, fp.created_on_Baseline as fp_created_on_Baseline, fp.created_on_Endline as fp_created_on_Endline, saf.created_on as saf_created_on, sar.created_on as sar_created_on, 

        cdm1.created_from_form_Baseline as cdm1_created_from_form_Baseline, cdm1.created_from_form_Endline as cdm1_created_from_form_Endline, cdm2.created_from_form_Baseline as cdm2_created_from_form_Baseline, cdm2.created_from_form_Endline as cdm2_created_from_form_Endline, cp.created_from_form_Baseline as cp_created_from_form_Baseline, cp.created_from_form_Endline as cp_created_from_form_Endline, cs.created_from_form_Baseline as cs_created_from_form_Baseline, cs.created_from_form_Endline as cs_created_from_form_Endline, fp.created_from_form_Baseline as fp_created_from_form_Baseline, fp.created_from_form_Endline as fp_created_from_form_Endline, saf.created_from_form as saf_created_from_form, sar.created_from_form as sar_created_from_form, 
        
        cdm1.assessment_grade_Baseline as cdm1_assessment_grade_Baseline, cdm1.assessment_grade_Endline as cdm1_assessment_grade_Endline, cdm2.assessment_grade_Baseline as cdm2_assessment_grade_Baseline, cdm2.assessment_grade_Endline as cdm2_assessment_grade_Endline, cp.assessment_grade_Baseline as cp_assessment_grade_Baseline, cp.assessment_grade_Endline as cp_assessment_grade_Endline, cs.assessment_grade_Baseline as cs_assessment_grade_Baseline, cs.assessment_grade_Endline as cs_assessment_grade_Endline, fp.assessment_grade_Baseline as fp_assessment_grade_Baseline, fp.assessment_grade_Endline as fp_assessment_grade_Endline, saf.assessment_grade as saf_assessment_grade, sar.assessment_grade as sar_assessment_grade, 

        cdm1.assessment_academic_year_Baseline as cdm1_assessment_academic_year_Baseline, cdm1.assessment_academic_year_Endline as cdm1_assessment_academic_year_Endline, cdm2.assessment_academic_year_Baseline as cdm2_assessment_academic_year_Baseline, cdm2.assessment_academic_year_Endline as cdm2_assessment_academic_year_Endline, cp.assessment_academic_year_Baseline as cp_assessment_academic_year_Baseline, cp.assessment_academic_year_Endline as cp_assessment_academic_year_Endline, cs.assessment_academic_year_Baseline as cs_assessment_academic_year_Baseline, cs.assessment_academic_year_Endline as cs_assessment_academic_year_Endline, fp.assessment_academic_year_Baseline as fp_assessment_academic_year_Baseline, fp.assessment_academic_year_Endline as fp_assessment_academic_year_Endline, saf.assessment_academic_year as saf_assessment_academic_year, sar.assessment_academic_year as sar_assessment_academic_year,
        
        cdm1.* except (barcode, assessment_batch_id_Baseline, created_on_Baseline, created_from_form_Baseline, assessment_grade_Baseline, assessment_academic_year_Baseline, assessment_batch_id_Endline, created_on_Endline, created_from_form_Endline, assessment_grade_Endline, assessment_academic_year_Endline),

        cdm2.* except (barcode, assessment_batch_id_Baseline, created_on_Baseline, created_from_form_Baseline, assessment_grade_Baseline, assessment_academic_year_Baseline, assessment_batch_id_Endline, created_on_Endline, created_from_form_Endline, assessment_grade_Endline, assessment_academic_year_Endline),

        cp.* except (barcode, assessment_batch_id_Baseline, created_on_Baseline, created_from_form_Baseline, assessment_grade_Baseline, assessment_academic_year_Baseline, assessment_batch_id_Endline, created_on_Endline, created_from_form_Endline, assessment_grade_Endline, assessment_academic_year_Endline),

        cs.* except (barcode, assessment_batch_id_Baseline, created_on_Baseline, created_from_form_Baseline, assessment_grade_Baseline, assessment_academic_year_Baseline, assessment_batch_id_Endline, created_on_Endline, created_from_form_Endline, assessment_grade_Endline, assessment_academic_year_Endline),

        fp.* except (barcode, assessment_batch_id_Baseline, created_on_Baseline, created_from_form_Baseline, assessment_grade_Baseline, assessment_academic_year_Baseline, assessment_batch_id_Endline, created_on_Endline, created_from_form_Endline, assessment_grade_Endline, assessment_academic_year_Endline),
        
        saf.* except (barcode, record_type, assessment_batch_id, created_on, created_from_form, assessment_grade, assessment_academic_year, error_status, data_cleanup, marks_recalculated, student_linked, is_latest),

        sar.* except (barcode, record_type, assessment_batch_id, created_on, created_from_form, assessment_grade, assessment_academic_year, error_status, data_cleanup, marks_recalculated, student_linked, is_latest)  

        from 
            cdm1 
            
            full outer join cdm2 on cdm1.barcode = cdm2.barcode and 
            (case when cdm1.assessment_academic_year_Baseline is not null then cdm1.assessment_academic_year_Baseline
            when cdm1.assessment_academic_year_Endline is not null then cdm1.assessment_academic_year_Endline end) =
            (case when cdm2.assessment_academic_year_Baseline is not null then cdm2.assessment_academic_year_Baseline
            when cdm2.assessment_academic_year_Endline is not null then cdm2.assessment_academic_year_Endline end)

            full outer join cp on cdm1.barcode = cp.barcode and 
            (case when cdm1.assessment_academic_year_Baseline is not null then cdm1.assessment_academic_year_Baseline
            when cdm1.assessment_academic_year_Endline is not null then cdm1.assessment_academic_year_Endline end) =
            (case when cp.assessment_academic_year_Baseline is not null then cp.assessment_academic_year_Baseline
            when cp.assessment_academic_year_Endline is not null then cp.assessment_academic_year_Endline end)

            full outer join cs on cdm1.barcode = cs.barcode and 
            (case when cdm1.assessment_academic_year_Baseline is not null then cdm1.assessment_academic_year_Baseline
            when cdm1.assessment_academic_year_Endline is not null then cdm1.assessment_academic_year_Endline end) =
            (case when cs.assessment_academic_year_Baseline is not null then cs.assessment_academic_year_Baseline
            when cs.assessment_academic_year_Endline is not null then cs.assessment_academic_year_Endline end)

            full outer join fp on cdm1.barcode = fp.barcode and 
            (case when cdm1.assessment_academic_year_Baseline is not null then cdm1.assessment_academic_year_Baseline
            when cdm1.assessment_academic_year_Endline is not null then cdm1.assessment_academic_year_Endline end) =
            (case when fp.assessment_academic_year_Baseline is not null then fp.assessment_academic_year_Baseline
            when fp.assessment_academic_year_Endline is not null then fp.assessment_academic_year_Endline end)

            full outer join saf on cdm1.barcode = saf.barcode and 
            (case when cdm1.assessment_academic_year_Baseline is not null then cdm1.assessment_academic_year_Baseline
            when cdm1.assessment_academic_year_Endline is not null then cdm1.assessment_academic_year_Endline end) = saf.assessment_academic_year

            full outer join sar on cdm1.barcode = sar.barcode and 
            (case when cdm1.assessment_academic_year_Baseline is not null then cdm1.assessment_academic_year_Baseline
            when cdm1.assessment_academic_year_Endline is not null then cdm1.assessment_academic_year_Endline end) = sar.assessment_academic_year

            order by barcode, assessment_academic_year
    )

    select * from int_assessments_combined 