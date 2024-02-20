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
        when cdm1.batch_id_Baseline is not null then cdm1.batch_id_Baseline
	    when cdm1.batch_id_Endline is not null then cdm1.batch_id_Endline
        when cdm2.batch_id_Baseline is not null then cdm2.batch_id_Baseline
        when cdm2.batch_id_Endline is not null then cdm2.batch_id_Endline
        when cp.batch_id_Baseline is not null then cp.batch_id_Baseline
        when cp.batch_id_Endline is not null then cp.batch_id_Endline
        when cs.batch_id_Baseline is not null then cs.batch_id_Baseline
        when cs.batch_id_Endline is not null then cs.batch_id_Endline
        when fp.batch_id_Baseline is not null then fp.batch_id_Baseline
        when fp.batch_id_Endline is not null then fp.batch_id_Endline
        when saf.batch_id is not null then saf.batch_id
        when sar.batch_id is not null then sar.batch_id end) as batch_id,

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
        when cdm1.grade_Baseline is not null then cdm1.grade_Baseline
	    when cdm1.grade_Endline is not null then cdm1.grade_Endline
        when cdm2.grade_Baseline is not null then cdm2.grade_Baseline
        when cdm2.grade_Endline is not null then cdm2.grade_Endline
        when cp.grade_Baseline is not null then cp.grade_Baseline
        when cp.grade_Endline is not null then cp.grade_Endline
        when cs.grade_Baseline is not null then cs.grade_Baseline
        when cs.grade_Endline is not null then cs.grade_Endline
        when fp.grade_Baseline is not null then fp.grade_Baseline
        when fp.grade_Endline is not null then fp.grade_Endline
        when saf.grade is not null then saf.grade
        when sar.grade is not null then sar.grade end) as grade,

        (case 
        when cdm1.academic_year_Baseline is not null then cdm1.academic_year_Baseline
	    when cdm1.academic_year_Endline is not null then cdm1.academic_year_Endline
        when cdm2.academic_year_Baseline is not null then cdm2.academic_year_Baseline
        when cdm2.academic_year_Endline is not null then cdm2.academic_year_Endline
        when cp.academic_year_Baseline is not null then cp.academic_year_Baseline
        when cp.academic_year_Endline is not null then cp.academic_year_Endline
        when cs.academic_year_Baseline is not null then cs.academic_year_Baseline
        when cs.academic_year_Endline is not null then cs.academic_year_Endline
        when fp.academic_year_Baseline is not null then fp.academic_year_Baseline
        when fp.academic_year_Endline is not null then fp.academic_year_Endline
        when saf.academic_year is not null then saf.academic_year
        when sar.academic_year is not null then sar.academic_year end) as academic_year,
        
        cdm1.barcode as cdm1_barcode, cdm2.barcode as cdm2_barcode, cp.barcode as cp_barcode, cs.barcode as cs_barcode, fp.barcode as fp_barcode, saf.barcode as saf_barcode, sar.barcode as sar_barcode, 

        cdm1.batch_id_Baseline as cdm1_batch_id_Baseline, cdm1.batch_id_Endline as cdm1_batch_id_Endline, cdm2.batch_id_Baseline as cdm2_batch_id_Baseline, cdm2.batch_id_Endline as cdm2_batch_id_Endline, cp.batch_id_Baseline as cp_batch_id_Baseline, cp.batch_id_Endline as cp_batch_id_Endline, cs.batch_id_Baseline as cs_batch_id_Baseline, cs.batch_id_Endline as cs_batch_id_Endline, fp.batch_id_Baseline as fp_batch_id_Baseline, fp.batch_id_Endline as fp_batch_id_Endline, saf.batch_id as saf_batch_id, sar.batch_id as sar_batch_id, 

        cdm1.created_on_Baseline as cdm1_created_on_Baseline, cdm1.created_on_Endline as cdm1_created_on_Endline, cdm2.created_on_Baseline as cdm2_created_on_Baseline, cdm2.created_on_Endline as cdm2_created_on_Endline, cp.created_on_Baseline as cp_created_on_Baseline, cp.created_on_Endline as cp_created_on_Endline, cs.created_on_Baseline as cs_created_on_Baseline, cs.created_on_Endline as cs_created_on_Endline, fp.created_on_Baseline as fp_created_on_Baseline, fp.created_on_Endline as fp_created_on_Endline, saf.created_on as saf_created_on, sar.created_on as sar_created_on, 

        cdm1.created_from_form_Baseline as cdm1_created_from_form_Baseline, cdm1.created_from_form_Endline as cdm1_created_from_form_Endline, cdm2.created_from_form_Baseline as cdm2_created_from_form_Baseline, cdm2.created_from_form_Endline as cdm2_created_from_form_Endline, cp.created_from_form_Baseline as cp_created_from_form_Baseline, cp.created_from_form_Endline as cp_created_from_form_Endline, cs.created_from_form_Baseline as cs_created_from_form_Baseline, cs.created_from_form_Endline as cs_created_from_form_Endline, fp.created_from_form_Baseline as fp_created_from_form_Baseline, fp.created_from_form_Endline as fp_created_from_form_Endline, saf.created_from_form as saf_created_from_form, sar.created_from_form as sar_created_from_form, 
        
        cdm1.grade_Baseline as cdm1_grade_Baseline, cdm1.grade_Endline as cdm1_grade_Endline, cdm2.grade_Baseline as cdm2_grade_Baseline, cdm2.grade_Endline as cdm2_grade_Endline, cp.grade_Baseline as cp_grade_Baseline, cp.grade_Endline as cp_grade_Endline, cs.grade_Baseline as cs_grade_Baseline, cs.grade_Endline as cs_grade_Endline, fp.grade_Baseline as fp_grade_Baseline, fp.grade_Endline as fp_grade_Endline, saf.grade as saf_grade, sar.grade as sar_grade, 

        cdm1.academic_year_Baseline as cdm1_academic_year_Baseline, cdm1.academic_year_Endline as cdm1_academic_year_Endline, cdm2.academic_year_Baseline as cdm2_academic_year_Baseline, cdm2.academic_year_Endline as cdm2_academic_year_Endline, cp.academic_year_Baseline as cp_academic_year_Baseline, cp.academic_year_Endline as cp_academic_year_Endline, cs.academic_year_Baseline as cs_academic_year_Baseline, cs.academic_year_Endline as cs_academic_year_Endline, fp.academic_year_Baseline as fp_academic_year_Baseline, fp.academic_year_Endline as fp_academic_year_Endline, saf.academic_year as saf_academic_year, sar.academic_year as sar_academic_year,
        
        cdm1.* except (barcode, batch_id_Baseline, created_on_Baseline, created_from_form_Baseline, grade_Baseline, academic_year_Baseline, batch_id_Endline, created_on_Endline, created_from_form_Endline, grade_Endline, academic_year_Endline),

        cdm2.* except (barcode, batch_id_Baseline, created_on_Baseline, created_from_form_Baseline, grade_Baseline, academic_year_Baseline, batch_id_Endline, created_on_Endline, created_from_form_Endline, grade_Endline, academic_year_Endline),

        cp.* except (barcode, batch_id_Baseline, created_on_Baseline, created_from_form_Baseline, grade_Baseline, academic_year_Baseline, batch_id_Endline, created_on_Endline, created_from_form_Endline, grade_Endline, academic_year_Endline),

        cs.* except (barcode, batch_id_Baseline, created_on_Baseline, created_from_form_Baseline, grade_Baseline, academic_year_Baseline, batch_id_Endline, created_on_Endline, created_from_form_Endline, grade_Endline, academic_year_Endline),

        fp.* except (barcode, batch_id_Baseline, created_on_Baseline, created_from_form_Baseline, grade_Baseline, academic_year_Baseline, batch_id_Endline, created_on_Endline, created_from_form_Endline, grade_Endline, academic_year_Endline),
        
        saf.* except (barcode, record_type, batch_id, created_on, created_from_form, grade, academic_year, error_status, data_cleanup, marks_recalculated, student_linked, is_latest),

        sar.* except (barcode, record_type, batch_id, created_on, created_from_form, grade, academic_year, error_status, data_cleanup, marks_recalculated, student_linked, is_latest)  

        from 
            cdm1 
            
            full outer join cdm2 on cdm1.barcode = cdm2.barcode and 
            (case when cdm1.academic_year_Baseline is not null then cdm1.academic_year_Baseline
            when cdm1.academic_year_Endline is not null then cdm1.academic_year_Endline end) =
            (case when cdm2.academic_year_Baseline is not null then cdm2.academic_year_Baseline
            when cdm2.academic_year_Endline is not null then cdm2.academic_year_Endline end)

            full outer join cp on cdm1.barcode = cp.barcode and 
            (case when cdm1.academic_year_Baseline is not null then cdm1.academic_year_Baseline
            when cdm1.academic_year_Endline is not null then cdm1.academic_year_Endline end) =
            (case when cp.academic_year_Baseline is not null then cp.academic_year_Baseline
            when cp.academic_year_Endline is not null then cp.academic_year_Endline end)

            full outer join cs on cdm1.barcode = cs.barcode and 
            (case when cdm1.academic_year_Baseline is not null then cdm1.academic_year_Baseline
            when cdm1.academic_year_Endline is not null then cdm1.academic_year_Endline end) =
            (case when cs.academic_year_Baseline is not null then cs.academic_year_Baseline
            when cs.academic_year_Endline is not null then cs.academic_year_Endline end)

            full outer join fp on cdm1.barcode = fp.barcode and 
            (case when cdm1.academic_year_Baseline is not null then cdm1.academic_year_Baseline
            when cdm1.academic_year_Endline is not null then cdm1.academic_year_Endline end) =
            (case when fp.academic_year_Baseline is not null then fp.academic_year_Baseline
            when fp.academic_year_Endline is not null then fp.academic_year_Endline end)

            full outer join saf on cdm1.barcode = saf.barcode and 
            (case when cdm1.academic_year_Baseline is not null then cdm1.academic_year_Baseline
            when cdm1.academic_year_Endline is not null then cdm1.academic_year_Endline end) = saf.academic_year

            full outer join sar on cdm1.barcode = sar.barcode and 
            (case when cdm1.academic_year_Baseline is not null then cdm1.academic_year_Baseline
            when cdm1.academic_year_Endline is not null then cdm1.academic_year_Endline end) = sar.academic_year

            order by barcode, academic_year
    )

    select * from int_assessments_combined 