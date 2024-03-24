with t0 as (select * from {{ref('int_assessment_raw')}}),

t1 as (

select assessment_barcode, assessment_batch_id, created_from_form, assessment_grade, assessment_academic_year, 

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
        
        cdm1_bl_created_on, cdm1_el_created_on, cdm2_bl_created_on, cdm2_el_created_on, cp_bl_created_on, cp_el_created_on, cs_bl_created_on, 
        cs_el_created_on, fp_bl_created_on, fp_el_created_on, saf_created_on, sar_created_on,

        (case

        when bl_q1 then 1 else 0 end) bl_cdm1_isnotnull, 

        

        cdm1_is_null

        





from t0)

select * from t1
















select 

School_State, Batch_Trainer_Name, B_Grade, Batch_Number, OMR_Barcode, Student_Full_Name, Gender,

Submitted_Assessments,

CDM1_OMR_Barcode, 

(case

when CDM1_Status = 'Both' then '1. Both'
when CDM1_Status = 'Only_BL' then '2. Only_BL'
when CDM1_Status = 'Only_EL' then '3. Only_EL'
when CDM1_Status = 'Neither' then '4. Neither'

else null end) CDM1_Status, 

(case

when CDM1_Status = 'Neither' then '1. Both_Pending'
when CDM1_Status = 'Only_EL' then '2. Pending_BL'
when CDM1_Status = 'Only_BL' then '3. Pending_EL'
when CDM1_Status = 'Both' then '4. Both_Done'
else null end) CDM1_Facilitator_Action,

CDM1_BL_OMR_Barcode, CDM1_EL_OMR_Barcode,

CDM2_OMR_Barcode, 

(case

when CDM2_Status = 'Both' and B_Grade in ('Grade 9', 'Grade 10') then '1. Completed'
when CDM2_Status = 'Both' and B_Grade in ('Grade 11', 'Grade 12') then 'Val: 1a.Completed (Invalid_BL)'

when CDM2_Status = 'Only_BL' and B_Grade in ('Grade 9', 'Grade 10') then '2. Only_BL'
when CDM2_Status = 'Only_BL' and B_Grade in ('Grade 11', 'Grade 12') then 'Val: 2a. Only_BL(Invalid)'

when CDM2_Status = 'Only_EL' and B_Grade in ('Grade 9', 'Grade 10') then '3. Only_EL'
when CDM2_Status = 'Only_EL' and B_Grade in ('Grade 11', 'Grade 12') then 'Val: 1b. Completed (BL_NA)'

when CDM2_Status = 'Neither' and B_Grade in ('Grade 9', 'Grade 10') then '4. Neither'
when CDM2_Status = 'Neither' and B_Grade in ('Grade 11', 'Grade 12') then 'Val: 4a. Neither (BL_NA)'

else null end) CDM2_Status,


(case

when CDM2_Status = 'Neither' and B_Grade in ('Grade 9', 'Grade 10') then '1. Both_Pending'
when CDM2_Status = 'Neither' and B_Grade in ('Grade 11', 'Grade 12') then '3. Pending_EL'

when CDM2_Status = 'Only_BL' and B_Grade in ('Grade 9', 'Grade 10') then '3. Pending_EL'
when CDM2_Status = 'Only_BL' and B_Grade in ('Grade 11', 'Grade 12') then '3. Pending_EL'

when CDM2_Status = 'Only_EL' and B_Grade in ('Grade 9', 'Grade 10') then '2. Pending_BL'
when CDM2_Status = 'Only_EL' and B_Grade in ('Grade 11', 'Grade 12') then '4. Completed'

when CDM2_Status = 'Both' and B_Grade in ('Grade 9', 'Grade 10') then '4. Completed'
when CDM2_Status = 'Both' and B_Grade in ('Grade 11', 'Grade 12') then '4. Completed'

else null end) CDM2_Facilitator_Action, 

CDM2_BL_OMR_Barcode, 

(case 

when B_Grade in ('Grade 9', 'Grade 10') then CDM2_BL_OMR_Barcode
else null end
) Val_CDM2_BL_OMR_Barcode,

CDM2_EL_OMR_Barcode,

CP_OMR_Barcode, 

(case

when CP_Status = 'Both' and B_Grade in ('Grade 9', 'Grade 10') then '1. Completed'
when CP_Status = 'Both' and B_Grade in ('Grade 11', 'Grade 12') then 'Val: 1a.Completed (Invalid_BL)'

when CP_Status = 'Only_BL' and B_Grade in ('Grade 9', 'Grade 10') then '2. Only_BL'
when CP_Status = 'Only_BL' and B_Grade in ('Grade 11', 'Grade 12') then 'Val: 2a. Only_BL(Invalid)'

when CP_Status = 'Only_EL' and B_Grade in ('Grade 9', 'Grade 10') then '3. Only_EL'
when CP_Status = 'Only_EL' and B_Grade in ('Grade 11', 'Grade 12') then 'Val: 1b. Completed (BL_NA)'

when CP_Status = 'Neither' and B_Grade in ('Grade 9', 'Grade 10') then '4. Neither'
when CP_Status = 'Neither' and B_Grade in ('Grade 11', 'Grade 12') then 'Val: 4a. Neither (BL_NA)'

else null end) CP_Status,

(case

when CP_Status = 'Neither' and B_Grade in ('Grade 9', 'Grade 10') then '1. Both_Pending'
when CP_Status = 'Neither' and B_Grade in ('Grade 11', 'Grade 12') then '3. Pending_EL'

when CP_Status = 'Only_BL' and B_Grade in ('Grade 9', 'Grade 10') then '3. Pending_EL'
when CP_Status = 'Only_BL' and B_Grade in ('Grade 11', 'Grade 12') then '3. Pending_EL'

when CP_Status = 'Only_EL' and B_Grade in ('Grade 9', 'Grade 10') then '2. Pending_BL'
when CP_Status = 'Only_EL' and B_Grade in ('Grade 11', 'Grade 12') then '4. Completed'

when CP_Status = 'Both' and B_Grade in ('Grade 9', 'Grade 10') then '4. Completed'
when CP_Status = 'Both' and B_Grade in ('Grade 11', 'Grade 12') then '4. Completed'

else null end) CP_Facilitator_Action, 

CP_BL_OMR_Barcode, 

(case 

when B_Grade in ('Grade 9', 'Grade 10') then CP_BL_OMR_Barcode
else null end
) Val_CP_BL_OMR_Barcode,

CP_EL_OMR_Barcode,

CS_OMR_Barcode, 

(case

when CS_Status = 'Both' and B_Grade in ('Grade 11', 'Grade 12') then '1. Completed'
when CS_Status = 'Both' and B_Grade in ('Grade 9', 'Grade 10') then 'Val: 1a.Completed (Invalid_BL)'

when CS_Status = 'Only_BL' and B_Grade in ('Grade 11', 'Grade 12') then '2. Only_BL'
when CS_Status = 'Only_BL' and B_Grade in ('Grade 9', 'Grade 10') then 'Val: 2a. Only_BL(Invalid)'

when CS_Status = 'Only_EL' and B_Grade in ('Grade 11', 'Grade 12') then '3. Only_EL'
when CS_Status = 'Only_EL' and B_Grade in ('Grade 9', 'Grade 10') then 'Val: 1b. Completed (BL_NA)'

when CS_Status = 'Neither' and B_Grade in ('Grade 11', 'Grade 12') then '4. Neither'
when CS_Status = 'Neither' and B_Grade in ('Grade 9', 'Grade 10') then 'Val: 4a. Neither (BL_NA)'

else null end) CS_Status, 

(case

when CS_Status = 'Neither' and B_Grade in ('Grade 11', 'Grade 12') then '1. Both_Pending'
when CS_Status = 'Neither' and B_Grade in ('Grade 9', 'Grade 10') then '3. Pending_EL'

when CS_Status = 'Only_BL' and B_Grade in ('Grade 11', 'Grade 12') then '3. Pending_EL'
when CS_Status = 'Only_BL' and B_Grade in ('Grade 9', 'Grade 10') then '3. Pending_EL'

when CS_Status = 'Only_EL' and B_Grade in ('Grade 11', 'Grade 12') then '2. Pending_BL'
when CS_Status = 'Only_EL' and B_Grade in ('Grade 9', 'Grade 10') then '4. Completed'

when CS_Status = 'Both' and B_Grade in ('Grade 11', 'Grade 12') then '4. Completed'
when CS_Status = 'Both' and B_Grade in ('Grade 9', 'Grade 10') then '4. Completed'

else null end) CS_Facilitator_Action,

CS_BL_OMR_Barcode, 

(case 

when B_Grade in ('Grade 11', 'Grade 12') then CS_BL_OMR_Barcode
else null end
) Val_CS_BL_OMR_Barcode,

CS_EL_OMR_Barcode,

PFF_OMR_Barcode, 

(case

when PFF_Status = 'Both' and B_Grade in ('Grade 11', 'Grade 12') then '1. Completed'
when PFF_Status = 'Both' and B_Grade in ('Grade 9', 'Grade 10') then 'Val: 1a.Completed (Invalid_BL)'

when PFF_Status = 'Only_BL' and B_Grade in ('Grade 11', 'Grade 12') then '2. Only_BL'
when PFF_Status = 'Only_BL' and B_Grade in ('Grade 9', 'Grade 10') then 'Val: 2a. Only_BL(Invalid)'

when PFF_Status = 'Only_EL' and B_Grade in ('Grade 11', 'Grade 12') then '3. Only_EL'
when PFF_Status = 'Only_EL' and B_Grade in ('Grade 9', 'Grade 10') then 'Val: 1b. Completed (BL_NA)'

when PFF_Status = 'Neither' and B_Grade in ('Grade 11', 'Grade 12') then '4. Neither'
when PFF_Status = 'Neither' and B_Grade in ('Grade 9', 'Grade 10') then 'Val: 4a. Neither (BL_NA)'

else null end) PFF_Status, 

(case

when PFF_Status = 'Neither' and B_Grade in ('Grade 11', 'Grade 12') then '1. Both_Pending'
when PFF_Status = 'Neither' and B_Grade in ('Grade 9', 'Grade 10') then '3. Pending_EL'

when PFF_Status = 'Only_BL' and B_Grade in ('Grade 11', 'Grade 12') then '3. Pending_EL'
when PFF_Status = 'Only_BL' and B_Grade in ('Grade 9', 'Grade 10') then '3. Pending_EL'

when PFF_Status = 'Only_EL' and B_Grade in ('Grade 11', 'Grade 12') then '2. Pending_BL'
when PFF_Status = 'Only_EL' and B_Grade in ('Grade 9', 'Grade 10') then '4. Completed'

when PFF_Status = 'Both' and B_Grade in ('Grade 11', 'Grade 12') then '4. Completed'
when PFF_Status = 'Both' and B_Grade in ('Grade 9', 'Grade 10') then '4. Completed'

else null end) PFF_Facilitator_Action,

PFF_BL_OMR_Barcode, 

(case 

when B_Grade in ('Grade 11', 'Grade 12') then PFF_BL_OMR_Barcode
else null end
) Val_PFF_BL_OMR_Barcode,

PFF_EL_OMR_Barcode,

Batch_Donor, School_Partner, School_District, School_Name, Batch_Language

from `antarang-dashboard.Views.Outcomes_23_Overall_1`