with source as (
    select * from {{ source('salesforce', 'Session_OMR_Type__c') }} where IsDeleted = false
),


renamed as (
    select
        Id as somrt_id,
        Name as somrt_no,
        Session__c as somrt_session_id,
        Session_Batch_Id__c as somrt_batch_id,
        OMR_Type__c as omr_type,
        OMR_Assessment_Object__c as omr_assessment_object,
        Case when (Source.OMR_Type__c ='Student Details 2')
        then Coalesce(student.BatchStudentCount,0) else  Source.OMR_Assessment_Count__c END as omr_assessment_count,
        OMR_Assessment_Record_Type__c as omr_assessment_record_type,
        Session_Batch_Number__c as somrt_batch_no,
        First_OMR_Uploaded_Date__c as first_omr_upload_date,
        OMRs_Received_Count__c as omr_received_count,
        OMR_Received_Date__c as omr_received_date,
        OMR_Received_By__c as omr_received_by,
        Number_of_Students_in_Batch__c as number_of_students_in_batch,
        --OMR_Submission_Notes__c as omr_submission_notes
         
    from source
    LEFT JOIN (
                Select  B.batch_id ,B.batch_grade,case when (B.batch_grade = 'Grade 9') then sum(S.G9_Batch_Student_Flag)
                when (B.batch_grade = 'Grade 10') then sum(S.G10_Batch_Student_Flag)
                when (B.batch_grade = 'Grade 11') then sum(S.G11_Batch_Student_Flag)
                when (B.batch_grade = 'Grade 12') then sum(S.G12_Batch_Student_Flag)
                ELSE 0 END  as BatchStudentCount
                ,'Student Details 2' Flagtype
                from {{ref('dev_stg_student')}} S
                INNER JOIN
                (Select batch_id,batch_grade from {{ref('dev_stg_batch')}}
                --where batch_id in ('a0oOW000002lnTxYAI' , 'a0oOW0000058JejYAE','a0oOW0000058JnNYAU')
                )B on B.batch_id =  case when (batch_grade = 'Grade 9') then g9_batch_id
                when (batch_grade = 'Grade 10') then g10_batch_id
                when (batch_grade = 'Grade 11') then g11_batch_id
                when (batch_grade = 'Grade 12') then g12_batch_id END
                group by B.batch_id,B.batch_grade
                )student -- For SD2
                on Source.Session_Batch_Id__c = student.batch_id and Source.OMR_Type__c = student.Flagtype
)


Select * From renamed
--where somrt_no = 'SOMRT-134610'
--where omr_type = 'Student Details 2' and omr_assessment_count >0.0 and somrt_no = 'SOMRT-123180'
 --where somrt_batch_id in ('a0oOW000002lnTxYAI' , 'a0oOW0000058JejYAE','a0oOW0000058JnNYAU')