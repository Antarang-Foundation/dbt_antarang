with source as (
    select * from {{ source('salesforce', 'Assessment_Marks__c')}}
),

t1 as (
    select
         Of_Student_CA_Track__c as percentage_student_ca_track,
         Of_Student_CP_Track__c as percentage_student_cp_track,
         Of_Student_SA_Track__c as percentage_student_sa_track,
         Name as assessment_marks_number,
         RecordTypeId as assessment_record_type,
         Student__c as assessment_student_id,
         Total_Marks__c as total_marks

    from source
    ),

t2 as (
    select student_id, student_barcode, student_grade, possible_career_report,
    batch_no, batch_academic_year, batch_grade, school_state, school_district, school_taluka, school_partner, batch_donor
    from {{ref('int_student_global')}}
),

t3 as (
    select * from t1 full outer join t2 on t1.assessment_student_id = t2.student_id
),

t4 as (
select 
student_id, student_barcode, student_grade, 
possible_career_report, percentage_student_ca_track, percentage_student_cp_track, percentage_student_sa_track,
assessment_marks_number, assessment_record_type, assessment_student_id, total_marks,
batch_no, batch_academic_year, batch_grade, school_state, school_district, school_taluka, school_partner, batch_donor,
CASE
            WHEN batch_academic_year <= 2021 THEN possible_career_report
            WHEN batch_academic_year = 2021 THEN percentage_student_sa_track
            ELSE 0
        END AS self_aware_career_choice

from t3 
)

select * from t4




