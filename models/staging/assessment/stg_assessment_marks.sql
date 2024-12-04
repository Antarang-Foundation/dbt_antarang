WITH source AS (
    SELECT * 
    FROM {{ source('salesforce', 'Assessment_Marks__c') }}
),

t1 AS (
    SELECT 
        Of_Student_CA_Track__c AS percentage_student_ca_track,
        Of_Student_CP_Track__c AS percentage_student_cp_track,
        Of_Student_SA_Track__c AS percentage_student_sa_track,
        Name AS assessment_marks_number,
        RecordTypeId AS assessment_record_type,
        Student__c AS assessment_student_id,
        Total_Marks__c AS total_marks
    FROM source
),

t2 AS (
    SELECT 
        student_id, 
        student_barcode, 
        student_grade, 
        possible_career_report,
        batch_no, 
        batch_academic_year, 
        batch_grade, 
        school_state, 
        school_district, 
        school_taluka, 
        school_partner, 
        batch_donor
    FROM {{ ref('int_student_global') }}
),

t3 AS (
    SELECT * 
    FROM t1 
    FULL OUTER JOIN t2 
    ON t1.assessment_student_id = t2.student_id
)
/*
t4 AS (
    SELECT 
        student_id, 
        student_barcode, 
        student_grade, 
        possible_career_report, 
        percentage_student_ca_track, 
        percentage_student_cp_track, 
        percentage_student_sa_track,
        assessment_marks_number, 
        assessment_record_type, 
        assessment_student_id, 
        total_marks,
        batch_no, 
        batch_academic_year, 
        batch_grade, 
        school_state, 
        school_district, 
        school_taluka, 
        school_partner, 
        batch_donor,
        CASE     --this case will not work because possible_career_report & percentage_student_sa_track are both not string or not integer. both fields data type is different.
            WHEN batch_academic_year <= 2021 THEN possible_career_report
            WHEN batch_academic_year = 2021 THEN percentage_student_sa_track
            ELSE 0
        END AS self_aware_career_choice
    FROM t3
)*/

SELECT * FROM t3
