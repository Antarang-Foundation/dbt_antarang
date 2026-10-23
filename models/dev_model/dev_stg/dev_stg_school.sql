WITH t0 AS (
    SELECT
        id AS school_id,
        name AS school_name,
        RecordTypeId AS record_type_id,
        Medium_Language_of_Instruction__c AS school_language,
        CAST(Academic_Year__c AS STRING) AS school_academic_year,
        School_Timing__c AS school_timing,
        COALESCE(Partner__c, 'No Partner') AS school_partner,
        State__c AS school_state_id,
        District__c AS school_district_id,
        City__c AS school_taluka_id,
        Ward__c AS school_ward_id,
        Enrolled_Grade_9__c AS enrolled_g9,
        Enrolled_Grade_10__c AS enrolled_g10,
        Enrolled_Grade_11__c AS enrolled_g11,
        Enrolled_Grade_12__c AS enrolled_g12,
        Tagged_for_Counselling__c AS tagged_for_counselling,
        School_Area__c AS school_area,
        Address__c AS school_address,
        Principal_Name__c AS principal_name,
        Phone AS phone
    FROM {{ source('salesforce', 'Account') }}
    WHERE IsDeleted = FALSE
      AND LOWER(name) NOT LIKE '%test%'
),

t1 AS (
    SELECT record_type_id, record_type
    FROM {{ ref('seed_recordtype') }}
    WHERE record_type = 'School'
),

t2 AS (
    SELECT
        t0.school_id,
        t0.school_name,
        t0.school_language,
        t0.school_academic_year,
        t0.school_timing,
        t0.school_partner,
        t0.enrolled_g9,
        t0.enrolled_g10,
        t0.enrolled_g11,
        t0.enrolled_g12,
        t0.tagged_for_counselling,
        t0.school_area,
        t0.school_address,
        t1.record_type,
        t0.school_state_id,
        t0.school_district_id,
        t0.school_ward_id,
        t0.school_taluka_id,
        t0.principal_name,
        t0.phone
    FROM t0
    INNER JOIN t1
        ON t0.record_type_id = t1.record_type_id
),

t3 AS (
    SELECT state_name, state_id
    FROM {{ ref('seed_state') }}
),

t4 AS (
    SELECT district_name, district_id
    FROM {{ ref('seed_district') }}
),

t5 AS (
    SELECT ward_name, ward_id
    FROM {{ ref('seed_ward') }}
),

t6 AS (
    SELECT taluka_name, taluka_id
    FROM {{ ref('seed_taluka') }}
),

t7 AS (
    SELECT
        t2.*,
        t3.state_name AS school_state,
        t4.district_name AS school_district,
        t5.ward_name AS school_ward,
        t6.taluka_name AS school_taluka
    FROM t2
    LEFT JOIN t3
        ON t2.school_state_id = t3.state_id
    LEFT JOIN t4
        ON t2.school_district_id = t4.district_id
    LEFT JOIN t5
        ON t2.school_ward_id = t5.ward_id
    LEFT JOIN t6
        ON t2.school_taluka_id = t6.taluka_id
),

t8 AS (
    SELECT DISTINCT
        ward_id,
        TRIM(school_name) AS school_name,
        pmshree_school
    FROM {{ ref('seed_pmshree') }}
),

t9 as (SELECT
    t7.*,
    COALESCE(t8.pmshree_school, FALSE) AS pmshree_school
FROM t7
LEFT JOIN t8
    ON t7.school_ward_id = t8.ward_id
    AND TRIM(t7.school_name) = t8.school_name
)

select * from t9