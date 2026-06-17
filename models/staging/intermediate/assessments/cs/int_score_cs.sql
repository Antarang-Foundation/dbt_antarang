WITH base AS (
  SELECT student_id, student_barcode, gender, batch_no, batch_academic_year, batch_language, facilitator_id, facilitator_name, 
  facilitator_email, school_id, school_name, school_taluka, school_ward, school_district, school_state, school_partner, school_area, 
  donor_id, batch_donor, batch_grade, assessment_barcode, bl_CreatedDate, bl_cs_no, bl_q4_1, bl_q4_2, bl_q4_3, bl_q4_4, bl_q4_5, 
  bl_q4_6, bl_q4_7, bl_q4_8, bl_q4_9, bl_q4_10, bl_q4_marks, bl_q5_1, bl_q5_2, bl_q5_3, bl_q5_4, bl_q5_5, bl_q5_6, bl_q5_7, bl_q5_8, 
  bl_q5_9, bl_q5_marks, el_CreatedDate, el_cs_no, el_q4_1, el_q4_2, el_q4_3, el_q4_4, el_q4_5, el_q4_6, el_q4_7, el_q4_8, el_q4_9, 
  el_q4_10, el_q4_marks, el_q5_1, el_q5_2, el_q5_3, el_q5_4, el_q5_5, el_q5_6, el_q5_7, el_q5_8, el_q5_9, el_q5_marks
  FROM {{ ref('int_student_global_cs') }}
),

bl AS (
  SELECT
    base.*,

    -- BL Q4 total
    (
      IFNULL(SAFE_CAST(bl_q4_1 AS INT64),0) +
      IFNULL(SAFE_CAST(bl_q4_2 AS INT64),0) +
      IFNULL(SAFE_CAST(bl_q4_3 AS INT64),0) +
      IFNULL(SAFE_CAST(bl_q4_4 AS INT64),0) +
      IFNULL(SAFE_CAST(bl_q4_5 AS INT64),0) +
      IFNULL(SAFE_CAST(bl_q4_6 AS INT64),0) +
      IFNULL(SAFE_CAST(bl_q4_7 AS INT64),0) +
      IFNULL(SAFE_CAST(bl_q4_8 AS INT64),0) +
      IFNULL(SAFE_CAST(bl_q4_9 AS INT64),0) +
      IFNULL(SAFE_CAST(bl_q4_10 AS INT64),0)
    ) AS bl_q4_total_marks,

    CASE
      WHEN (
        IFNULL(SAFE_CAST(bl_q4_1 AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q4_2 AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q4_3 AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q4_4 AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q4_5 AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q4_6 AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q4_7 AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q4_8 AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q4_9 AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q4_10 AS INT64),0)
      ) = 0 THEN '0 Experiential Opportunities'
      ELSE '1+ Experiential Opportunities'
    END AS bl_q4_bucket

  FROM base
),

el AS (
  SELECT
    bl.*,

    -- EL Q4 total
    (
      IFNULL(SAFE_CAST(el_q4_1 AS INT64),0) +
      IFNULL(SAFE_CAST(el_q4_2 AS INT64),0) +
      IFNULL(SAFE_CAST(el_q4_3 AS INT64),0) +
      IFNULL(SAFE_CAST(el_q4_4 AS INT64),0) +
      IFNULL(SAFE_CAST(el_q4_5 AS INT64),0) +
      IFNULL(SAFE_CAST(el_q4_6 AS INT64),0) +
      IFNULL(SAFE_CAST(el_q4_7 AS INT64),0) +
      IFNULL(SAFE_CAST(el_q4_8 AS INT64),0) +
      IFNULL(SAFE_CAST(el_q4_9 AS INT64),0) +
      IFNULL(SAFE_CAST(el_q4_10 AS INT64),0)
    ) AS el_q4_total_marks,

    CASE
      WHEN (
        IFNULL(SAFE_CAST(el_q4_1 AS INT64),0) +
        IFNULL(SAFE_CAST(el_q4_2 AS INT64),0) +
        IFNULL(SAFE_CAST(el_q4_3 AS INT64),0) +
        IFNULL(SAFE_CAST(el_q4_4 AS INT64),0) +
        IFNULL(SAFE_CAST(el_q4_5 AS INT64),0) +
        IFNULL(SAFE_CAST(el_q4_6 AS INT64),0) +
        IFNULL(SAFE_CAST(el_q4_7 AS INT64),0) +
        IFNULL(SAFE_CAST(el_q4_8 AS INT64),0) +
        IFNULL(SAFE_CAST(el_q4_9 AS INT64),0) +
        IFNULL(SAFE_CAST(el_q4_10 AS INT64),0)
      ) = 0 THEN '0 Experiential Opportunities'
      ELSE '1+ Experiential Opportunities'
    END AS el_q4_bucket

  FROM bl
)

SELECT *
FROM el