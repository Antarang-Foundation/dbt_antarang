WITH base AS (
  SELECT *
  FROM {{ ref('int_score_cs') }}
),

/* =========================
   BL CALCULATIONS
========================= */
bl AS (
  SELECT
    base.*,

    /* -------------------------
       BL Q5 TOTAL + BUCKET + SCORE
    ------------------------- */
    (
      IFNULL(SAFE_CAST(bl_q5_1 AS INT64),0) +
      IFNULL(SAFE_CAST(bl_q5_2 AS INT64),0) +
      IFNULL(SAFE_CAST(bl_q5_3 AS INT64),0) +
      IFNULL(SAFE_CAST(bl_q5_4 AS INT64),0) +
      IFNULL(SAFE_CAST(bl_q5_5 AS INT64),0) +
      IFNULL(SAFE_CAST(bl_q5_6 AS INT64),0) +
      IFNULL(SAFE_CAST(bl_q5_7 AS INT64),0) +
      IFNULL(SAFE_CAST(bl_q5_8 AS INT64),0) +
      IFNULL(SAFE_CAST(bl_q5_9 AS INT64),0)
    ) AS bl_q5_total_marks,

    SAFE_DIVIDE(
      (
        IFNULL(SAFE_CAST(bl_q5_1 AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q5_2 AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q5_3 AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q5_4 AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q5_5 AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q5_6 AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q5_7 AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q5_8 AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q5_9 AS INT64),0)
      ), 9
    ) AS bl_q5_overall_score,

    CASE
      WHEN (
        IFNULL(SAFE_CAST(bl_q5_1 AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q5_2 AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q5_3 AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q5_4 AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q5_5 AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q5_6 AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q5_7 AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q5_8 AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q5_9 AS INT64),0)
      ) = 0 THEN '0 CRS'
      WHEN (
        IFNULL(SAFE_CAST(bl_q5_1 AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q5_2 AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q5_3 AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q5_4 AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q5_5 AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q5_6 AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q5_7 AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q5_8 AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q5_9 AS INT64),0)
      ) BETWEEN 1 AND 4 THEN '1-4 CRS'
      ELSE '5-9 CRS'
    END AS bl_q5_bucket

  FROM base
),

/* =========================
   EL CALCULATIONS
========================= */
el AS (
  SELECT
    bl.*,

    /* -------------------------
       EL Q5 TOTAL + BUCKET + SCORE
    ------------------------- */
    (
      IFNULL(SAFE_CAST(el_q5_1 AS INT64),0) +
      IFNULL(SAFE_CAST(el_q5_2 AS INT64),0) +
      IFNULL(SAFE_CAST(el_q5_3 AS INT64),0) +
      IFNULL(SAFE_CAST(el_q5_4 AS INT64),0) +
      IFNULL(SAFE_CAST(el_q5_5 AS INT64),0) +
      IFNULL(SAFE_CAST(el_q5_6 AS INT64),0) +
      IFNULL(SAFE_CAST(el_q5_7 AS INT64),0) +
      IFNULL(SAFE_CAST(el_q5_8 AS INT64),0) +
      IFNULL(SAFE_CAST(el_q5_9 AS INT64),0)
    ) AS el_q5_total_marks,

    SAFE_DIVIDE(
      (
        IFNULL(SAFE_CAST(el_q5_1 AS INT64),0) +
        IFNULL(SAFE_CAST(el_q5_2 AS INT64),0) +
        IFNULL(SAFE_CAST(el_q5_3 AS INT64),0) +
        IFNULL(SAFE_CAST(el_q5_4 AS INT64),0) +
        IFNULL(SAFE_CAST(el_q5_5 AS INT64),0) +
        IFNULL(SAFE_CAST(el_q5_6 AS INT64),0) +
        IFNULL(SAFE_CAST(el_q5_7 AS INT64),0) +
        IFNULL(SAFE_CAST(el_q5_8 AS INT64),0) +
        IFNULL(SAFE_CAST(el_q5_9 AS INT64),0)
      ), 9
    ) AS el_q5_overall_score,

    CASE
      WHEN (
        IFNULL(SAFE_CAST(el_q5_1 AS INT64),0) +
        IFNULL(SAFE_CAST(el_q5_2 AS INT64),0) +
        IFNULL(SAFE_CAST(el_q5_3 AS INT64),0) +
        IFNULL(SAFE_CAST(el_q5_4 AS INT64),0) +
        IFNULL(SAFE_CAST(el_q5_5 AS INT64),0) +
        IFNULL(SAFE_CAST(el_q5_6 AS INT64),0) +
        IFNULL(SAFE_CAST(el_q5_7 AS INT64),0) +
        IFNULL(SAFE_CAST(el_q5_8 AS INT64),0) +
        IFNULL(SAFE_CAST(el_q5_9 AS INT64),0)
      ) = 0 THEN '0 CRS'
      WHEN (
        IFNULL(SAFE_CAST(el_q5_1 AS INT64),0) +
        IFNULL(SAFE_CAST(el_q5_2 AS INT64),0) +
        IFNULL(SAFE_CAST(el_q5_3 AS INT64),0) +
        IFNULL(SAFE_CAST(el_q5_4 AS INT64),0) +
        IFNULL(SAFE_CAST(el_q5_5 AS INT64),0) +
        IFNULL(SAFE_CAST(el_q5_6 AS INT64),0) +
        IFNULL(SAFE_CAST(el_q5_7 AS INT64),0) +
        IFNULL(SAFE_CAST(el_q5_8 AS INT64),0) +
        IFNULL(SAFE_CAST(el_q5_9 AS INT64),0)
      ) BETWEEN 1 AND 4 THEN '1-4 CRS'
      ELSE '5-9 CRS'
    END AS el_q5_bucket

  FROM bl
)

/* =========================
   FINAL OUTPUT
========================= */
SELECT
  el.*,

  /* =========================
     BL vs EL DELTA
  ========================= */
  (el_q4_total_marks - bl_q4_total_marks) AS bl_el_q4_score,
  (el_q5_total_marks - bl_q5_total_marks) AS bl_el_q5_score,

  /* =========================
     FINAL CLASSIFICATION (REQUIRED)
  ========================= */

  CASE
    WHEN (el_q5_overall_score - bl_q5_overall_score) > 0 THEN 'Improvement'
    WHEN (el_q5_overall_score - bl_q5_overall_score) < 0 THEN 'Area for Growth'
    ELSE 'No Change'
  END AS bl_el_q5_status,

  CASE
    WHEN (el_q4_total_marks - bl_q4_total_marks) > 0 THEN 'Improvement'
    WHEN (el_q4_total_marks - bl_q4_total_marks) < 0 THEN 'Area for Growth'
    ELSE 'No Change'
  END AS bl_el_q4_status

FROM el