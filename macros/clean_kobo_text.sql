{% macro clean_kobo_text(column_name) %}

TRIM(
  REGEXP_REPLACE(
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(

            SAFE_CAST({{ column_name }} AS STRING),

            -- 1️⃣ Kobo multi-select separator → comma
            r'__+', ', '
          ),

          -- 2️⃣ Remaining underscores → space
          r'_', ' '
        ),

        -- 3️⃣ Fix common apostrophe issue (don t → don't)
        r"\b([A-Za-z]+)\s+t\b", r"\1't"
      ),

      -- 4️⃣ Normalize comma spacing
      r'\s*,\s*', ', '
    ),

    -- 5️⃣ Remove trailing comma
    r',\s*$', ''
  )
)

{% endmacro %}