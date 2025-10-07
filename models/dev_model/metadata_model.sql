SELECT
  table_catalog,
  table_schema,
  table_name,
  column_name,
  data_type
FROM
  `antarang-dashboard.dalgo_DBT_Antarang_Foundation.INFORMATION_SCHEMA.COLUMNS`
  where table_name like 'dev_stg%'
ORDER BY
  table_schema,
  table_name