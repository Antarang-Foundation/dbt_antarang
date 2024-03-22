with t as (
    
    (with 

t0 as (select * from {{ ref("stg_cp") }}),

t1 as (SELECT column_name, (COUNT(DISTINCT val)) AS distinct_count
FROM (
  SELECT TRIM(x[0], '"') AS column_name, TRIM(x[safe_offset(1)], '"') AS val
  FROM t0,
  UNNEST(SPLIT(TRIM(TO_JSON_STRING(t0), '{}'), ',"')) kv,
  UNNEST([STRUCT(SPLIT(kv, '":') AS x)]) 
)  
GROUP BY column_name),

t2 as (SELECT column_name, COUNT(1) AS null_count FROM t0, UNNEST(REGEXP_EXTRACT_ALL(TO_JSON_STRING(t0), r'"(\w+)":null')) column_name GROUP BY column_name),

t3 as (select 'l1. stg_cp' as table_name, column_name, (select count (*) from t0) as total_records, 
(case when null_count is not null then null_count else 0 end) as null_count, distinct_count from t1 full outer join t2 using(column_name))

select * from t3)

union all

(with 

t0 as (select * from {{ ref("int_cp_latest") }}),

t1 as (SELECT column_name, (COUNT(DISTINCT val)) AS distinct_count
FROM (
  SELECT TRIM(x[0], '"') AS column_name, TRIM(x[safe_offset(1)], '"') AS val
  FROM t0,
  UNNEST(SPLIT(TRIM(TO_JSON_STRING(t0), '{}'), ',"')) kv,
  UNNEST([STRUCT(SPLIT(kv, '":') AS x)]) 
)  
GROUP BY column_name),

t2 as (SELECT column_name, COUNT(1) AS null_count FROM t0, UNNEST(REGEXP_EXTRACT_ALL(TO_JSON_STRING(t0), r'"(\w+)":null')) column_name GROUP BY column_name),

t3 as (select 'l2_1. int_cp_latest' as table_name, column_name, (select count (*) from t0) as total_records, 
(case when null_count is not null then null_count else 0 end) as null_count, distinct_count from t1 full outer join t2 using(column_name))

select * from t3)

union all

(with 

t0 as (select * from {{ ref("int_student_global_cp_latest") }}),

t1 as (SELECT column_name, (COUNT(DISTINCT val)) AS distinct_count
FROM (
  SELECT TRIM(x[0], '"') AS column_name, TRIM(x[safe_offset(1)], '"') AS val
  FROM t0,
  UNNEST(SPLIT(TRIM(TO_JSON_STRING(t0), '{}'), ',"')) kv,
  UNNEST([STRUCT(SPLIT(kv, '":') AS x)]) 
)  
GROUP BY column_name),

t2 as (SELECT column_name, COUNT(1) AS null_count FROM t0, UNNEST(REGEXP_EXTRACT_ALL(TO_JSON_STRING(t0), r'"(\w+)":null')) column_name GROUP BY column_name),

t3 as (select 'l2_2. int_student_global_cp_latest' as table_name, column_name, (select count (*) from t0) as total_records, 
(case when null_count is not null then null_count else 0 end) as null_count, distinct_count from t1 full outer join t2 using(column_name))

select * from t3)

union all

(with 

t0 as (select * from {{ ref("int_cp_pivot") }}),

t1 as (SELECT column_name, (COUNT(DISTINCT val)) AS distinct_count
FROM (
  SELECT TRIM(x[0], '"') AS column_name, TRIM(x[safe_offset(1)], '"') AS val
  FROM t0,
  UNNEST(SPLIT(TRIM(TO_JSON_STRING(t0), '{}'), ',"')) kv,
  UNNEST([STRUCT(SPLIT(kv, '":') AS x)]) 
)  
GROUP BY column_name),

t2 as (SELECT column_name, COUNT(1) AS null_count FROM t0, UNNEST(REGEXP_EXTRACT_ALL(TO_JSON_STRING(t0), r'"(\w+)":null')) column_name GROUP BY column_name),

t3 as (select 'l3_1. int_cp_pivot' as table_name, column_name, (select count (*) from t0) as total_records, 
(case when null_count is not null then null_count else 0 end) as null_count, distinct_count from t1 full outer join t2 using(column_name))

select * from t3)

union all

(with 

t0 as (select * from {{ ref("int_student_global_cp_pivot") }}),

t1 as (SELECT column_name, (COUNT(DISTINCT val)) AS distinct_count
FROM (
  SELECT TRIM(x[0], '"') AS column_name, TRIM(x[safe_offset(1)], '"') AS val
  FROM t0,
  UNNEST(SPLIT(TRIM(TO_JSON_STRING(t0), '{}'), ',"')) kv,
  UNNEST([STRUCT(SPLIT(kv, '":') AS x)]) 
)  
GROUP BY column_name),

t2 as (SELECT column_name, COUNT(1) AS null_count FROM t0, UNNEST(REGEXP_EXTRACT_ALL(TO_JSON_STRING(t0), r'"(\w+)":null')) column_name GROUP BY column_name),

t3 as (select 'l3_2. int_student_global_cp_pivot' as table_name, column_name, (select count (*) from t0) as total_records, 
(case when null_count is not null then null_count else 0 end) as null_count, distinct_count from t1 full outer join t2 using(column_name))

select * from t3)),

cp_metadata as (select table_name, column_name, total_records, null_count, (total_records - null_count) as nonnull_count, distinct_count, (total_records - distinct_count) as duplicate_count, 

round(100*null_count/total_records, 1) as pct_null, round(100*(total_records - null_count)/total_records, 1) as pct_nonnull, 
round(100*distinct_count/total_records, 1) as pct_distinct, round(100*(total_records - distinct_count)/total_records, 1) as pct_duplicate from t order by column_name, table_name)

select * from cp_metadata where column_name != ""

order by column_name, table_name