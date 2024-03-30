with t as (
    
(with 

t0 as (select * from {{ ref("stg_student") }}),

t1 as (SELECT column_name, (COUNT(DISTINCT val)) AS distinct_count
FROM (
  SELECT TRIM(x[0], '"') AS column_name, TRIM(x[safe_offset(1)], '"') AS val
  FROM t0,
  UNNEST(SPLIT(TRIM(TO_JSON_STRING(t0), '{}'), ',"')) kv,
  UNNEST([STRUCT(SPLIT(kv, '":') AS x)]) 
)  
GROUP BY column_name),

t2 as (SELECT column_name, COUNT(1) AS null_count FROM t0, UNNEST(REGEXP_EXTRACT_ALL(TO_JSON_STRING(t0), r'"(\w+)":null')) column_name GROUP BY column_name),

t3 as (select 'a1. stg_student' as table_name, column_name, (select count (*) from t0) as total_records, 
(case when null_count is not null then null_count else 0 end) as null_count, distinct_count from t1 full outer join t2 using(column_name))

select * from t3)

union all

(with 

t0 as (select * from {{ ref("int_student") }}),

t1 as (SELECT column_name, (COUNT(DISTINCT val)) AS distinct_count
FROM (
  SELECT TRIM(x[0], '"') AS column_name, TRIM(x[safe_offset(1)], '"') AS val
  FROM t0,
  UNNEST(SPLIT(TRIM(TO_JSON_STRING(t0), '{}'), ',"')) kv,
  UNNEST([STRUCT(SPLIT(kv, '":') AS x)]) 
)  
GROUP BY column_name),

t2 as (SELECT column_name, COUNT(1) AS null_count FROM t0, UNNEST(REGEXP_EXTRACT_ALL(TO_JSON_STRING(t0), r'"(\w+)":null')) column_name GROUP BY column_name),

t3 as (select 'a2. int_student' as table_name, column_name, (select count (*) from t0) as total_records, 
(case when null_count is not null then null_count else 0 end) as null_count, distinct_count from t1 full outer join t2 using(column_name))

select * from t3)

union all

(with 

t0 as (select * from {{ ref("stg_batch") }}),

t1 as (SELECT column_name, (COUNT(DISTINCT val)) AS distinct_count
FROM (
  SELECT TRIM(x[0], '"') AS column_name, TRIM(x[safe_offset(1)], '"') AS val
  FROM t0,
  UNNEST(SPLIT(TRIM(TO_JSON_STRING(t0), '{}'), ',"')) kv,
  UNNEST([STRUCT(SPLIT(kv, '":') AS x)]) 
)  
GROUP BY column_name),

t2 as (SELECT column_name, COUNT(1) AS null_count FROM t0, UNNEST(REGEXP_EXTRACT_ALL(TO_JSON_STRING(t0), r'"(\w+)":null')) column_name GROUP BY column_name),

t3 as (select 'b1. stg_batch' as table_name, column_name, (select count (*) from t0) as total_records, 
(case when null_count is not null then null_count else 0 end) as null_count, distinct_count from t1 full outer join t2 using(column_name))

select * from t3)

union all

(with 

t0 as (select * from {{ ref("stg_school") }}),

t1 as (SELECT column_name, (COUNT(DISTINCT val)) AS distinct_count
FROM (
  SELECT TRIM(x[0], '"') AS column_name, TRIM(x[safe_offset(1)], '"') AS val
  FROM t0,
  UNNEST(SPLIT(TRIM(TO_JSON_STRING(t0), '{}'), ',"')) kv,
  UNNEST([STRUCT(SPLIT(kv, '":') AS x)]) 
)  
GROUP BY column_name),

t2 as (SELECT column_name, COUNT(1) AS null_count FROM t0, UNNEST(REGEXP_EXTRACT_ALL(TO_JSON_STRING(t0), r'"(\w+)":null')) column_name GROUP BY column_name),

t3 as (select 'c1. stg_school' as table_name, column_name, (select count (*) from t0) as total_records, 
(case when null_count is not null then null_count else 0 end) as null_count, distinct_count from t1 full outer join t2 using(column_name))

select * from t3)

union all

(with 

t0 as (select * from {{ ref("stg_facilitator") }}),

t1 as (SELECT column_name, (COUNT(DISTINCT val)) AS distinct_count
FROM (
  SELECT TRIM(x[0], '"') AS column_name, TRIM(x[safe_offset(1)], '"') AS val
  FROM t0,
  UNNEST(SPLIT(TRIM(TO_JSON_STRING(t0), '{}'), ',"')) kv,
  UNNEST([STRUCT(SPLIT(kv, '":') AS x)]) 
)  
GROUP BY column_name),

t2 as (SELECT column_name, COUNT(1) AS null_count FROM t0, UNNEST(REGEXP_EXTRACT_ALL(TO_JSON_STRING(t0), r'"(\w+)":null')) column_name GROUP BY column_name),

t3 as (select 'd1. stg_facilitator' as table_name, column_name, (select count (*) from t0) as total_records, 
(case when null_count is not null then null_count else 0 end) as null_count, distinct_count from t1 full outer join t2 using(column_name))

select * from t3)

union all

(with 

t0 as (select * from {{ ref("stg_donor") }}),

t1 as (SELECT column_name, (COUNT(DISTINCT val)) AS distinct_count
FROM (
  SELECT TRIM(x[0], '"') AS column_name, TRIM(x[safe_offset(1)], '"') AS val
  FROM t0,
  UNNEST(SPLIT(TRIM(TO_JSON_STRING(t0), '{}'), ',"')) kv,
  UNNEST([STRUCT(SPLIT(kv, '":') AS x)]) 
)  
GROUP BY column_name),

t2 as (SELECT column_name, COUNT(1) AS null_count FROM t0, UNNEST(REGEXP_EXTRACT_ALL(TO_JSON_STRING(t0), r'"(\w+)":null')) column_name GROUP BY column_name),

t3 as (select 'e1. stg_donor' as table_name, column_name, (select count (*) from t0) as total_records, 
(case when null_count is not null then null_count else 0 end) as null_count, distinct_count from t1 full outer join t2 using(column_name))

select * from t3)

union all

(with 

t0 as (select * from {{ ref("int_student_global") }}),

t1 as (SELECT column_name, (COUNT(DISTINCT val)) AS distinct_count
FROM (
  SELECT TRIM(x[0], '"') AS column_name, TRIM(x[safe_offset(1)], '"') AS val
  FROM t0,
  UNNEST(SPLIT(TRIM(TO_JSON_STRING(t0), '{}'), ',"')) kv,
  UNNEST([STRUCT(SPLIT(kv, '":') AS x)]) 
)  
GROUP BY column_name),

t2 as (SELECT column_name, COUNT(1) AS null_count FROM t0, UNNEST(REGEXP_EXTRACT_ALL(TO_JSON_STRING(t0), r'"(\w+)":null')) column_name GROUP BY column_name),

t3 as (select 'f1. int_student_global' as table_name, column_name, (select count (*) from t0) as total_records, 
(case when null_count is not null then null_count else 0 end) as null_count, distinct_count from t1 full outer join t2 using(column_name))

select * from t3)

union all

(with 

t0 as (select * from {{ ref("stg_session") }}),

t1 as (SELECT column_name, (COUNT(DISTINCT val)) AS distinct_count
FROM (
  SELECT TRIM(x[0], '"') AS column_name, TRIM(x[safe_offset(1)], '"') AS val
  FROM t0,
  UNNEST(SPLIT(TRIM(TO_JSON_STRING(t0), '{}'), ',"')) kv,
  UNNEST([STRUCT(SPLIT(kv, '":') AS x)]) 
)  
GROUP BY column_name),

t2 as (SELECT column_name, COUNT(1) AS null_count FROM t0, UNNEST(REGEXP_EXTRACT_ALL(TO_JSON_STRING(t0), r'"(\w+)":null')) column_name GROUP BY column_name),

t3 as (select 'g1. stg_session' as table_name, column_name, (select count (*) from t0) as total_records, 
(case when null_count is not null then null_count else 0 end) as null_count, distinct_count from t1 full outer join t2 using(column_name))

select * from t3)

union all

(with 

t0 as (select * from {{ ref("stg_somrt") }}),

t1 as (SELECT column_name, (COUNT(DISTINCT val)) AS distinct_count
FROM (
  SELECT TRIM(x[0], '"') AS column_name, TRIM(x[safe_offset(1)], '"') AS val
  FROM t0,
  UNNEST(SPLIT(TRIM(TO_JSON_STRING(t0), '{}'), ',"')) kv,
  UNNEST([STRUCT(SPLIT(kv, '":') AS x)]) 
)  
GROUP BY column_name),

t2 as (SELECT column_name, COUNT(1) AS null_count FROM t0, UNNEST(REGEXP_EXTRACT_ALL(TO_JSON_STRING(t0), r'"(\w+)":null')) column_name GROUP BY column_name),

t3 as (select 'h1. stg_somrt' as table_name, column_name, (select count (*) from t0) as total_records, 
(case when null_count is not null then null_count else 0 end) as null_count, distinct_count from t1 full outer join t2 using(column_name))

select * from t3)

union all

(with 

t0 as (select * from {{ ref("stg_attendance") }}),

t1 as (SELECT column_name, (COUNT(DISTINCT val)) AS distinct_count
FROM (
  SELECT TRIM(x[0], '"') AS column_name, TRIM(x[safe_offset(1)], '"') AS val
  FROM t0,
  UNNEST(SPLIT(TRIM(TO_JSON_STRING(t0), '{}'), ',"')) kv,
  UNNEST([STRUCT(SPLIT(kv, '":') AS x)]) 
)  
GROUP BY column_name),

t2 as (SELECT column_name, COUNT(1) AS null_count FROM t0, UNNEST(REGEXP_EXTRACT_ALL(TO_JSON_STRING(t0), r'"(\w+)":null')) column_name GROUP BY column_name),

t3 as (select 'i1. stg_attendance' as table_name, column_name, (select count (*) from t0) as total_records, 
(case when null_count is not null then null_count else 0 end) as null_count, distinct_count from t1 full outer join t2 using(column_name))

select * from t3)

union all

(with 

t0 as (select * from {{ ref("int_session") }}),

t1 as (SELECT column_name, (COUNT(DISTINCT val)) AS distinct_count
FROM (
  SELECT TRIM(x[0], '"') AS column_name, TRIM(x[safe_offset(1)], '"') AS val
  FROM t0,
  UNNEST(SPLIT(TRIM(TO_JSON_STRING(t0), '{}'), ',"')) kv,
  UNNEST([STRUCT(SPLIT(kv, '":') AS x)]) 
)  
GROUP BY column_name),

t2 as (SELECT column_name, COUNT(1) AS null_count FROM t0, UNNEST(REGEXP_EXTRACT_ALL(TO_JSON_STRING(t0), r'"(\w+)":null')) column_name GROUP BY column_name),

t3 as (select 'j1. int_session' as table_name, column_name, (select count (*) from t0) as total_records, 
(case when null_count is not null then null_count else 0 end) as null_count, distinct_count from t1 full outer join t2 using(column_name))

select * from t3)

union all

(with 

t0 as (select * from {{ ref("int_student_global_session") }}),

t1 as (SELECT column_name, (COUNT(DISTINCT val)) AS distinct_count
FROM (
  SELECT TRIM(x[0], '"') AS column_name, TRIM(x[safe_offset(1)], '"') AS val
  FROM t0,
  UNNEST(SPLIT(TRIM(TO_JSON_STRING(t0), '{}'), ',"')) kv,
  UNNEST([STRUCT(SPLIT(kv, '":') AS x)]) 
)  
GROUP BY column_name),

t2 as (SELECT column_name, COUNT(1) AS null_count FROM t0, UNNEST(REGEXP_EXTRACT_ALL(TO_JSON_STRING(t0), r'"(\w+)":null')) column_name GROUP BY column_name),

t3 as (select 'k1. int_student_global_session' as table_name, column_name, (select count (*) from t0) as total_records, 
(case when null_count is not null then null_count else 0 end) as null_count, distinct_count from t1 full outer join t2 using(column_name))

select * from t3)

union all

(with 

t0 as (select * from {{ ref("int_global_session") }}),

t1 as (SELECT column_name, (COUNT(DISTINCT val)) AS distinct_count
FROM (
  SELECT TRIM(x[0], '"') AS column_name, TRIM(x[safe_offset(1)], '"') AS val
  FROM t0,
  UNNEST(SPLIT(TRIM(TO_JSON_STRING(t0), '{}'), ',"')) kv,
  UNNEST([STRUCT(SPLIT(kv, '":') AS x)]) 
)  
GROUP BY column_name),

t2 as (SELECT column_name, COUNT(1) AS null_count FROM t0, UNNEST(REGEXP_EXTRACT_ALL(TO_JSON_STRING(t0), r'"(\w+)":null')) column_name GROUP BY column_name),

t3 as (select 'k2. int_global_session' as table_name, column_name, (select count (*) from t0) as total_records, 
(case when null_count is not null then null_count else 0 end) as null_count, distinct_count from t1 full outer join t2 using(column_name))

select * from t3)),

global_metadata as (select table_name, column_name, total_records, null_count, (total_records - null_count) as nonnull_count, distinct_count, (total_records - distinct_count) as duplicate_count, 

round(100*null_count/total_records, 1) as pct_null, round(100*(total_records - null_count)/total_records, 1) as pct_nonnull, 
round(100*distinct_count/total_records, 1) as pct_distinct, round(100*(total_records - distinct_count)/total_records, 1) as pct_duplicate from t order by column_name, table_name)

select * from global_metadata where column_name != ""


