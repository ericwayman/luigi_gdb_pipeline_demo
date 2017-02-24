-------TEST FUNCTION------------
DROP FUNCTION IF EXISTS test();
CREATE FUNCTION test()
RETURNS text
AS
$BODY$
BEGIN
return 'success';
END;
$BODY$ LANGUAGE plpgsql VOLATILE;
---------------------------------
-- CREATE INPUT TABLE WITH hourly counts for each user
--user_column = user_name
DROP FUNCTION IF EXISTS create_hourly_counts_table(text,text,text);
CREATE OR REPLACE FUNCTION create_hourly_counts_table(input_table text,output_table text, user_column text)
RETURNS VOID
AS
$BODY$
BEGIN
EXECUTE 'DROP TABLE IF EXISTS '||output_table;
EXECUTE 'CREATE TABLE '||output_table||' AS
SELECT '||user_column||', 
    date_trunc(''day'',to_timestamp(start_time)) AS day,
    EXTRACT(HOUR FROM to_timestamp(start_time)) AS hour_of_day, 
    COUNT(*) AS hourly_count
FROM '||input_table||'
GROUP BY 1, 2, 3
DISTRIBUTED RANDOMLY';
END;
$BODY$ LANGUAGE plpgsql VOLATILE;

--generate a PCA input table for a given hour of the day.
--name_id is to assign the usernames a row number,
DROP FUNCTION IF EXISTS create_pca_input_table(text,text,text,int);
CREATE OR REPLACE FUNCTION create_pca_input_table(input_table text, output_table text,user_column text,hour_of_day int)
RETURNS void
AS
$BODY$
BEGIN
EXECUTE 'DROP TABLE IF EXISTS '||output_table;
EXECUTE 'CREATE TABLE '||output_table||' AS
(
WITH a AS 
(
    SELECT * FROM '||input_table||' where hour_of_day = '||hour_of_day||'
), 

b AS 
(
select day, day_id
FROM 
(
    select day, rank() over (ORDER BY day) AS day_id
    FROM a
    group by 1
) t1
group by 1, 2
), 

c AS 
(
    select '||user_column||', rank() over (ORDER BY '||user_column||') AS name_id
    FROM a
    group by 1
)

select  b.day_id, c.name_id, t1.*
FROM  a t1
INNER JOIN b ON t1.day = b.day
INNER JOIN c ON t1.'||user_column||' = c.'||user_column||'
)
DISTRIBUTED RANDOMLY';
END;
$BODY$ LANGUAGE plpgsql VOLATILE;

--find principal components
DROP FUNCTION IF EXISTS find_principal_components(
text,text,text,text,text,float8
);
CREATE OR REPLACE FUNCTION find_principal_components(
    input_table text,
    output_table text,
    time_id_col text,
    name_id_col text,
    val_col text,
    percentage_val float8)
   RETURNS void AS
$BODY$
DECLARE
    num_time_int integer;
    num_users integer;

BEGIN

EXECUTE 'select count(distinct('||time_id_col||')) FROM '||input_table into num_time_int;
EXECUTE 'select count(distinct('||name_id_col||')) FROM '||input_table into num_users;

EXECUTE 'DROP TABLE IF EXISTS '||output_table;
EXECUTE 'DROP TABLE IF EXISTS '||output_table||'_mean';
EXECUTE 'SELECT madlib.pca_sparse_train( '''||input_table||''',
                  '''||output_table||''',
                  '''||time_id_col||''',
                  '''||name_id_col||''',
                  '''||val_col||''',
                  '||num_time_int||',
                  '||num_users||',
                  '||percentage_val||'
                )';

END;
$BODY$
  LANGUAGE plpgsql VOLATILE STRICT;

DROP FUNCTION IF EXISTS extract_large_pca_components(text,text,text,text,float);
CREATE OR REPLACE FUNCTION extract_large_pca_components(
                                                        output_table text,
                                                        base_feature_table text,
                                                        pca_table text,
                                                        user_column text,
                                                        treshold float)
RETURNS VOID
AS
$BODY$
BEGIN
EXECUTE 'DROP TABLE IF EXISTS '||output_table;
EXECUTE 'CREATE TABLE '||output_table||' AS
SELECT
a.'||user_column||',
b.outlier_score
FROM
(
SELECT
'||user_column||',
name_id
FROM
'||base_feature_table||'
GROUP BY 1,2
) a
INNER JOIN
(
SELECT
featureno,
MAX(pcval) AS outlier_score
FROM
(
    SELECT
    featureno,
    pcval
    FROM
        (
        SELECT
        row_id,
        generate_series(1,array_upper(principal_components,1)) as featureno,
        unnest(principal_components) as pcval
        FROM '||pca_table||'
        ) t1
    where pcval > abs('||treshold||')
    )t2
GROUP BY 1
) b
ON (a.name_id=b.featureno)
DISTRIBUTED RANDOMLY';
END;
$BODY$ LANGUAGE plpgsql VOLATILE;
