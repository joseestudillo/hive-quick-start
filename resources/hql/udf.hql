-- old way (temporary)

ADD JAR ../../target/hive-0.0.1-SNAPSHOT.jar;
DROP TEMPORARY FUNCTION IF EXISTS to_uppercase;
CREATE TEMPORARY FUNCTION to_uppercase AS 'com.joseestudillo.hive.udf.StringToUppercaseUDF';

DROP TEMPORARY FUNCTION IF EXISTS to_keys_array;
CREATE TEMPORARY FUNCTION to_keys_array AS 'com.joseestudillo.hive.udf.MapKeysGenericUDF';


!echo "to_uppercase use example";
select to_uppercase(astring) from serde_table;

!echo "to_keys_array use example";
select to_keys_array(astringmap) from serde_table;


-- new way (relative to the database, must be added in from of the function name, database.function_name)

-- DROP FUNCTION IF EXISTS to_uppercase;
-- CREATE FUNCTION to_uppercase AS 'com.joseestudillo.hive.udf.StringToUppercaseUDF' USING '../../target/hive-0.0.1-SNAPSHOT.jar';
-- DROP FUNCTION IF EXISTS to_keys_array;
-- CREATE FUNCTION to_keys_array AS 'com.joseestudillo.hive.udf.MapKeysUDF' USING '../../target/hive-0.0.1-SNAPSHOT.jar';


-- Using java defined function in queries

SELECT reflect("java.lang.String", "valueOf", 1) FROM serde_table LIMIT 1; --to show results in select like this, the table doesn't matter but it must exists
