ADD JAR ../../target/hive-0.0.1-SNAPSHOT.jar;

DROP TABLE serde_table;
CREATE TABLE IF NOT EXISTS serde_table(
	aTinyint TINYINT, aSmallint SMALLINT, aInt INT, aBigint BIGINT, aFloat FLOAT, aDouble DOUBLE, aDecimal DECIMAL,
	aTimestamp TIMESTAMP, aDate DATE,
	aString STRING, aVarchar VARCHAR(64), aChar CHAR(1),
	aBoolean BOOLEAN, aBinary BINARY,
	aStringArray ARRAY<STRING>,
	aStringMap MAP<STRING, STRING>,
	aStruct STRUCT<stringColumn : STRING COMMENT "a column of type string", intColumn : INT COMMENT "a column of type int">,
	aUnion UNIONTYPE<STRING, INT>  
)
ROW FORMAT SERDE 
  'com.joseestudillo.hive.serde.AllTypeSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
  
-- data loading  (It's fake data as the serde will generate the data randomly)
LOAD DATA LOCAL INPATH '../data/data-serde_table.txt' OVERWRITE INTO TABLE serde_table;

-- example queries accessing complex types
SELECT * from serde_table;
SELECT astringarray[0] FROM serde_table;
SELECT astringmap['key0'] FROM serde_table;
SELECT astruct.stringColumn FROM serde_table;

