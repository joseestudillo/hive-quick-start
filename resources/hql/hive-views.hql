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

DROP VIEW IF EXISTS view_1;
CREATE VIEW view_1(string_field_1, num_field_1)
  COMMENT 'Example view 1'
  AS
  SELECT aString, aDecimal
  FROM serde_table;

DROP VIEW IF EXISTS view_2;
CREATE VIEW view_2(string_field_2, num_field_2)
  COMMENT 'Example view 2'
  AS
  SELECT aVarchar, aDecimal
  FROM serde_table;

!echo "-- Views are specially useful to group several queries under one job";

!echo "-- QUERY: SELECT * FROM serde_table";
SELECT * FROM serde_table;

!echo "-- QUERY: SELECT * FROM view_1 v1, view_2 v2 WHERE v1.num_field_1 = v2.num_field_2";
SELECT * FROM view_1 v1, view_2 v2 WHERE v1.num_field_1 = v2.num_field_2;

!echo "-- SELECT view_1.string_field_1, view_2.string_field_2, view_2.num_field_2 FROM view_1 JOIN view_2 ON (view_1.num_field2 = view_2.num_field_2)";
SELECT view_1.string_field_1, view_2.string_field_2, view_2.num_field_2 FROM view_1 JOIN view_2 ON (view_1.num_field_1 = view_2.num_field_2);
