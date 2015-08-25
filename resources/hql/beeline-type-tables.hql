-- We use the default database/schema

-- numeric table

DROP TABLE 
	numeric_type_table;

CREATE EXTERNAL TABLE IF NOT EXISTS 
	numeric_type_table(
	 aTinyint TINYINT, aSmallint SMALLINT, aInt INT, aBigint BIGINT, aFloat FLOAT, aDouble DOUBLE, aDecimal DECIMAL
	)
ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ',';
	-- Row example: 1, 2, 3, 4, 5.0, 6.0, 7

-- date table

DROP TABLE 
	date_type_table;

CREATE EXTERNAL TABLE IF NOT EXISTS 
	date_type_table(
		aTimestamp TIMESTAMP, aDate DATE
	)
ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ','
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
;
	-- Row example: 2015-01-01 01:01:01.123, 2015-01-01 

-- string table

DROP TABLE 
	string_type_table;

CREATE EXTERNAL TABLE IF NOT EXISTS 
	string_type_table(
		aString STRING, aVarchar VARCHAR(64), aChar STRING
	)
ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ','
;


-- misc table

DROP TABLE 
	misc_type_table;

CREATE EXTERNAL TABLE IF NOT EXISTS 
	misc_type_table(
		aBoolean BOOLEAN, aBinary BINARY
	);
	-- true, 1


-- array table

DROP TABLE 
	array_type_table;

CREATE EXTERNAL TABLE IF NOT EXISTS 
	array_type_table(
		stringArray ARRAY<STRING>
	)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ':'
COLLECTION ITEMS TERMINATED BY ','
;


-- map table

DROP TABLE 
	map_type_table;

CREATE EXTERNAL TABLE IF NOT EXISTS 
	map_type_table(
		stringMap MAP<STRING, STRING>
	)
ROW FORMAT DELIMITED
   FIELDS TERMINATED BY ':'
   COLLECTION ITEMS TERMINATED BY ','
   MAP KEYS TERMINATED BY '|'
;


-- struct table. Struct: similar to array but with different types.

DROP TABLE 
	struct_type_table;

CREATE EXTERNAL TABLE IF NOT EXISTS 
	struct_type_table(
		aStruct STRUCT<stringColumn : STRING COMMENT "a column of type string", intColumn : INT COMMENT "a column of type int">
	)
ROW FORMAT DELIMITED
   FIELDS TERMINATED BY ':'
   COLLECTION ITEMS TERMINATED BY ','
;
	

-- union table. Union: allows to store several types in the same field.

DROP TABLE 
	union_type_table;

CREATE EXTERNAL TABLE IF NOT EXISTS 
	union_type_table(
		aUnion UNIONTYPE<STRING, INT>
	)
;
	
	
--
-- Load phase
--

LOAD DATA LOCAL INPATH '../data/data-numeric_type_table.txt' OVERWRITE INTO TABLE numeric_type_table;
LOAD DATA LOCAL INPATH '../data/data-date_type_table.txt' OVERWRITE INTO TABLE date_type_table;
LOAD DATA LOCAL INPATH '../data/data-string_type_table.txt' OVERWRITE INTO TABLE string_type_table;
LOAD DATA LOCAL INPATH '../data/data-misc_type_table.txt' OVERWRITE INTO TABLE misc_type_table;
LOAD DATA LOCAL INPATH '../data/data-array_type_table.txt' OVERWRITE INTO TABLE array_type_table;
LOAD DATA LOCAL INPATH '../data/data-map_type_table.txt' OVERWRITE INTO TABLE map_type_table;
LOAD DATA LOCAL INPATH '../data/data-struct_type_table.txt' OVERWRITE INTO TABLE struct_type_table;
-- LOAD DATA LOCAL INPATH '../data/data-union_type_table.txt' OVERWRITE INTO TABLE union_type_table; -- It doesnt work properly, serde?
