DROP TABLE partition_table;
CREATE EXTERNAL TABLE IF NOT EXISTS 
	partition_table(
		aString STRING, aVarchar VARCHAR(64), aChar CHAR(1)
	)
PARTITIONED BY (stringPartition string, bigIntPartition bigint)
ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ','
;
  
-- data loading  
LOAD DATA LOCAL INPATH '../data/data-string_type_table.txt' OVERWRITE INTO table partition_table PARTITION(stringPartition = 'string01', bigIntPartition = 123);
LOAD DATA LOCAL INPATH '../data/data-string_type_table.txt' OVERWRITE INTO table partition_table PARTITION(stringPartition = 'string02', bigIntPartition = 456);

