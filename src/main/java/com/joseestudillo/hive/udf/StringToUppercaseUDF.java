package com.joseestudillo.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Simple UDF to get the upper cased version of a given string. This type of UDF only works with 'Writables'. For any type a GenericUDF must be implemented.
 * 
 * To declare it:
 * 
 * {@code
 * CREATE TEMPORARY FUNCTION to_uppercase AS 'com.joseestudillo.hive.udf.StringToUppercaseUDF';
 * }
 * 
 * To use it:
 * 
 * {@code
 * select to_uppercase(astring) from serde_table;
 * }
 * 
 * @author Jose Estudillo
 *
 */
public class StringToUppercaseUDF extends UDF {
	public Text evaluate(String text) {
		return text != null ? new Text(text.toUpperCase()) : null;
	}
}