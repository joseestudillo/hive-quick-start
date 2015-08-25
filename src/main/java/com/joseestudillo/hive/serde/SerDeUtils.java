package com.joseestudillo.hive.serde;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * 
 * Utility class for Hive SerDes
 * 
 * @author Jose Estudillo
 *
 */
public class SerDeUtils {
	//TODO constants are taken from org.apache.hadoop.hive.serde.Constants which is deprecated, find the right way to get this information

	private static final String LIST_COLUMNS_SEPARATOR = ",";
	private static final String LIST_COLUMNS = "columns";
	private static final String LIST_COLUMN_TYPES = "columns.types";

	public static List<String> getColumnNames(Properties tableProperties) {
		return Arrays.asList(tableProperties.getProperty(LIST_COLUMNS).split(LIST_COLUMNS_SEPARATOR));
	}

	public static List<TypeInfo> getColumnTypes(Properties tableProperties) {
		return TypeInfoUtils.getTypeInfosFromTypeString(tableProperties.getProperty(LIST_COLUMN_TYPES));
	}

	public static ObjectInspector getObjectInspector(List<String> columnNames, List<TypeInfo> columnTypes) {
		StructTypeInfo rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
		return TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo);
	}

}
