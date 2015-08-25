package com.joseestudillo.hive.udf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.log4j.Logger;

/**
 * UDF to convert a map into an String. This type of UDF also works with non-'Writables'.
 * 
 * To declare it:
 * 
 * {@code
 * CREATE TEMPORARY FUNCTION to_keys_array AS 'com.joseestudillo.hive.udf.MapKeysGenericUDF';
 * }
 * 
 * To use it:
 * 
 * {@code
 * select to_keys_array(astringmap) from serde_table;
 * }
 * 
 * @author Jose Estudillo
 *
 */
public class MapKeysGenericUDF extends GenericUDF {

	public static final Logger log = Logger.getLogger(MapKeysGenericUDF.class);
	public static final String STR_ARRAY_TYPE = "array<string>"; // TODO find out how to get the object inspector from String[].class

	MapObjectInspector mapOI;

	@Override
	public String getDisplayString(String[] arg0) {
		return String.format("%s: %s -> %s", this.getClass().getName(), Map.class.getName(), String[].class.getName());
	}

	@Override
	/**
	 * Returns the Object inspector for the return type of this UDF and validates the object inspector for each of the input
	 */
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		// right number of args
		if (arguments.length != 1) {
			throw new UDFArgumentLengthException("1 argument required");
		}

		// right args type
		ObjectInspector inputObjectInspector = arguments[0];

		if (inputObjectInspector == null) {
			return null;
		}

		if (!(inputObjectInspector instanceof MapObjectInspector)) {
			throw new UDFArgumentException("The argument type must be Map<String,String>");
		}

		this.mapOI = (MapObjectInspector) inputObjectInspector;

		// ObjectInspector objectInspector =
		// TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(TypeInfoFactory.getPrimitiveTypeInfoFromJavaPrimitive(String[].class));
		// //doesnt work
		ObjectInspector objectInspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(TypeInfoUtils.getTypeInfoFromTypeString(STR_ARRAY_TYPE));
		return objectInspector;
		// for standard object inspectors use: PrimitiveObjectInspectorFactory.x;
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		@SuppressWarnings("unchecked")
		Map<String, String> map = (Map<String, String>) this.mapOI.getMap(arguments[0].get());
		if (map != null) {
			List<String> list = new ArrayList<String>();
			list.addAll(map.keySet());
			String[] result = list.toArray(new String[list.size()]);
			log.info(String.format("%s -> %s", map, Arrays.toString(result)));
			return result;
		} else {
			return null;
		}
	}

}
