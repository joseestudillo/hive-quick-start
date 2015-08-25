package com.joseestudillo.hive.udf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Assert;
import org.junit.Test;

/**
 * 
 * @author Jose Estudillo
 *
 */
public class MapKeysGenericUDFTest {
	@Test
	public void testEvaluation() throws HiveException, IOException {
		// initialize UDF
		MapKeysGenericUDF udf = new MapKeysGenericUDF();
		ObjectInspector inputOI = ObjectInspectorFactory.getStandardMapObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector,
				PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		StandardListObjectInspector outputInspector = (StandardListObjectInspector) udf.initialize(new ObjectInspector[] { inputOI });
		System.out.println(outputInspector.getClass());

		// generate input
		Map<String, String> inputMap = new HashMap<String, String>();
		inputMap.put("a", "1");
		inputMap.put("b", "2");

		DeferredObject[] toEvaluate = toDeferredObjects(inputMap);
		DeferredObject[] toEvaluateEmpty = toDeferredObjects(new HashMap<String, String>());
		DeferredObject[] toEvaluateNull = toDeferredObjects(null);

		// evaluations
		Object result = udf.evaluate(toEvaluate);
		Assert.assertEquals(true, outputInspector.getList(result).containsAll(inputMap.keySet()));

		result = udf.evaluate(toEvaluateEmpty);
		Assert.assertEquals(true, outputInspector.getList(result).isEmpty());

		// arguments are null
		result = udf.evaluate(toEvaluateNull);
		Assert.assertNull(result);

		udf.close();
	}

	private static DeferredObject[] toDeferredObjects(Object... objects) {
		if (objects == null) {
			return new DeferredObject[] { new DeferredJavaObject(null) };
		}
		DeferredObject[] result = new DeferredObject[objects.length];
		for (int i = 0; i < result.length; i++) {
			result[i] = new DeferredJavaObject(objects[i]);
		}
		return result;
	}

}
