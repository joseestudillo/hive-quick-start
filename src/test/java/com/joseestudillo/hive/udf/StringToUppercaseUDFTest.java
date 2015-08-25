package com.joseestudillo.hive.udf;

import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

/**
 * 
 * @author Jose Estudillo
 *
 */
public class StringToUppercaseUDFTest {
	@Test
	public void testEvaluate() {
		String text = "text";
		Assert.assertEquals(new StringToUppercaseUDF().evaluate(text), new Text(text.toUpperCase()));
	}

	@Test
	public void testEvaluateNull() {
		Assert.assertEquals(new StringToUppercaseUDF().evaluate(null), null);
	}
}
