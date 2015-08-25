package com.joseestudillo.hive.serde;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObject;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

/**
 * 
 * Hive Serde that generates random data based on the number of lines of the input file. The idea is to generate a table that contains all the exisitng type in
 * hive
 * 
 * @author Jose Estudillo
 *
 */
public class AllTypeSerDe extends AbstractSerDe {

	private static final Logger log = Logger.getLogger(AllTypeSerDe.class);

	private ObjectInspector objectInspector; // this will determine the types of the columns
	private List<String> columnNames;
	private List<TypeInfo> columnTypes;
	private List<Object> tmpRow;
	private int sequence = 0;

	@Override
	public void initialize(Configuration conf, Properties tableProperties) throws SerDeException {
		log.info("initialize");

		this.columnNames = SerDeUtils.getColumnNames(tableProperties);
		this.columnTypes = SerDeUtils.getColumnTypes(tableProperties);
		this.objectInspector = SerDeUtils.getObjectInspector(this.columnNames, this.columnTypes);

		// initialize the temporary row
		this.tmpRow = new ArrayList<Object>(this.columnNames.size());
		for (int i = 0; i < this.columnNames.size(); i++) {
			this.tmpRow.add(null);
		}
		log.info(String.format("Columns: %s", this.columnNames));
		log.info(String.format("Types: %s", this.columnTypes));
	}

	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		log.info("getObjectInspector");
		return this.objectInspector;
	}

	@Override
	public Object deserialize(Writable writable) throws SerDeException {
		Text rowText = (Text) writable;
		log.info(String.format("Deserializing: %s", rowText));
		// parse input
		// we will fake this step as we just want to fill the table up

		// fill the tmp Row where every value should be in the right java type
		this.setRowValues();

		// PrimitiveObjectInspectorUtils.
		log.info(String.format("Rows created: %s", this.tmpRow));
		return this.tmpRow;
	}

	private void setRowValues() {
		int currentSeq = this.sequence++;
		byte aTinyint = 1;
		short aSmallint = 1;
		int aInt = Integer.MAX_VALUE;
		long aBigint = Long.MAX_VALUE;
		float aFloat = Float.MAX_VALUE;
		double aDouble = Double.MAX_VALUE;
		BigDecimal aDecimal = new BigDecimal(currentSeq);
		java.sql.Timestamp aTimestamp = new java.sql.Timestamp(0);
		java.sql.Date aDate = new java.sql.Date(0);
		String aString = "String value " + currentSeq; // unbounded string
		String aVarchar = "Varchar value " + currentSeq; // limited variable length string up to 65k characters
		String aChar = "V"; // fixed length string up to 255 characters
		boolean aBoolean = true;
		byte[] aBinary = new byte[] { 0, 1 };
		String[] aStringArray = new String[] { "a", "b", "c" };
		Map<String, String> aStringMap = new HashMap<String, String>();
		aStringMap.put("key0", "value0");
		aStringMap.put("key1", "value1");
		Object[] aStruct = new Object[] { "value0", 1 };
		String aUnion = "union"; // this could be also an int

		this.tmpRow.set(0, aTinyint);
		this.tmpRow.set(1, aSmallint);
		this.tmpRow.set(2, aInt);
		this.tmpRow.set(3, aBigint);
		this.tmpRow.set(4, aFloat);
		this.tmpRow.set(5, aDouble);
		this.tmpRow.set(6, HiveDecimal.create(aDecimal));
		this.tmpRow.set(7, aTimestamp);
		this.tmpRow.set(8, aDate);
		this.tmpRow.set(9, aString);
		this.tmpRow.set(10, new HiveVarchar(aVarchar, 64)); // TODO find an smart way to get the length from the typeInfo
		this.tmpRow.set(11, new HiveChar(aChar, 1)); // TODO find an smart way to get the length from the typeInfo
		this.tmpRow.set(12, aBoolean);
		this.tmpRow.set(13, aBinary);
		this.tmpRow.set(14, aStringArray);
		this.tmpRow.set(15, aStringMap);
		this.tmpRow.set(16, aStruct);
		UnionObject unionObject = new StandardUnionObjectInspector.StandardUnion((byte) 0, aUnion);
		this.tmpRow.set(17, unionObject);

	}

	@Override
	public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
		log.info(String.format("Serializing(%s, %s)", obj, objInspector));
		return new Text(obj.toString()); // TODO implement properly
	}

	@Override
	public SerDeStats getSerDeStats() {
		return null;
	}

	@Override
	public Class<? extends Writable> getSerializedClass() {
		log.info("getSerializedClass()");
		return Text.class;
	}

}
