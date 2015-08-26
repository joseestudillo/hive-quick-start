package com.joseestudillo.hive;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

/**
 * 
 * Example of connections the the Hive's Metastore from Java.
 * 
 * By Default it connects to a new local derby instance.
 * 
 * @author Jose Estudillo
 *
 */
public class HiveMetastore {

	private static final Logger log = Logger.getLogger(HiveMetastore.class);

	public static final String HIVE_LOCAL_SHARED_DERBY_CONF = "/hive-site-shared-derby.xml";
	public static final String HIVE_LOCAL_CONF = "/hive-site-local.xml";

	/**
	 * It creates a hive <code>Database</code> instance. The idea is to show how to create a database programmatically in the same way Hive console does.
	 * 
	 * @param dbName
	 * @param description
	 * @param config
	 * @return a new instance of <code>Database</code> if it doesn't exist, or the instance of the existing one.
	 * @throws TException
	 * @throws MetaException
	 * @throws InvalidObjectException
	 * @throws AlreadyExistsException
	 */
	public static Database createDatabase(HiveMetaStoreClient client, String dbName, String description, HiveConf config) throws AlreadyExistsException,
			InvalidObjectException, MetaException, TException {
		Database db;
		if (client.getAllDatabases().contains(dbName)) {
			db = client.getDatabase(dbName);
		} else {
			String dbUri = String.format("%s/%s.db", config.get("hive.metastore.warehouse.dir"), dbName);
			db = new Database(dbName, description, dbUri, new HashMap<String, String>());
			client.createDatabase(db);
		}
		return db;
	}

	/**
	 * Creates a Hive <code>Table</code> instance. The idea is to show how to create a table programmatically in the same way Hive console does.
	 * 
	 * @param client
	 * @param tableName
	 * @param database
	 * @return a new instance of <code>Table</code> if it doesn't exist, or the instance of the existing one.
	 * @throws AlreadyExistsException
	 * @throws InvalidObjectException
	 * @throws MetaException
	 * @throws NoSuchObjectException
	 * @throws TException
	 */
	public static Table createTable(HiveMetaStoreClient client, String tableName, Database database) throws AlreadyExistsException, InvalidObjectException,
			MetaException, NoSuchObjectException, TException {
		Table tbl;
		if (client.tableExists(database.getName(), tableName)) {
			tbl = client.getTable(database.getName(), tableName);
		} else {
			List<FieldSchema> cols = Arrays.asList(
					new FieldSchema("aString", "string", "a comment about the string field"),
					new FieldSchema("aInt", "int", "a comment about the int field"));
			String location = String.format("%s/%s", database.getLocationUri(), tableName);
			String inputFormat = org.apache.hadoop.mapred.TextInputFormat.class.getCanonicalName();
			String outputFormat = org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat.class.getCanonicalName();
			boolean compressed = false;
			int numBuckets = -1;
			Map<String, String> serderParameters = new HashMap<String, String>();
			serderParameters.put("field.delim", ",");
			serderParameters.put("serialization.format", ",");
			SerDeInfo serdeInfo = new SerDeInfo(null, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getCanonicalName(), serderParameters);
			List<String> bucketCols = Collections.emptyList();
			List<Order> sortCols = Collections.emptyList();
			Map<String, String> storageDescriptorParameters = new HashMap<String, String>();
			StorageDescriptor sd = new StorageDescriptor(cols, location, inputFormat, outputFormat, compressed, numBuckets, serdeInfo, bucketCols, sortCols,
					storageDescriptorParameters);

			String tblName = tableName;
			String dbName = database.getName();
			String owner = System.getProperty("user.name");
			int createTime = (int) System.currentTimeMillis();
			int lastAccessTime = 0;
			int retention = 0;

			List<FieldSchema> partitionKeys = Collections.emptyList();
			Map<String, String> parameters = new HashMap<String, String>();
			String viewOriginalText = null;
			String viewExpandedText = null;
			String tableType = "MANAGED_TABLE";
			tbl = new Table(tblName, dbName, owner, createTime, lastAccessTime, retention, sd, partitionKeys, parameters, viewOriginalText,
					viewExpandedText, tableType);
			client.createTable(tbl);
		}
		return tbl;
	}

	/**
	 * Logs the information about the given hive database.
	 * 
	 * @param client
	 * @param log
	 * @throws NoSuchObjectException
	 * @throws TException
	 */
	public static void logHiveDatabase(HiveMetaStoreClient client, Logger log) throws NoSuchObjectException, TException {
		List<String> databaseNames = client.getDatabases("*"); //client.getAllDatabases()
		log.info(String.format("%d databases found", databaseNames.size()));
		for (String databaseName : databaseNames) {
			Database tmpDatabase = client.getDatabase(databaseName);
			List<String> tableNames = client.getTables(databaseName, "*");
			log.info(String.format("%s has %d tables", tmpDatabase, tableNames.size()));
			for (String tableName : tableNames) {
				Table tmpTable = client.getTable(databaseName, tableName);
				log.info(String.format("\t%s", tmpTable));
			}
		}
	}

	public static void run(String... args) throws Exception {

		InputStream configInputStream;
		if (args.length > 0) {
			configInputStream = new FileInputStream(new File(args[0]));
		} else {
			configInputStream = HiveMetastore.class.getResourceAsStream(HIVE_LOCAL_CONF);
		}

		HiveConf hiveConf = new HiveConf();
		hiveConf.addResource(configInputStream);

		Driver driver = new Driver(hiveConf);
		HiveMetaStoreClient client = new HiveMetaStoreClient(hiveConf);
		SessionState.start(new SessionState(hiveConf));

		log.info("Hive database information:");
		logHiveDatabase(client, log);

		String dbName = "java_database";
		String tblName = "java_table";
		Database database = createDatabase(client, dbName, "database create from java", hiveConf);
		log.info(String.format("Database created: %s", database));
		Table table = createTable(client, tblName, database);
		log.info(String.format("Table created: %s", table));

		log.info("Hive database information:");
		logHiveDatabase(client, log);

		String query;
		CommandProcessorResponse response;

		//TODO add code to show query results
		query = "show databases";
		log.info(String.format("-- %s", query));
		response = driver.run(query);
		log.info(String.format("Query Respose:", response));

		query = "select * from java_database.java_table";
		log.info(String.format("-- %s", query));
		response = driver.run(query);
		log.info(String.format("Query Respose:", response));
	}

	public static void main(String[] args) throws Exception {
		run((args.length == 0) ? HIVE_LOCAL_CONF : args[0]);
	}
}
