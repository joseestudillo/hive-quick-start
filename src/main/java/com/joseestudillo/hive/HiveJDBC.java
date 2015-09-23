package com.joseestudillo.hive;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

/**
 * 
 * Example class to show how to use JDBC in hive.
 * 
 * By Default it connects to a new local derby instance.
 * 
 * Notice that in the file hive-site.xml is in the class path its configuration will be loaded.
 * 
 * @author Jose Estudillo
 * 
 */
public class HiveJDBC {

	public static final Logger log = Logger.getLogger(HiveJDBC.class);
	public static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";
	//this connection string creates the local derby instance if required. 
	//for this option the configuration will be loaded from hive-site.xml in the work space automatically
	public static final String HIVE_LOCAL = "jdbc:hive2:///";
	//this connection string required a running hiveserver2 server
	public static final String HIVE_LOCAL_HIVESERVER2 = "jdbc:hive2://localhost:10000";

	private static void logResultSet(ResultSet rs) throws SQLException {
		ResultSetMetaData rsmd = rs.getMetaData();
		int nCols = rsmd.getColumnCount();
		StringBuffer tmp;
		while (rs.next()) {
			tmp = new StringBuffer();
			tmp.append("{");
			int i = 1;
			tmp.append(String.format("%s:%s", rsmd.getColumnName(i), rs.getObject(i)));
			for (i = i + 1; i <= nCols; i++) {
				tmp.append(String.format(", %s:%s", rsmd.getColumnName(i), rs.getObject(i)));
			}
			tmp.append("}");
			log.info(tmp.toString());
		}
		rs.close();
	}

	private static void logTables(DatabaseMetaData metadata) throws SQLException {
		ResultSet rs = metadata.getTables(null, null, "%", null);
		while (rs.next()) {
			log.info("Table: " + rs.getString(3));
		}
	}

	public static void run(String... args) throws Exception {
		
		String principal = "hive/sandbox.hortonworks.com@HORTONWORKS.COM";
		String keytabPath = "/tmp/full-keytab.keytab";
		
		org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
	    conf.set("hadoop.security.authentication", "Kerberos");
	    UserGroupInformation.setConfiguration(conf);
	    UserGroupInformation.loginUserFromKeytab(principal, keytabPath);
	    
		/*
		conf.set("yarn.timeline-service.http-authentication.kerberos.keytab", "/etc/security/keytabs/spnego.service.keytab");
		conf.set("yarn.timeline-service.http-authentication.kerberos.principal", "HTTP/_HOST@HORTONWORKS.COM");
		conf.set("hadoop.http.authentication.kerberos.keytab", "${user.home}/hadoop.keytab");
		conf.set("nfs.kerberos.principal", "nfs/sandbox.hortonworks.com@HORTONWORKS.COM");
		conf.set("dfs.web.authentication.kerberos.keytab", "/etc/security/keytabs/spnego.service.keytab");
		conf.set("dfs.datanode.kerberos.principal", "dn/sandbox.hortonworks.com@HORTONWORKS.COM");
		conf.set("hadoop.kerberos.kinit.command", "kinit");
		conf.set("yarn.timeline-service.http-authentication.type", "kerberos");
		conf.set("dfs.namenode.kerberos.principal", "nn/sandbox.hortonworks.com@HORTONWORKS.COM");
		conf.set("hadoop.security.authentication", "kerberos");
		conf.set("dfs.secondary.namenode.kerberos.internal.spnego.principal", "HTTP/sandbox.hortonworks.com@HORTONWORKS.COM");
		conf.set("dfs.secondary.namenode.kerberos.principal", "nn/sandbox.hortonworks.com@HORTONWORKS.COM");
		conf.set("dfs.web.authentication.kerberos.principal", "HTTP/sandbox.hortonworks.com@HORTONWORKS.COM");
		conf.set("dfs.namenode.kerberos.internal.spnego.principal", "HTTP/sandbox.hortonworks.com@HORTONWORKS.COMc");
		conf.set("hadoop.http.authentication.kerberos.principal", "HTTP/_HOST@LOCALHOST");
		*/
		
		
		Class.forName(HIVE_DRIVER);

		String connectionString = (args.length == 0) ? HIVE_LOCAL_HIVESERVER2 : args[0];

		log.info(String.format("Using: %s", connectionString));

		Connection connection = DriverManager.getConnection(connectionString, "", "");
		String query;

		DatabaseMetaData metadata = connection.getMetaData();
		log.info(String.format("Connected to: %s %s", metadata.getDatabaseProductName(),
				metadata.getDatabaseProductVersion()));
		//showTables(metadata);

		Statement stmt = connection.createStatement();

		query = "CREATE TABLE IF NOT EXISTS test_table (s STRING, i INT)";
		log.info(String.format("-- %s", query));
		stmt.execute(query);

		query = "SELECT * FROM test_table";
		log.info(String.format("-- %s", query));
		ResultSet rSet = stmt.executeQuery(query);
		logResultSet(rSet);

		query = "SHOW DATABASES"; //"SHOW SCHEMAS" also works
		log.info(String.format("-- %s", query));
		rSet = stmt.executeQuery(query);
		logResultSet(rSet);

		query = "USE default";
		log.info(String.format("-- %s", query));
		stmt.execute(query);

		query = "SHOW TABLES";
		log.info(String.format("-- %s", query));
		rSet = stmt.executeQuery(query);
		logResultSet(rSet);

		connection.close();
		System.exit(0);
	}

	public static void main(String[] args) throws Exception {
		run((args.length == 0) ? HIVE_LOCAL : args[0]);
	}
}
