package com.joseestudillo.hive;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

/**
 * 
 * Example class to show how to use JDBC in hive
 * 
 * @author Jose Estudillo
 * 
 */
public class HiveJDBC {

	public static final Logger log = Logger.getLogger(HiveJDBC.class);
	public static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";
	//this connection string creates the local derby instance if required
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

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
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
}
