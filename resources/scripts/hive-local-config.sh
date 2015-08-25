export HIVE_OPTS='
 -hiveconf mapreduce.jobtracker.address=local
 -hiveconf fs.defaultFS=file:///tmp/hive/fs
 -hiveconf hive.metastore.uris=thrift://localhost:9083
 -hiveconf hive.metastore.warehouse.dir=file:///tmp/hive/warehouse
 -hiveconf javax.jdo.option.ConnectionDriverName=org.apache.derby.jdbc.ClientDriver
 -hiveconf javax.jdo.option.ConnectionURL=jdbc:derby://localhost:1527/tmp/hive/metastore_db;create=true'

# to create a new instance of derby:
# -hiveconf javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=/tmp/hive/metastore_db;create=true
# for a derby server running 
# -hiveconf javax.jdo.option.ConnectionURL=jdbc:derby://localhost:1527/tmp/hive/metastore_db;create=true

export HADOOP_USER_CLASSPATH_FIRST=true

