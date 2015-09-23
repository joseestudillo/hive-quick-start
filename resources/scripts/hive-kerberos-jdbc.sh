JAR=../../target/hive-0.0.1-SNAPSHOT.jar
CLASS=com.joseestudillo.hive.HiveJDBC
JDBC="jdbc:hive2://sandbox.hortonworks.com:10000/default\;principal=hive/sandbox.hortonworks.com@HORTONWORKS.COM"
PRINCIPAL="hive/sandbox.hortonworks.com@HORTONWORKS.COM"
KEYTAB_FILE="/tmp/full-keytab.keytab"

CMD="java -Dlog4j.configuration=file://`pwd`/../../src/main/resources/log4j.xml -cp $JAR $CLASS $JDBC $PRINCIPAL $KEYTAB_FILE"
echo $CMD 
eval $CMD
