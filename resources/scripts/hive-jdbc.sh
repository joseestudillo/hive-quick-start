JAR=../../target/hive-0.0.1-SNAPSHOT.jar
CLASS=com.joseestudillo.hive.HiveJDBC
JDBC="jdbc:hive2://sandbox.hortonworks.com:10000/default"

CMD="java -Dlog4j.configuration=file://`pwd`/../../src/main/resources/log4j.xml -cp $JAR $CLASS $JDBC"
echo $CMD 
eval $CMD
