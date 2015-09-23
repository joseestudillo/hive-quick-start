# --hiveconf log4j.rootLogger=INFO,console is not working for me, I had to go the $HIVE_HOME/conf folder and set beeline-log4j.properties there

PRINCIPAL=hive/sandbox.hortonworks.com@HORTONWORKS.COM
KEYTAB_PATH=/tmp/full-keytab.keytab
JDBC="jdbc:hive2://sandbox.hortonworks.com:10000/default\;principal=hive/sandbox.hortonworks.com@HORTONWORKS.COM"

CMD="
beeline \
--hiveconf hive.server2.authentication=kerberos \
--hiveconf hive.metastore.kerberos.keytab.file=$KEYTAB_PATH
--hiveconf hive.metastore.kerberos.principal=$PRINCIPAL
-u $JDBC
"

echo "$CMD"
eval "$CMD"
