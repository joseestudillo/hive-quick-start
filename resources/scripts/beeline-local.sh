. $HIVE_HOME/bin/hive-local-config.sh
#For error related with jline terminal go to: https://cwiki.apache.org/confluence/display/Hive/Hive+on+Spark%3A+Getting+Started#HiveonSpark:GettingStarted-ConfiguringHive
# --hiveconf log4j.rootLogger=INFO,console is not working for me, I had to go the $HIVE_HOME/conf folder and set beeline-log4j.properties there

beeline -u jdbc:hive2:// "$@"
