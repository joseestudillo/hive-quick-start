. $HIVE_HOME/bin/hive-local-config.sh

#For error related with jline terminal go to: https://cwiki.apache.org/confluence/display/Hive/Hive+on+Spark%3A+Getting+Started#HiveonSpark:GettingStarted-ConfiguringHive
#to show more verbose logging pass the parameter: --hiveconf hive.root.logger=INFO,console

hive "$@"
