# Hive

## Concepts:

- _database_ or _schema_: defines a group of tables

## Installing

- create the folder `/user/local/apache/hive/`
- extract the content of the latest release into it. At the time of this writing `apache-hive-1.2.1-bin`
- create a symb link: `ln -sf apache-hive-1.2.1-bin/ current`
- create the environment variables:
```bash
#Hive
export HIVE_HOME=/usr/local/apache/hive/current
export PATH=$PATH:$HIVE_HOME/bin
```

This should be enough for a development instance, but in some cases we will want to run the hive CLI, the metastore or even the hiveserver2 at the same time, to be able to do this we will need a database that actually keeps the information. To avoid installing a database in the host, we will use a Derby database Server. This will require some extra configuration

```bash
 #Derby
export DERBY_HOME=/usr/local/apache/derby/current
export DERBY_INSTALL=$DERBY_HOME
export PATH=$PATH:$DERBY_HOME/bin

 #Hive
export HIVE_HOME=/usr/local/apache/hive/current
export PATH=$PATH:$HIVE_HOME/bin
export HIVE_AUX_JARS_PATH=$DERBY_HOME/lib
```

Besides this configuration an additional file is required, `$HIVE_HOME/conf/jdo.properties` which is included in the project.

## Launching hive

In the folder `resources/hive/scripts` there are several Scripts that make easier launching the different Hive applications.

- `install-serde.sh`: Script designed to be used in place, the idea is to compile the project and generate a jar file that can be used as a SerDe library.
- `hive-local-config.sh`: This script is intent to be included in all the other scripts. It contains the general configuration to run hive locally. Here is where you configure if you want the applications to be launched using a Derby server or an standalone instance. Notice that if you use derby server, a derby server must be running before launching any of the other scripts. As it is explained on the script:

```bash
 # to create a new instance of derby:
 # -hiveconf javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=/tmp/hive/metastore_db;create=true
 # to use a derby server: 
 # -hiveconf javax.jdo.option.ConnectionURL=jdbc:derby://localhost:1527/tmp/hive/metastore_db;create=true
```
- `hive-derby-local.sh`: launches a derby server to be shared across different hive applications.
- `beeline-local.sh`: launch the beeline CLI.
- `hive-local.sh`: launch the hive CLI.
- `hive-metastore-server-local.sh`: lauches the hive metastore.
- `hiveserver2-local.sh`: launches the hiveserver2. 

## Code examples

- `com.joseestudillo.hive`:
  - `HiveJDBC`: Using JDBC againts Hive.
  - `HiveMetastore`: Connecting the to the Hive metastore from Java.
- `com.joseestudillo.hive.serde`: SerDe examples.
- `com.joseestudillo.hive.udf`: Udf examples, there are also examples about how to unit test UDFs.

- `resources/hql`: contains examples of HQL scripts, showing how to do different operations from the Hive CLI. All this scripts can be run using `hive-local.sh -f SCRIPT_NAME.hql`, notice that some of then will require the jar file generated (`install-serde.sh` will do the job).

## TODOs
- Create your own Type and ObjectInspector
- How to use/query union types
