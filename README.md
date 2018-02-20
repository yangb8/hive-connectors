# hive-connectors
Apache Hive connectors for Pravega.

Description
-----------

Implementation of a Hive connector for Pravega. It leverages Pravega batch client to read all existing events in parallel.

Build
-------
The build script handles Pravega as a _source dependency_, meaning that the connector is linked to a specific commit of Pravega (as opposed to a specific release version) in order to faciliate co-development.  This is accomplished with a combination of a _git submodule_ and the use of Gradle's _composite build_ feature. 

### Cloning the repository
When cloning the connector repository, be sure to instruct git to recursively checkout submodules, e.g.:
```
git clone --recurse-submodules https://github.com/yangb8/hive-connectors.git
```

To update an existing repository:
```
git submodule update --init --recursive
mv pravega/config/ pravega/config.tmp (temporary workaround to avoid submodule build failure)
```

### Building Pravega
Pravega is built automatically by the connector build script.

### Building Hive Connector
Build the connector:
```
./gradlew build (w/o dependencies)
./gradlew shadowJar (w/ dependencies)
```

Test
-------
```
./gradlew test
```

Run Examples
---
```
First, install/setup hdfs, hive and pravega properly

hive> add jar ~/hive-connectors/build/libs/hive-connectors-0.3.0-SNAPSHOT-all.jar;

hive> CREATE external TABLE pravega (
  data string
) STORED BY 'io.pravega.connectors.hive.PravegaStorageHandler'
TBLPROPERTIES("pravega.scope"="myScope","pravega.stream"="myStream", "pravega.uri"="tcp://192.168.0.188:9090", "pravega.deserializer"="io.pravega.client.stream.impl.JavaSerializer");

hive> select * from pravega;

hive> select * from pravega where data>"a1b2c3";
```
