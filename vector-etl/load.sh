#!/bin/sh
java -cp ../hbase-mr/target/hbase-mr-0.1-SNAPSHOT.jar:target/vector-etl-0.1-SNAPSHOT.jar:target/dependency/* io.hgis.load.LoadSites

