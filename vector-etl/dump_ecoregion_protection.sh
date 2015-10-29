#!/bin/bash

for i in `seq 1 9`;
do
    java -cp ../hbase-mr/target/hbase-mr-0.1-SNAPSHOT.jar:target/vector-etl-0.1-SNAPSHOT.jar:target/dependency/* io.hgis.dump.ExecuteDump ee_protection ${i}
done

