#!/usr/bin/env bash

for i in `seq 1 9`;
do
    hadoop jar target/vector-mr-0.1-SNAPSHOT-jar-with-dependencies.jar io.hgis.vector.mr.EcoregionProtectionMR true
done
