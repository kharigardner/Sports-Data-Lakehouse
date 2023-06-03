#!/bin/bash

echo "Launching Spark"

$SPARK_HOME/bin/pyspark --packages io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-aws:3.2.2,com.amazonaws:aws-java-sdk-bundle:1.11.11,net.java.dev.jets3t:jets3t:0.9.4 \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
    --conf "spark.hive.metastore.warehouse.dir='usr/local/spark/warehouse'" \
    --conf "spark.databricks.delta.schema.autoMerge.enabled=true" \
    --conf "spark.databricks.delta.checkLatestSchemaOnRead=false"