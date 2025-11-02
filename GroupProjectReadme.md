# How to compile and move jar file to container
sbt spark/assembly && cp spark/target/scala-2.13/SparkJob.jar docker/apps/spark

# Go into bash of the container
docker exec -it spark-master /bin/bash

# Submit the Spark job using the spark-submit command
/opt/spark/bin/spark-submit --class org.cscie88c.spark.TaxiDataAnalyzeMainJob --master spark://spark-master:7077 /opt/spark-apps/SparkJob.jar /opt/spark-data/yellow_tripdata_2025-01.parquet /opt/spark-data/taxi_zone_lookup.csv /opt/spark-data/out/ 1 4 
