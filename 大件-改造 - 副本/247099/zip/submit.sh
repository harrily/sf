#!/bin/sh
#!/bin/bash
source /etc/profile
spark-submit \
--class com.sf.realtime.spark.batch.main.forecast.InRoadCargoTaskByHbase \
--master yarn \
--deploy-mode client \
--driver-memory 10g \
--num-executors 150 \
--executor-memory 8G \
--executor-cores 2 \
--queue root.predict_ky \
eos-fms-rms-realtime-spark.jar