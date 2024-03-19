#!/bin/sh
#!/bin/bash
source /etc/profile
startTime=$1
endTime=$2
spark-submit \
--class com.sf.realtime.spark.batch.main.forecast.HasArriveCargoTaskNew \
--master yarn \
--deploy-mode client \
--driver-memory 10g \
--num-executors 20 \
--executor-memory 10G \
--executor-cores 2 \
--queue root.predict_ky \
eos-fms-rms-realtime-spark.jar "${startTime}" "${endTime}"