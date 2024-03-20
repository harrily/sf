#!/bin/sh
#!/bin/bash
source /etc/profile
incDay=$1
incHour=$2
beginTime=$3
endTime=$4
spark-submit \
--class com.sf.realtime.spark.batch.main.sf.KyHasArriveCargoYZByHbase \
--master yarn \
--deploy-mode client \
--driver-memory 10g \
--num-executors 150 \
--executor-memory 8G \
--executor-cores 2 \
--queue root.predict_ky \
eos-fms-rms-realtime-spark.jar "$incDay" "$incHour" "$beginTime" "$endTime"