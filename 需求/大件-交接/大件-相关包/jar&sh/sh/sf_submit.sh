#!/bin/sh
#!/bin/bash
source /etc/profile
startDay=$1
currentDay=$2
last3Day=$3
spark-submit \
--class com.sf.realtime.spark.batch.main.forecast.InRoadCargoTaskNew \
--master yarn \
--deploy-mode client \
--driver-memory 10g \
--num-executors 30 \
--executor-memory 20G \
--executor-cores 2 \
--queue root.predict_ky \
eos-fms-rms-realtime-spark.jar $startDay "$currentDay" $last3Day


 