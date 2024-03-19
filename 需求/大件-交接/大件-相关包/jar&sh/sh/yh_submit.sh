#!/bin/sh
#!/bin/bash
source /etc/profile
currentDay=$1
incDay=$2
spark-submit \
--class com.sf.realtime.spark.batch.main.sf.KyHasArriveCargoYZ \
--master yarn \
--deploy-mode client \
--driver-memory 10g \
--num-executors 40 \
--executor-memory 10G \
--executor-cores 2 \
--queue root.predict_ky \
eos-fms-rms-realtime-spark.jar "$currentDay" "$incDay"