#!/bin/sh
#!/bin/bash
source /etc/profile
currentDay=$1
spark-submit \
--class com.sf.realtime.spark.batch.main.forecast.SxVehicleInSfDept \
--master yarn \
--deploy-mode client \
--driver-memory 5g \
--num-executors 30 \
--executor-memory 10G \
--executor-cores 2 \
--queue root.predict_ky \
eos-fms-rms-realtime-spark.jar "$currentDay"