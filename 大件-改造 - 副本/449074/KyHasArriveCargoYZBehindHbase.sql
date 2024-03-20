

set spark.streaming.stopGracefullyOnShutdown=true;
set spark.streaming.backpressure.enabled=true;
set spark.sql.shuffle.partitions=300;
set spark.default.parallelism=300;
set spark.debug.maxToStringFields=100;
set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;



-- 刷新车标，写入hive
insert overwrite table tmp_ordi_predict.dmpdsp_vt_has_arrive_cars_hf partition (inc_day,inc_hour) 
select
  idKey,
  requireId,
  carNo,
  transLevel,
  carStatus,
  srcZoneCode,
  destZoneCode,
  sum(ticket) as ticket,
  sum(weight) as weight,
  actualTime,
  '$[time(yyyy-MM-dd HH:mm:ss)]' as insertTime,
  '${incDay}' as inc_day,
  '${incHour}'as inc_hour
from
  tmp_ordi_predict.tmp_ky_has_arrive_cargo_yz_behind_hbase
group by
  idKey,
  requireId,
  carNo,
  transLevel,
  carStatus,
  srcZoneCode,
  destZoneCode,
  actualTime
;