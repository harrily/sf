-- 已到达货量计算完毕 写入结果表
set hive.merge.sparkFiles=true;
set mapred.max.split.size=268435456;
set mapred.min.split.size.per.rack=268435456;
set mapred.min.split.size.per.node=268435456;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.reducers.bytes.per.reducer=268435456;
set hive.exec.reducers.max=1099;
set hive.merge.size.per.task=268435456;
set hive.merge.smallfiles.avgsize=134217728;



insert overwrite table tmp_ordi_predict.ky_has_arrive_cargo_rb_tmp_553293 partition(inc_day='$[time(yyyyMMdd)]') 
select regexp_replace(reflect("java.util.UUID", "randomUUID"), "-", "")  as id,a.require_id,a.car_no,a.src_zone_code,a.dest_zone_code,a.send_time,a.arrive_time,a.tickets,a.weight,'$[time(yyyy-MM-dd HH:mm:ss)]' as as create_time,a.translevel ,a.car_status from 
(select 
	require_id,car_no,translevel,src_zone_code,dest_zone_code,send_time,arrive_time,car_status
	sum(tickets) as tickets,
	sum(weight) as weight	
from tmp_ordi_predict.tmp_all_arrive_behind_hbase_553293
group by require_id,car_no,translevel,src_zone_code,dest_zone_code,send_time,arrive_time,car_status
) a where a.src_zone_code !=  a.dest_zone_code 
;

