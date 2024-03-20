-- 已到达货量计算完毕 写入结果表
insert overwrite table tmp_ordi_predict.ky_has_send_cargo_tmp_248838 partition(inc_day='$[time(yyyyMMdd)]') 
select a.require_id,a.car_no,a.src_zone_code,a.dest_zone_code,a.send_time,a.arrive_time,a.tickets,a.weight,'$[time(yyyy-MM-dd HH:mm:ss)]',a.translevel from 
(select 
	require_id,car_no,translevel,src_zone_code,dest_zone_code,send_time,arrive_time,
	sum(tickets) as tickets,
	sum(weight) as weight	
from tmp_ordi_predict.tmp_all_arrive_behind_hbase_248838
group by require_id,car_no,translevel,src_zone_code,dest_zone_code,send_time,arrive_time
) a where a.src_zone_code !=  a.dest_zone_code 
;

