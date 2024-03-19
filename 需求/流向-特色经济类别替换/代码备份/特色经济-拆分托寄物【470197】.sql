

set hive.auto.convert.join=true;

/*
drop table if exists tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp1;
create table tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp1  
stored as parquet as
--select * from (
    select  waybill_no,
         source_zone_code,
		 consigned_tm,
		 cons_name,
		 concat_ws(',',cons_name) as cons_name_concat,
		 product_code,
		 freight_monthly_acct_code,
		 dest_zone_code,
		 quantity,
		  meterage_weight_qty,
		 signin_tm,
		 inc_day
   --      row_number() over(partition by waybill_no order by 1) rn 
     from dwd.dwd_waybill_info_dtl_di t-- modify by 01390963 20200818 
	-- 	 from gdl.tt_waybill_info
 where inc_day between  '$[time(yyyyMMdd,-10d)]' and '$[time(yyyyMMdd)]' ;
 */
 
 --托寄物去重
 set spark.sql.shuffle.partitions=600;
 set spark.sql.adaptive.enabled=false;
drop table if exists tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp2;
create table tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp2   
stored as parquet as
select  
	cons_name_concat
	,upper(cons_name_concat) as cons_name_concat_up
	,min(inc_day) as inc_day 
from tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp1
where inc_day between '$[time(yyyyMMdd,-10d)]' and '$[time(yyyyMMdd,-1d)]'
group by 
	cons_name_concat
	,upper(cons_name_concat);