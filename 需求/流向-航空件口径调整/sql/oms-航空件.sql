


/**
    -、航空件回刷历史数据至临时表
        -、模型需要回刷历史版本一份
	 step0 ，昨天订单数据写入临时表，后面1~24小时共用 
	
**/
drop table if exists tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620;
create table if not exists tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620 
stored as parquet as
select
  orderid,
  waybillno,
  from_unixtime(cast(cast(ordertmstamp as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') as ordertm,
  syssource,
  srcdeptcode,
  srccitycode,
  srcareacode,
  srchqcode,
  destdeptcode,
  destcitycode,
  destareacode,
  desthqcode,
  limittypecode,
  productcode,
  incomecode,
  lasttime,
  iscancel,
  ispick,
  limittag,
  from_unixtime(cast(cast(processversiontm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss') as processversiontm,
  b.route_code
from
 ( select * from dm_kafka_rdmp.dm_full_order_dtl_df -- 订单实时表
where
  inc_day = '$[time(yyyyMMdd,-1d)]' ) a  -- 手动指定-小时所在天
  left join 
  (select a1.waybill_no,a1.route_code from
	  (select waybill_no,route_code,row_number() over(partition by waybill_no) as rn
		from dwd.dwd_waybill_info_dtl_di
		where inc_day >= '$[time(yyyyMMdd,-30d)]' and route_code != '' and route_code is not null
	  ) a1
	where a1.rn = 1
  ) b on a.waybillno = b.waybill_no
  ;
  
  
  

drop table if exists tmp_ordi_predict.tmp_oms_cityflow_dynamic_order_1_byday_tmp20230620;
create table if not exists tmp_ordi_predict.tmp_oms_cityflow_dynamic_order_1_byday_tmp20230620 
stored as parquet as
select 
t.*
from (
select 
a.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-1d)]', '01') AS inc_dayhour,  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
	and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0004' and limittag = 'SP6' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0008' and limittag in('T4','T801') and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
      else '0' end  as is_air_order
,row_number() over(partition by a.orderid order by a.processversiontm desc) as rank    
from 
  (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620 ) a) t
where t.rank=1
      and t.iscancel=0
      -- and to_date(t.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
	  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
      -- and t.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
	   and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 01:00:00')  -- 手动指定-小时

union all 
select 
t.*
from (
select 
a.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-1d)]', '02') AS inc_dayhour,  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
	and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0004' and limittag = 'SP6' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0008' and limittag in('T4','T801') and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
      else '0' end  as is_air_order
,row_number() over(partition by a.orderid order by a.processversiontm desc) as rank    
from 
  (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620 ) a) t
where t.rank=1
      and t.iscancel=0
      -- and to_date(t.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
	  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
      -- and t.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
	   and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 02:00:00')  -- 手动指定-小时
	
union all 
select 
t.*
from (
select 
a.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-1d)]', '03') AS inc_dayhour,  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
	and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0004' and limittag = 'SP6' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0008' and limittag in('T4','T801') and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
      else '0' end  as is_air_order
,row_number() over(partition by a.orderid order by a.processversiontm desc) as rank    
from 
  (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620 ) a) t
where t.rank=1
      and t.iscancel=0
      -- and to_date(t.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
	  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
      -- and t.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
	   and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 03:00:00')  -- 手动指定-小时
	  
union all 
select 
t.*
from (
select 
a.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-1d)]', '04') AS inc_dayhour,  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
	and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0004' and limittag = 'SP6' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0008' and limittag in('T4','T801') and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
      else '0' end  as is_air_order
,row_number() over(partition by a.orderid order by a.processversiontm desc) as rank    
from 
  (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620 ) a) t
where t.rank=1
      and t.iscancel=0
      -- and to_date(t.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
	  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
      -- and t.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
	   and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 04:00:00')  -- 手动指定-小时
	  
union all 
select 
t.*
from (
select 
a.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-1d)]', '05') AS inc_dayhour,  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
	and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0004' and limittag = 'SP6' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0008' and limittag in('T4','T801') and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
      else '0' end  as is_air_order
,row_number() over(partition by a.orderid order by a.processversiontm desc) as rank    
from 
  (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620 ) a) t
where t.rank=1
      and t.iscancel=0
      -- and to_date(t.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
	  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
      -- and t.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
	   and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 05:00:00')  -- 手动指定-小时
	   
	   
	  union all 
select 
t.*
from (
select 
a.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-1d)]', '06') AS inc_dayhour,  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
	and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0004' and limittag = 'SP6' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0008' and limittag in('T4','T801') and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
      else '0' end  as is_air_order
,row_number() over(partition by a.orderid order by a.processversiontm desc) as rank    
from 
  (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620 ) a) t
where t.rank=1
      and t.iscancel=0
      -- and to_date(t.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
	  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
      -- and t.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
	   and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 06:00:00')  -- 手动指定-小时
	   
	union all 
select 
t.*
from (
select 
a.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-1d)]', '07') AS inc_dayhour,  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
	and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0004' and limittag = 'SP6' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0008' and limittag in('T4','T801') and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
      else '0' end  as is_air_order
,row_number() over(partition by a.orderid order by a.processversiontm desc) as rank    
from 
  (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620 ) a) t
where t.rank=1
      and t.iscancel=0
      -- and to_date(t.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
	  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
      -- and t.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
	   and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 07:00:00')  -- 手动指定-小时
	   

union all 
select 
t.*
from (
select 
a.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-1d)]', '08') AS inc_dayhour,  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
	and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0004' and limittag = 'SP6' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0008' and limittag in('T4','T801') and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
      else '0' end  as is_air_order
,row_number() over(partition by a.orderid order by a.processversiontm desc) as rank    
from 
  (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620 ) a) t
where t.rank=1
      and t.iscancel=0
      -- and to_date(t.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
	  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
      -- and t.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
	   and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 08:00:00')  -- 手动指定-小时
	   
	   
union all 
select 
t.*
from (
select 
a.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-1d)]', '09') AS inc_dayhour,  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
	and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0004' and limittag = 'SP6' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0008' and limittag in('T4','T801') and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
      else '0' end  as is_air_order
,row_number() over(partition by a.orderid order by a.processversiontm desc) as rank    
from 
  (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620 ) a) t
where t.rank=1
      and t.iscancel=0
      -- and to_date(t.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
	  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
      -- and t.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
	   and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 09:00:00')  -- 手动指定-小时	   



union all 
select 
t.*
from (
select 
a.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-1d)]', '10') AS inc_dayhour,  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
	and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0004' and limittag = 'SP6' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0008' and limittag in('T4','T801') and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
      else '0' end  as is_air_order
,row_number() over(partition by a.orderid order by a.processversiontm desc) as rank    
from 
  (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620 ) a) t
where t.rank=1
      and t.iscancel=0
      -- and to_date(t.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
	  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
      -- and t.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
	   and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 10:00:00')  -- 手动指定-小时

union all 
select 
t.*
from (
select 
a.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-1d)]', '11') AS inc_dayhour,  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
	and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0004' and limittag = 'SP6' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0008' and limittag in('T4','T801') and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
      else '0' end  as is_air_order
,row_number() over(partition by a.orderid order by a.processversiontm desc) as rank    
from 
  (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620 ) a) t
where t.rank=1
      and t.iscancel=0
      -- and to_date(t.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
	  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
      -- and t.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
	   and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 11:00:00')  -- 手动指定-小时
	   
	   
union all 
select 
t.*
from (
select 
a.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-1d)]', '12') AS inc_dayhour,  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
	and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0004' and limittag = 'SP6' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0008' and limittag in('T4','T801') and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
      else '0' end  as is_air_order
,row_number() over(partition by a.orderid order by a.processversiontm desc) as rank    
from 
  (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620 ) a) t
where t.rank=1
      and t.iscancel=0
      -- and to_date(t.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
	  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
      -- and t.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
	   and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 12:00:00')  -- 手动指定-小时
	   
	   
	   
	union all 
select 
t.*
from (
select 
a.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-1d)]', '13') AS inc_dayhour,  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
	and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0004' and limittag = 'SP6' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0008' and limittag in('T4','T801') and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
      else '0' end  as is_air_order
,row_number() over(partition by a.orderid order by a.processversiontm desc) as rank    
from 
  (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620 ) a) t
where t.rank=1
      and t.iscancel=0
      -- and to_date(t.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
	  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
      -- and t.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
	   and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 13:00:00')  -- 手动指定-小时
	   
	   
union all 
select 
t.*
from (
select 
a.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-1d)]', '14') AS inc_dayhour,  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
	and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0004' and limittag = 'SP6' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0008' and limittag in('T4','T801') and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
      else '0' end  as is_air_order
,row_number() over(partition by a.orderid order by a.processversiontm desc) as rank    
from 
  (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620 ) a) t
where t.rank=1
      and t.iscancel=0
      -- and to_date(t.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
	  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
      -- and t.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
	   and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 14:00:00')  -- 手动指定-小时
	   
	   
union all 
select 
t.*
from (
select 
a.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-1d)]', '15') AS inc_dayhour,  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
	and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0004' and limittag = 'SP6' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0008' and limittag in('T4','T801') and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
      else '0' end  as is_air_order
,row_number() over(partition by a.orderid order by a.processversiontm desc) as rank    
from 
  (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620 ) a) t
where t.rank=1
      and t.iscancel=0
      -- and to_date(t.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
	  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
      -- and t.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
	   and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 15:00:00')  -- 手动指定-小时
	   
	   
union all 
select 
t.*
from (
select 
a.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-1d)]', '16') AS inc_dayhour,  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
	and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0004' and limittag = 'SP6' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0008' and limittag in('T4','T801') and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
      else '0' end  as is_air_order
,row_number() over(partition by a.orderid order by a.processversiontm desc) as rank    
from 
  (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620 ) a) t
where t.rank=1
      and t.iscancel=0
      -- and to_date(t.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
	  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
      -- and t.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
	   and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 16:00:00')  -- 手动指定-小时
	   
	   
union all 
select 
t.*
from (
select 
a.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-1d)]', '17') AS inc_dayhour,  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
	and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0004' and limittag = 'SP6' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0008' and limittag in('T4','T801') and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
      else '0' end  as is_air_order
,row_number() over(partition by a.orderid order by a.processversiontm desc) as rank    
from 
  (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620 ) a) t
where t.rank=1
      and t.iscancel=0
      -- and to_date(t.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
	  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
      -- and t.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
	   and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 17:00:00')  -- 手动指定-小时
	   
	   
union all 
select 
t.*
from (
select 
a.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-1d)]', '18') AS inc_dayhour,  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
	and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0004' and limittag = 'SP6' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0008' and limittag in('T4','T801') and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
      else '0' end  as is_air_order
,row_number() over(partition by a.orderid order by a.processversiontm desc) as rank    
from 
  (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620 ) a) t
where t.rank=1
      and t.iscancel=0
      -- and to_date(t.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
	  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
      -- and t.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
	   and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 18:00:00')  -- 手动指定-小时
	   
	   
union all 
select 
t.*
from (
select 
a.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-1d)]', '19') AS inc_dayhour,  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
	and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0004' and limittag = 'SP6' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0008' and limittag in('T4','T801') and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
      else '0' end  as is_air_order
,row_number() over(partition by a.orderid order by a.processversiontm desc) as rank    
from 
  (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620 ) a) t
where t.rank=1
      and t.iscancel=0
      -- and to_date(t.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
	  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
      -- and t.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
	   and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 19:00:00')  -- 手动指定-小时
	   
	   
	   union all 
select 
t.*
from (
select 
a.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-1d)]', '20') AS inc_dayhour,  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
	and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0004' and limittag = 'SP6' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0008' and limittag in('T4','T801') and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
      else '0' end  as is_air_order
,row_number() over(partition by a.orderid order by a.processversiontm desc) as rank    
from 
  (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620 ) a) t
where t.rank=1
      and t.iscancel=0
      -- and to_date(t.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
	  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
      -- and t.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
	   and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 20:00:00')  -- 手动指定-小时
	   
	   
union all 
select 
t.*
from (
select 
a.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-1d)]', '21') AS inc_dayhour,  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
	and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0004' and limittag = 'SP6' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0008' and limittag in('T4','T801') and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
      else '0' end  as is_air_order
,row_number() over(partition by a.orderid order by a.processversiontm desc) as rank    
from 
  (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620 ) a) t
where t.rank=1
      and t.iscancel=0
      -- and to_date(t.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
	  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
      -- and t.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
	   and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 21:00:00')  -- 手动指定-小时
	   
	   
union all 
select 
t.*
from (
select 
a.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-1d)]', '22') AS inc_dayhour,  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
	and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0004' and limittag = 'SP6' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0008' and limittag in('T4','T801') and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
      else '0' end  as is_air_order
,row_number() over(partition by a.orderid order by a.processversiontm desc) as rank    
from 
  (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620 ) a) t
where t.rank=1
      and t.iscancel=0
      -- and to_date(t.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
	  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
      -- and t.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
	   and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 22:00:00')  -- 手动指定-小时
	   
	   
union all 
select 
t.*
from (
select 
a.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-1d)]', '23') AS inc_dayhour,  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
	and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0004' and limittag = 'SP6' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0008' and limittag in('T4','T801') and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
      else '0' end  as is_air_order
,row_number() over(partition by a.orderid order by a.processversiontm desc) as rank    
from 
  (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620 ) a) t
where t.rank=1
      and t.iscancel=0
      -- and to_date(t.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
	  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
      -- and t.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
	   and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 23:00:00')  -- 手动指定-小时
	   
	   
union all 
select 
t.*
from (
select 
a.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-1d)]', '24') AS inc_dayhour,  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
	and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0004' and limittag = 'SP6' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
            when productcode='SE0008' and limittag in('T4','T801') and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
      else '0' end  as is_air_order
,row_number() over(partition by a.orderid order by a.processversiontm desc) as rank    
from 
  (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620 ) a) t
where t.rank=1
      and t.iscancel=0
      -- and to_date(t.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
	  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
      -- and t.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
	   and t.ordertm <= concat('$[time(yyyy-MM-dd)]', ' 00:00:00')  -- 手动指定-小时
;

 /**
 step2  分组计数，写入临时备份结果表
**/
set hive.exec.dynamic.partition= true;
set hive.exec.dynamic.partition.mode= nostrict;
set hive.exec.max.dynamic.partitions=2000;  -- 所有执行节点上最多可以多少个动态分区
set hive.exec.max.dynamic.partitions.pernode = 500;

insert overwrite table dm_ordi_predict.dws_cityflow_dynamic_order_hi_backup20230620 partition (inc_day,inc_dayhour)
 select 
  order_dt
  ,city_flow
  ,all_order_num
  ,air_order_num
  ,regexp_replace(order_dt,'-','') as inc_day
  ,inc_dayhour
 from 
(select 
  to_date(t.ordertm) as order_dt
 ,inc_dayhour
 ,city_flow 
 ,count(orderid) as all_order_num
 ,count(case when is_air_order='1' then orderid end) as air_order_num
 from tmp_ordi_predict.tmp_oms_cityflow_dynamic_order_1_byday_tmp20230620 t
 group by to_date(t.ordertm) 
 ,inc_dayhour
 ,city_flow ) al;
