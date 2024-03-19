
/**流向-oms-航空件调整-new-回刷历史数据（按天）
	1、回刷历史，按天， T-0d
	2、使用运单宽表获取路由代码
	3、限制processtime
	4、处理srcdeptcode = 'null'

**/
drop table if exists tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620_new2;
create table if not exists tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620_new2 
stored as parquet as
select
  orderid,
  waybillno_new as waybillno ,
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
 (
    select a.*,if(a.waybillno is null or a.waybillno = '' , b.waybill_no,a.waybillno) as waybillno_new from 
        (
            select * from dm_kafka_rdmp.dm_full_order_dtl_df where  inc_day = '$[time(yyyyMMdd,-0d)]'  -- 手动指定-刷新当天
            -- 回刷历史，添加处理时间限制
            AND from_unixtime(cast(processversiontm AS bigint) / 1000, 'yyyy-MM-dd HH:mm:ss') < '$[time(yyyy-MM-dd HH:00:00)]' 
        ) a 
    left join 
        (select al.waybill_no,al.order_no from 
            (SELECT waybill_no,order_no ,row_number() over(partition by order_no) as rn 
                FROM ods_shiva_oms.tt_waybill_no_to_order_no
                where inc_day between '$[time(yyyyMMdd,-10d)]' and '$[time(yyyyMMdd,-0d)]'
                and waybill_no != '' and waybill_no is not null
                and order_no != '' and order_no is not null 
            ) al where al.rn = 1 
        )  b 
    on a.orderno = b.order_no
 ) a  -- 刷新历史，使用运单宽表
  left join 
  (select a1.waybill_no,a1.route_code from
	  (select waybill_no,route_code,row_number() over(partition by waybill_no) as rn
		from dwd.dwd_waybill_info_dtl_di
		where inc_day between '$[time(yyyyMMdd,-0d)]' and '$[time(yyyyMMdd,+7d)]'      
        and route_code != '' and route_code is not null
	  ) a1
	where a1.rn = 1
  ) b on a.waybillno_new = b.waybill_no
  ;




  
drop table if exists tmp_ordi_predict.tmp_oms_cityflow_dynamic_order_1_byhour_tmp20230620_new2;
create table if not exists tmp_ordi_predict.tmp_oms_cityflow_dynamic_order_1_byhour_tmp20230620_new2 
stored as parquet as
select 
t.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-0d)]', '01') AS inc_dayhour  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
            and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0004' and limittag = 'SP6' and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0008' and limittag in('T4','T801') and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
else '0' end  as is_air_order
from 
    (select 
            t1.* 
            ,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
        from
            (
                select 
                    a.* 
                    ,rank() over(partition by a.orderid order by a.processversiontm desc) as rank    
                from 
                    (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620_new2) a   
            ) t1
        where t1.rank=1
        and t1.iscancel=0
        --and to_date(t1.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
		and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-0d)]'  --手动指定-小时所在天
        --and t1.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
		and t.ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 01:00:00')  -- 手动指定-小时
    ) t 
where t.rn=1 

union all 
select 
t.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-0d)]', '02') AS inc_dayhour  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
            and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0004' and limittag = 'SP6' and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0008' and limittag in('T4','T801') and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
else '0' end  as is_air_order
from 
    (select 
            t1.* 
            ,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
        from
            (
                select 
                    a.* 
                    ,rank() over(partition by a.orderid order by a.processversiontm desc) as rank    
                from 
                    (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620_new2) a   
            ) t1
        where t1.rank=1
        and t1.iscancel=0
        --and to_date(t1.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
		and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-0d)]'  --手动指定-小时所在天
        --and t1.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
		and t.ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 02:00:00')  -- 手动指定-小时
    ) t 
where t.rn=1 

union all 
select 
t.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-0d)]', '03') AS inc_dayhour  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
            and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0004' and limittag = 'SP6' and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0008' and limittag in('T4','T801') and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
else '0' end  as is_air_order
from 
    (select 
            t1.* 
            ,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
        from
            (
                select 
                    a.* 
                    ,rank() over(partition by a.orderid order by a.processversiontm desc) as rank    
                from 
                    (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620_new2) a   
            ) t1
        where t1.rank=1
        and t1.iscancel=0
        --and to_date(t1.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
		and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-0d)]'  --手动指定-小时所在天
        --and t1.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
		and t.ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 03:00:00')  -- 手动指定-小时
    ) t 
where t.rn=1 

union all 
select 
t.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-0d)]', '04') AS inc_dayhour  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
            and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0004' and limittag = 'SP6' and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0008' and limittag in('T4','T801') and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
else '0' end  as is_air_order
from 
    (select 
            t1.* 
            ,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
        from
            (
                select 
                    a.* 
                    ,rank() over(partition by a.orderid order by a.processversiontm desc) as rank    
                from 
                    (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620_new2) a   
            ) t1
        where t1.rank=1
        and t1.iscancel=0
        --and to_date(t1.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
		and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-0d)]'  --手动指定-小时所在天
        --and t1.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
		and t.ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 04:00:00')  -- 手动指定-小时
    ) t 
where t.rn=1 

union all 
select 
t.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-0d)]', '05') AS inc_dayhour  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
            and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0004' and limittag = 'SP6' and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0008' and limittag in('T4','T801') and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
else '0' end  as is_air_order
from 
    (select 
            t1.* 
            ,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
        from
            (
                select 
                    a.* 
                    ,rank() over(partition by a.orderid order by a.processversiontm desc) as rank    
                from 
                    (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620_new2) a   
            ) t1
        where t1.rank=1
        and t1.iscancel=0
        --and to_date(t1.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
		and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-0d)]'  --手动指定-小时所在天
        --and t1.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
		and t.ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 05:00:00')  -- 手动指定-小时
    ) t 
where t.rn=1 

union all 
select 
t.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-0d)]', '06') AS inc_dayhour  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
            and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0004' and limittag = 'SP6' and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0008' and limittag in('T4','T801') and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
else '0' end  as is_air_order
from 
    (select 
            t1.* 
            ,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
        from
            (
                select 
                    a.* 
                    ,rank() over(partition by a.orderid order by a.processversiontm desc) as rank    
                from 
                    (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620_new2) a   
            ) t1
        where t1.rank=1
        and t1.iscancel=0
        --and to_date(t1.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
		and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-0d)]'  --手动指定-小时所在天
        --and t1.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
		and t.ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 06:00:00')  -- 手动指定-小时
    ) t 
where t.rn=1 t1
union all 
select 
t.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-0d)]', '07') AS inc_dayhour  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
            and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0004' and limittag = 'SP6' and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0008' and limittag in('T4','T801') and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
else '0' end  as is_air_order
from 
    (select 
            t1.* 
            ,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
        from
            (
                select 
                    a.* 
                    ,rank() over(partition by a.orderid order by a.processversiontm desc) as rank    
                from 
                    (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620_new2) a   
            ) t1
        where t1.rank=1
        and t1.iscancel=0
        --and to_date(t1.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
		and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-0d)]'  --手动指定-小时所在天
        --and t1.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
		and t.ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 07:00:00')  -- 手动指定-小时
    ) t 
where t.rn=1 


union all 
select 
t.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-0d)]', '08') AS inc_dayhour  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
            and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0004' and limittag = 'SP6' and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0008' and limittag in('T4','T801') and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
else '0' end  as is_air_order
from 
    (select 
            t1.* 
            ,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
        from
            (
                select 
                    a.* 
                    ,rank() over(partition by a.orderid order by a.processversiontm desc) as rank    
                from 
                    (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620_new2) a   
            ) t1
        where t1.rank=1
        and t1.iscancel=0
        --and to_date(t1.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
		and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-0d)]'  --手动指定-小时所在天
        --and t1.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
		and t.ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 08:00:00')  -- 手动指定-小时
    ) t 
where t.rn=1 

union all 
select 
t.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-0d)]', '09') AS inc_dayhour  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
            and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0004' and limittag = 'SP6' and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0008' and limittag in('T4','T801') and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
else '0' end  as is_air_order
from 
    (select 
            t1.* 
            ,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
        from
            (
                select 
                    a.* 
                    ,rank() over(partition by a.orderid order by a.processversiontm desc) as rank    
                from 
                    (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620_new2) a   
            ) t1
        where t1.rank=1
        and t1.iscancel=0
        --and to_date(t1.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
		and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-0d)]'  --手动指定-小时所在天
        --and t1.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
		and t.ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 09:00:00')  -- 手动指定-小时
    ) t 
where t.rn=1 

union all 
select 
t.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-0d)]', '10') AS inc_dayhour  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
            and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0004' and limittag = 'SP6' and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0008' and limittag in('T4','T801') and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
else '0' end  as is_air_order
from 
    (select 
            t1.* 
            ,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
        from
            (
                select 
                    a.* 
                    ,rank() over(partition by a.orderid order by a.processversiontm desc) as rank    
                from 
                    (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620_new2) a   
            ) t1
        where t1.rank=1
        and t1.iscancel=0
        --and to_date(t1.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
		and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-0d)]'  --手动指定-小时所在天
        --and t1.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
		and t.ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 10:00:00')  -- 手动指定-小时
    ) t 
where t.rn=1 

union all 
select 
t.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-0d)]', '11') AS inc_dayhour  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
            and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0004' and limittag = 'SP6' and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0008' and limittag in('T4','T801') and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
else '0' end  as is_air_order
from 
    (select 
            t1.* 
            ,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
        from
            (
                select 
                    a.* 
                    ,rank() over(partition by a.orderid order by a.processversiontm desc) as rank    
                from 
                    (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620_new2) a   
            ) t1
        where t1.rank=1
        and t1.iscancel=0
        --and to_date(t1.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
		and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-0d)]'  --手动指定-小时所在天
        --and t1.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
		and t.ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 11:00:00')  -- 手动指定-小时
    ) t 
where t.rn=1 

union all 
select 
t.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-0d)]', '12') AS inc_dayhour  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
            and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0004' and limittag = 'SP6' and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0008' and limittag in('T4','T801') and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
else '0' end  as is_air_order
from 
    (select 
            t1.* 
            ,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
        from
            (
                select 
                    a.* 
                    ,rank() over(partition by a.orderid order by a.processversiontm desc) as rank    
                from 
                    (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620_new2) a   
            ) t1
        where t1.rank=1
        and t1.iscancel=0
        --and to_date(t1.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
		and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-0d)]'  --手动指定-小时所在天
        --and t1.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
		and t.ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 12:00:00')  -- 手动指定-小时
    ) t 
where t.rn=1 


union all 
select 
t.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-0d)]', '13') AS inc_dayhour  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
            and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0004' and limittag = 'SP6' and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0008' and limittag in('T4','T801') and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
else '0' end  as is_air_order
from 
    (select 
            t1.* 
            ,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
        from
            (
                select 
                    a.* 
                    ,rank() over(partition by a.orderid order by a.processversiontm desc) as rank    
                from 
                    (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620_new2) a   
            ) t1
        where t1.rank=1
        and t1.iscancel=0
        --and to_date(t1.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
		and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-0d)]'  --手动指定-小时所在天
        --and t1.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
		and t.ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 13:00:00')  -- 手动指定-小时
    ) t 
where t.rn=1 

union all 
select 
t.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-0d)]', '14') AS inc_dayhour  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
            and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0004' and limittag = 'SP6' and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0008' and limittag in('T4','T801') and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
else '0' end  as is_air_order
from 
    (select 
            t1.* 
            ,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
        from
            (
                select 
                    a.* 
                    ,rank() over(partition by a.orderid order by a.processversiontm desc) as rank    
                from 
                    (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620_new2) a   
            ) t1
        where t1.rank=1
        and t1.iscancel=0
        --and to_date(t1.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
		and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-0d)]'  --手动指定-小时所在天
        --and t1.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
		and t.ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 14:00:00')  -- 手动指定-小时
    ) t 
where t.rn=1 

union all 
select 
t.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-0d)]', '15') AS inc_dayhour  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
            and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0004' and limittag = 'SP6' and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0008' and limittag in('T4','T801') and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
else '0' end  as is_air_order
from 
    (select 
            t1.* 
            ,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
        from
            (
                select 
                    a.* 
                    ,rank() over(partition by a.orderid order by a.processversiontm desc) as rank    
                from 
                    (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620_new2) a   
            ) t1
        where t1.rank=1
        and t1.iscancel=0
        --and to_date(t1.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
		and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-0d)]'  --手动指定-小时所在天
        --and t1.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
		and t.ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 15:00:00')  -- 手动指定-小时
    ) t 
where t.rn=1 


union all 
select 
t.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-0d)]', '16') AS inc_dayhour  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
            and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0004' and limittag = 'SP6' and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0008' and limittag in('T4','T801') and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
else '0' end  as is_air_order
from 
    (select 
            t1.* 
            ,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
        from
            (
                select 
                    a.* 
                    ,rank() over(partition by a.orderid order by a.processversiontm desc) as rank    
                from 
                    (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620_new2) a   
            ) t1
        where t1.rank=1
        and t1.iscancel=0
        --and to_date(t1.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
		and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-0d)]'  --手动指定-小时所在天
        --and t1.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
		and t.ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 16:00:00')  -- 手动指定-小时
    ) t 
where t.rn=1 


union all 
select 
t.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-0d)]', '17') AS inc_dayhour  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
            and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0004' and limittag = 'SP6' and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0008' and limittag in('T4','T801') and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
else '0' end  as is_air_order
from 
    (select 
            t1.* 
            ,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
        from
            (
                select 
                    a.* 
                    ,rank() over(partition by a.orderid order by a.processversiontm desc) as rank    
                from 
                    (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620_new2) a   
            ) t1
        where t1.rank=1
        and t1.iscancel=0
        --and to_date(t1.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
		and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-0d)]'  --手动指定-小时所在天
        --and t1.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
		and t.ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 17:00:00')  -- 手动指定-小时
    ) t 
where t.rn=1 

union all 
select 
t.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-0d)]', '18') AS inc_dayhour  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
            and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0004' and limittag = 'SP6' and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0008' and limittag in('T4','T801') and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
else '0' end  as is_air_order
from 
    (select 
            t1.* 
            ,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
        from
            (
                select 
                    a.* 
                    ,rank() over(partition by a.orderid order by a.processversiontm desc) as rank    
                from 
                    (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620_new2) a   
            ) t1
        where t1.rank=1
        and t1.iscancel=0
        --and to_date(t1.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
		and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-0d)]'  --手动指定-小时所在天
        --and t1.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
		and t.ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 18:00:00')  -- 手动指定-小时
    ) t 
where t.rn=1 

union all 
select 
t.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-0d)]', '19') AS inc_dayhour  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
            and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0004' and limittag = 'SP6' and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0008' and limittag in('T4','T801') and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
else '0' end  as is_air_order
from 
    (select 
            t1.* 
            ,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
        from
            (
                select 
                    a.* 
                    ,rank() over(partition by a.orderid order by a.processversiontm desc) as rank    
                from 
                    (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620_new2) a   
            ) t1
        where t1.rank=1
        and t1.iscancel=0
        --and to_date(t1.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
		and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-0d)]'  --手动指定-小时所在天
        --and t1.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
		and t.ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 19:00:00')  -- 手动指定-小时
    ) t 
where t.rn=1 


union all 
select 
t.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-0d)]', '20') AS inc_dayhour  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
            and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0004' and limittag = 'SP6' and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0008' and limittag in('T4','T801') and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
else '0' end  as is_air_order
from 
    (select 
            t1.* 
            ,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
        from
            (
                select 
                    a.* 
                    ,rank() over(partition by a.orderid order by a.processversiontm desc) as rank    
                from 
                    (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620_new2) a   
            ) t1
        where t1.rank=1
        and t1.iscancel=0
        --and to_date(t1.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
		and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-0d)]'  --手动指定-小时所在天
        --and t1.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
		and t.ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 20:00:00')  -- 手动指定-小时
    ) t 
where t.rn=1 


union all 
select 
t.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-0d)]', '21') AS inc_dayhour  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
            and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0004' and limittag = 'SP6' and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0008' and limittag in('T4','T801') and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
else '0' end  as is_air_order
from 
    (select 
            t1.* 
            ,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
        from
            (
                select 
                    a.* 
                    ,rank() over(partition by a.orderid order by a.processversiontm desc) as rank    
                from 
                    (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620_new2) a   
            ) t1
        where t1.rank=1
        and t1.iscancel=0
        --and to_date(t1.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
		and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-0d)]'  --手动指定-小时所在天
        --and t1.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
		and t.ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 21:00:00')  -- 手动指定-小时
    ) t 
where t.rn=1 


union all 
select 
t.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-0d)]', '22') AS inc_dayhour  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
            and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0004' and limittag = 'SP6' and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0008' and limittag in('T4','T801') and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
else '0' end  as is_air_order
from 
    (select 
            t1.* 
            ,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
        from
            (
                select 
                    a.* 
                    ,rank() over(partition by a.orderid order by a.processversiontm desc) as rank    
                from 
                    (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620_new2) a   
            ) t1
        where t1.rank=1
        and t1.iscancel=0
        --and to_date(t1.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
		and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-0d)]'  --手动指定-小时所在天
        --and t1.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
		and t.ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 22:00:00')  -- 手动指定-小时
    ) t 
where t.rn=1 


union all 
select 
t.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-0d)]', '23') AS inc_dayhour  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
            and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0004' and limittag = 'SP6' and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0008' and limittag in('T4','T801') and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
else '0' end  as is_air_order
from 
    (select 
            t1.* 
            ,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
        from
            (
                select 
                    a.* 
                    ,rank() over(partition by a.orderid order by a.processversiontm desc) as rank    
                from 
                    (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620_new2) a   
            ) t1
        where t1.rank=1
        and t1.iscancel=0
        --and to_date(t1.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
		and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-0d)]'  --手动指定-小时所在天
        --and t1.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
		and t.ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 23:00:00')  -- 手动指定-小时
    ) t 
where t.rn=1 

union all 
select 
t.*
,concat(srccitycode,'-',destcitycode) as city_flow
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
,concat('$[time(yyyyMMdd,-0d)]', '24') AS inc_dayhour  -- 手动指定-小时
-- 202104之后逻辑
--  20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
            and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0004' and limittag = 'SP6' and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
    when productcode='SE0008' and limittag in('T4','T801') and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
else '0' end  as is_air_order
from 
    (select 
            t1.* 
            ,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
        from
            (
                select 
                    a.* 
                    ,rank() over(partition by a.orderid order by a.processversiontm desc) as rank    
                from 
                    (tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620_new2) a   
            ) t1
        where t1.rank=1
        and t1.iscancel=0
        --and to_date(t1.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
		and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-0d)]'  --手动指定-小时所在天
        --and t1.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
		and t.ordertm <= concat('$[time(yyyy-MM-dd,+1d)]', ' 00:00:00')  -- 手动指定-小时
    ) t 
where t.rn=1 
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
 from tmp_ordi_predict.tmp_oms_cityflow_dynamic_order_1_byhour_tmp20230620_new2 t
 group by to_date(t.ordertm) 
 ,inc_dayhour
 ,city_flow ) al;
