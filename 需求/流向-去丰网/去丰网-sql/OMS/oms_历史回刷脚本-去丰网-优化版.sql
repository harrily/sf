
/**
	 step0 ，昨天订单数据写入临时表，后面1~24小时共用
**/
drop table if exists tmp_ordi_predict.dm_full_order_dtl_df_by_day;
create table if not exists tmp_ordi_predict.dm_full_order_dtl_df_by_day 
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
  from_unixtime(cast(cast(processversiontm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss') as processversiontm
from
  dm_kafka_rdmp.dm_full_order_dtl_df -- 订单实时表
where
  inc_day = '$[time(yyyyMMdd,-1d)]'   -- 手动指定-小时所在天
and (srchqcode<>'CN39' or srchqcode is null)         
and (desthqcode<>'CN39' or desthqcode is null)
and (incomecode<>'9' or incomecode is null )               
and (nvl(srcdeptcode,'') in (select dept_code from dim.dim_department where hq_code<>'CN39' and dept_code is not null group by dept_code ) or srcdeptcode is null )  
and (nvl(destdeptcode,'') in (select dept_code from dim.dim_department where hq_code<>'CN39' and dept_code is not null group by dept_code ) or destdeptcode is null ) ;

/**
	step1 计算昨天(相对业务日期)的数据汇总
**/
drop table if exists tmp_ordi_predict.tmp_oms_cityflow_dynamic_order_1_byday;
create table if not exists tmp_ordi_predict.tmp_oms_cityflow_dynamic_order_1_byday 
stored as parquet as
select
  t.*
from
  (
    select
      a.*,
      concat(srccitycode, '-', destcitycode) as city_flow,
	  -- case when substr('$[time(yyyyMMddHH)]', 9, 2) = '00' then concat('$[time(yyyyMMdd,-1h)]', '24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour,
	 concat('$[time(yyyyMMdd,-1d)]', '01') AS inc_dayhour,  -- 手动指定-小时
	  case
        when productcode in (
          'SE0001',
          'SE0107',
          'SE0109',
          'SE0146',
          'SE0089',
          'SE0121',
          'SE0137',
          'SE000201',
          'SE0103',
          'SE0051',
          'SE0152',
          'SE000206'
        ) then '1'
        when productcode = 'SE0004' and limittag = 'SP6' then '1'
        when productcode = 'SE0008' and limittag in('T4', 'T801') then '1'
        else '0' end as is_air_order,
      row_number() over( partition by a.orderid order by a.processversiontm desc) as rank
    from
      (
		tmp_ordi_predict.dm_full_order_dtl_df_by_day
      ) a
  ) t
where
  t.rank = 1
  and t.iscancel = 0
  -- and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1h)]'
  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
  -- and t.ordertm <= concat('$[time(yyyy-MM-dd HH)]', ':00:00')
  and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 01:00:00')  -- 手动指定-小时

UNION ALL
select
  t.*
from
  (
    select
      a.*,
      concat(srccitycode, '-', destcitycode) as city_flow,
	  -- case when substr('$[time(yyyyMMddHH)]', 9, 2) = '00' then concat('$[time(yyyyMMdd,-1h)]', '24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour,
	  concat('$[time(yyyyMMdd,-1d)]', '02') AS inc_dayhour,  -- 手动指定-小时
	  case
        when productcode in (
          'SE0001',
          'SE0107',
          'SE0109',
          'SE0146',
          'SE0089',
          'SE0121',
          'SE0137',
          'SE000201',
          'SE0103',
          'SE0051',
          'SE0152',
          'SE000206'
        ) then '1'
        when productcode = 'SE0004' and limittag = 'SP6' then '1'
        when productcode = 'SE0008' and limittag in('T4', 'T801') then '1'
        else '0' end as is_air_order,
      row_number() over( partition by a.orderid order by a.processversiontm desc) as rank
    from
      (
        tmp_ordi_predict.dm_full_order_dtl_df_by_day 
      ) a
  ) t
where
  t.rank = 1
  and t.iscancel = 0
  -- and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1h)]'
  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
  -- and t.ordertm <= concat('$[time(yyyy-MM-dd HH)]', ':00:00')
  and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 02:00:00')  -- 手动指定-小时

UNION ALL
select
  t.*
from
  (
    select
      a.*,
      concat(srccitycode, '-', destcitycode) as city_flow,
	  -- case when substr('$[time(yyyyMMddHH)]', 9, 2) = '00' then concat('$[time(yyyyMMdd,-1h)]', '24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour,
	  concat('$[time(yyyyMMdd,-1d)]', '03') AS inc_dayhour,  -- 手动指定-小时
	  case
        when productcode in (
          'SE0001',
          'SE0107',
          'SE0109',
          'SE0146',
          'SE0089',
          'SE0121',
          'SE0137',
          'SE000201',
          'SE0103',
          'SE0051',
          'SE0152',
          'SE000206'
        ) then '1'
        when productcode = 'SE0004' and limittag = 'SP6' then '1'
        when productcode = 'SE0008' and limittag in('T4', 'T801') then '1'
        else '0' end as is_air_order,
      row_number() over( partition by a.orderid order by a.processversiontm desc) as rank
    from
      (
        tmp_ordi_predict.dm_full_order_dtl_df_by_day 
      ) a
  ) t
where
  t.rank = 1
  and t.iscancel = 0
  -- and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1h)]'
  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
  -- and t.ordertm <= concat('$[time(yyyy-MM-dd HH)]', ':00:00')
  and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 03:00:00')  -- 手动指定-小时
  
UNION ALL
select
  t.*
from
  (
    select
      a.*,
      concat(srccitycode, '-', destcitycode) as city_flow,
	  -- case when substr('$[time(yyyyMMddHH)]', 9, 2) = '00' then concat('$[time(yyyyMMdd,-1h)]', '24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour,
	  concat('$[time(yyyyMMdd,-1d)]', '04') AS inc_dayhour,  -- 手动指定-小时
	  case
        when productcode in (
          'SE0001',
          'SE0107',
          'SE0109',
          'SE0146',
          'SE0089',
          'SE0121',
          'SE0137',
          'SE000201',
          'SE0103',
          'SE0051',
          'SE0152',
          'SE000206'
        ) then '1'
        when productcode = 'SE0004' and limittag = 'SP6' then '1'
        when productcode = 'SE0008' and limittag in('T4', 'T801') then '1'
        else '0' end as is_air_order,
      row_number() over( partition by a.orderid order by a.processversiontm desc) as rank
    from
      (
        tmp_ordi_predict.dm_full_order_dtl_df_by_day 
      ) a
  ) t
where
  t.rank = 1
  and t.iscancel = 0
  -- and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1h)]'
  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
  -- and t.ordertm <= concat('$[time(yyyy-MM-dd HH)]', ':00:00')
  and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 04:00:00')  -- 手动指定-小时
  
UNION ALL
select
  t.*
from
  (
    select
      a.*,
      concat(srccitycode, '-', destcitycode) as city_flow,
	  -- case when substr('$[time(yyyyMMddHH)]', 9, 2) = '00' then concat('$[time(yyyyMMdd,-1h)]', '24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour,
	  concat('$[time(yyyyMMdd,-1d)]', '05') AS inc_dayhour,  -- 手动指定-小时
	  case
        when productcode in (
          'SE0001',
          'SE0107',
          'SE0109',
          'SE0146',
          'SE0089',
          'SE0121',
          'SE0137',
          'SE000201',
          'SE0103',
          'SE0051',
          'SE0152',
          'SE000206'
        ) then '1'
        when productcode = 'SE0004' and limittag = 'SP6' then '1'
        when productcode = 'SE0008' and limittag in('T4', 'T801') then '1'
        else '0' end as is_air_order,
      row_number() over( partition by a.orderid order by a.processversiontm desc) as rank
    from
      (
        tmp_ordi_predict.dm_full_order_dtl_df_by_day 
      ) a
  ) t
where
  t.rank = 1
  and t.iscancel = 0
  -- and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1h)]'
  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
  -- and t.ordertm <= concat('$[time(yyyy-MM-dd HH)]', ':00:00')
  and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 05:00:00')  -- 手动指定-小时
  
UNION ALL
select
  t.*
from
  (
    select
      a.*,
      concat(srccitycode, '-', destcitycode) as city_flow,
	  -- case when substr('$[time(yyyyMMddHH)]', 9, 2) = '00' then concat('$[time(yyyyMMdd,-1h)]', '24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour,
	  concat('$[time(yyyyMMdd,-1d)]', '06') AS inc_dayhour,  -- 手动指定-小时
	  case
        when productcode in (
          'SE0001',
          'SE0107',
          'SE0109',
          'SE0146',
          'SE0089',
          'SE0121',
          'SE0137',
          'SE000201',
          'SE0103',
          'SE0051',
          'SE0152',
          'SE000206'
        ) then '1'
        when productcode = 'SE0004' and limittag = 'SP6' then '1'
        when productcode = 'SE0008' and limittag in('T4', 'T801') then '1'
        else '0' end as is_air_order,
      row_number() over( partition by a.orderid order by a.processversiontm desc) as rank
    from
      (
		tmp_ordi_predict.dm_full_order_dtl_df_by_day	  
      ) a
  ) t
where
  t.rank = 1
  and t.iscancel = 0
  -- and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1h)]'
  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
  -- and t.ordertm <= concat('$[time(yyyy-MM-dd HH)]', ':00:00')
  and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 06:00:00')  -- 手动指定-小时
 
 UNION ALL
select
  t.*
from
  (
    select
      a.*,
      concat(srccitycode, '-', destcitycode) as city_flow,
	  -- case when substr('$[time(yyyyMMddHH)]', 9, 2) = '00' then concat('$[time(yyyyMMdd,-1h)]', '24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour,
	  concat('$[time(yyyyMMdd,-1d)]', '07') AS inc_dayhour,  -- 手动指定-小时
	  case
        when productcode in (
          'SE0001',
          'SE0107',
          'SE0109',
          'SE0146',
          'SE0089',
          'SE0121',
          'SE0137',
          'SE000201',
          'SE0103',
          'SE0051',
          'SE0152',
          'SE000206'
        ) then '1'
        when productcode = 'SE0004' and limittag = 'SP6' then '1'
        when productcode = 'SE0008' and limittag in('T4', 'T801') then '1'
        else '0' end as is_air_order,
      row_number() over( partition by a.orderid order by a.processversiontm desc) as rank
    from
      (
        tmp_ordi_predict.dm_full_order_dtl_df_by_day
      ) a
  ) t
where
  t.rank = 1
  and t.iscancel = 0
  -- and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1h)]'
  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
  -- and t.ordertm <= concat('$[time(yyyy-MM-dd HH)]', ':00:00')
  and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 07:00:00')  -- 手动指定-小时
  
  UNION ALL
select
  t.*
from
  (
    select
      a.*,
      concat(srccitycode, '-', destcitycode) as city_flow,
	  -- case when substr('$[time(yyyyMMddHH)]', 9, 2) = '00' then concat('$[time(yyyyMMdd,-1h)]', '24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour,
	  concat('$[time(yyyyMMdd,-1d)]', '08') AS inc_dayhour,  -- 手动指定-小时
	  case
        when productcode in (
          'SE0001',
          'SE0107',
          'SE0109',
          'SE0146',
          'SE0089',
          'SE0121',
          'SE0137',
          'SE000201',
          'SE0103',
          'SE0051',
          'SE0152',
          'SE000206'
        ) then '1'
        when productcode = 'SE0004' and limittag = 'SP6' then '1'
        when productcode = 'SE0008' and limittag in('T4', 'T801') then '1'
        else '0' end as is_air_order,
      row_number() over( partition by a.orderid order by a.processversiontm desc) as rank
    from
      (
        tmp_ordi_predict.dm_full_order_dtl_df_by_day 
      ) a
  ) t
where
  t.rank = 1
  and t.iscancel = 0
  -- and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1h)]'
  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
  -- and t.ordertm <= concat('$[time(yyyy-MM-dd HH)]', ':00:00')
  and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 08:00:00')  -- 手动指定-小时
  
  UNION ALL
select
  t.*
from
  (
    select
      a.*,
      concat(srccitycode, '-', destcitycode) as city_flow,
	  -- case when substr('$[time(yyyyMMddHH)]', 9, 2) = '00' then concat('$[time(yyyyMMdd,-1h)]', '24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour,
	  concat('$[time(yyyyMMdd,-1d)]', '09') AS inc_dayhour,  -- 手动指定-小时
	  case
        when productcode in (
          'SE0001',
          'SE0107',
          'SE0109',
          'SE0146',
          'SE0089',
          'SE0121',
          'SE0137',
          'SE000201',
          'SE0103',
          'SE0051',
          'SE0152',
          'SE000206'
        ) then '1'
        when productcode = 'SE0004' and limittag = 'SP6' then '1'
        when productcode = 'SE0008' and limittag in('T4', 'T801') then '1'
        else '0' end as is_air_order,
      row_number() over( partition by a.orderid order by a.processversiontm desc) as rank
    from
      (
        tmp_ordi_predict.dm_full_order_dtl_df_by_day  
      ) a
  ) t
where
  t.rank = 1
  and t.iscancel = 0
  -- and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1h)]'
  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
  -- and t.ordertm <= concat('$[time(yyyy-MM-dd HH)]', ':00:00')
  and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 09:00:00')  -- 手动指定-小时
  
  UNION ALL
select
  t.*
from
  (
    select
      a.*,
      concat(srccitycode, '-', destcitycode) as city_flow,
	  -- case when substr('$[time(yyyyMMddHH)]', 9, 2) = '00' then concat('$[time(yyyyMMdd,-1h)]', '24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour,
	  concat('$[time(yyyyMMdd,-1d)]', '10') AS inc_dayhour,  -- 手动指定-小时
	  case
        when productcode in (
          'SE0001',
          'SE0107',
          'SE0109',
          'SE0146',
          'SE0089',
          'SE0121',
          'SE0137',
          'SE000201',
          'SE0103',
          'SE0051',
          'SE0152',
          'SE000206'
        ) then '1'
        when productcode = 'SE0004' and limittag = 'SP6' then '1'
        when productcode = 'SE0008' and limittag in('T4', 'T801') then '1'
        else '0' end as is_air_order,
      row_number() over( partition by a.orderid order by a.processversiontm desc) as rank
    from
      (
        tmp_ordi_predict.dm_full_order_dtl_df_by_day
      ) a
  ) t
where
  t.rank = 1
  and t.iscancel = 0
  -- and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1h)]'
  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
  -- and t.ordertm <= concat('$[time(yyyy-MM-dd HH)]', ':00:00')
  and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 10:00:00')  -- 手动指定-小时
  
   UNION ALL
select
  t.*
from
  (
    select
      a.*,
      concat(srccitycode, '-', destcitycode) as city_flow,
	  -- case when substr('$[time(yyyyMMddHH)]', 9, 2) = '00' then concat('$[time(yyyyMMdd,-1h)]', '24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour,
	  concat('$[time(yyyyMMdd,-1d)]', '11') AS inc_dayhour,  -- 手动指定-小时
	  case
        when productcode in (
          'SE0001',
          'SE0107',
          'SE0109',
          'SE0146',
          'SE0089',
          'SE0121',
          'SE0137',
          'SE000201',
          'SE0103',
          'SE0051',
          'SE0152',
          'SE000206'
        ) then '1'
        when productcode = 'SE0004' and limittag = 'SP6' then '1'
        when productcode = 'SE0008' and limittag in('T4', 'T801') then '1'
        else '0' end as is_air_order,
      row_number() over( partition by a.orderid order by a.processversiontm desc) as rank
    from
      (
        tmp_ordi_predict.dm_full_order_dtl_df_by_day
      ) a
  ) t
where
  t.rank = 1
  and t.iscancel = 0
  -- and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1h)]'
  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
  -- and t.ordertm <= concat('$[time(yyyy-MM-dd HH)]', ':00:00')
  and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 11:00:00')  -- 手动指定-小时
  
 UNION ALL
select
  t.*
from
  (
    select
      a.*,
      concat(srccitycode, '-', destcitycode) as city_flow,
	  -- case when substr('$[time(yyyyMMddHH)]', 9, 2) = '00' then concat('$[time(yyyyMMdd,-1h)]', '24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour,
	  concat('$[time(yyyyMMdd,-1d)]', '12') AS inc_dayhour,  -- 手动指定-小时
	  case
        when productcode in (
          'SE0001',
          'SE0107',
          'SE0109',
          'SE0146',
          'SE0089',
          'SE0121',
          'SE0137',
          'SE000201',
          'SE0103',
          'SE0051',
          'SE0152',
          'SE000206'
        ) then '1'
        when productcode = 'SE0004' and limittag = 'SP6' then '1'
        when productcode = 'SE0008' and limittag in('T4', 'T801') then '1'
        else '0' end as is_air_order,
      row_number() over( partition by a.orderid order by a.processversiontm desc) as rank
    from
      (
        tmp_ordi_predict.dm_full_order_dtl_df_by_day  
      ) a
  ) t
where
  t.rank = 1
  and t.iscancel = 0
  -- and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1h)]'
  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
  -- and t.ordertm <= concat('$[time(yyyy-MM-dd HH)]', ':00:00')
  and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 12:00:00')  -- 手动指定-小时
  
   UNION ALL
select
  t.*
from
  (
    select
      a.*,
      concat(srccitycode, '-', destcitycode) as city_flow,
	  -- case when substr('$[time(yyyyMMddHH)]', 9, 2) = '00' then concat('$[time(yyyyMMdd,-1h)]', '24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour,
	  concat('$[time(yyyyMMdd,-1d)]', '13') AS inc_dayhour,  -- 手动指定-小时
	  case
        when productcode in (
          'SE0001',
          'SE0107',
          'SE0109',
          'SE0146',
          'SE0089',
          'SE0121',
          'SE0137',
          'SE000201',
          'SE0103',
          'SE0051',
          'SE0152',
          'SE000206'
        ) then '1'
        when productcode = 'SE0004' and limittag = 'SP6' then '1'
        when productcode = 'SE0008' and limittag in('T4', 'T801') then '1'
        else '0' end as is_air_order,
      row_number() over( partition by a.orderid order by a.processversiontm desc) as rank
    from
      (
        tmp_ordi_predict.dm_full_order_dtl_df_by_day
      ) a
  ) t
where
  t.rank = 1
  and t.iscancel = 0
  -- and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1h)]'
  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
  -- and t.ordertm <= concat('$[time(yyyy-MM-dd HH)]', ':00:00')
  and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 13:00:00')  -- 手动指定-小时
  
   UNION ALL
select
  t.*
from
  (
    select
      a.*,
      concat(srccitycode, '-', destcitycode) as city_flow,
	  -- case when substr('$[time(yyyyMMddHH)]', 9, 2) = '00' then concat('$[time(yyyyMMdd,-1h)]', '24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour,
	  concat('$[time(yyyyMMdd,-1d)]', '14') AS inc_dayhour,  -- 手动指定-小时
	  case
        when productcode in (
          'SE0001',
          'SE0107',
          'SE0109',
          'SE0146',
          'SE0089',
          'SE0121',
          'SE0137',
          'SE000201',
          'SE0103',
          'SE0051',
          'SE0152',
          'SE000206'
        ) then '1'
        when productcode = 'SE0004' and limittag = 'SP6' then '1'
        when productcode = 'SE0008' and limittag in('T4', 'T801') then '1'
        else '0' end as is_air_order,
      row_number() over( partition by a.orderid order by a.processversiontm desc) as rank
    from
      (
        tmp_ordi_predict.dm_full_order_dtl_df_by_day
      ) a
  ) t
where
  t.rank = 1
  and t.iscancel = 0
  -- and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1h)]'
  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
  -- and t.ordertm <= concat('$[time(yyyy-MM-dd HH)]', ':00:00')
  and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 14:00:00')  -- 手动指定-小时
  
   UNION ALL
select
  t.*
from
  (
    select
      a.*,
      concat(srccitycode, '-', destcitycode) as city_flow,
	  -- case when substr('$[time(yyyyMMddHH)]', 9, 2) = '00' then concat('$[time(yyyyMMdd,-1h)]', '24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour,
	  concat('$[time(yyyyMMdd,-1d)]', '15') AS inc_dayhour,  -- 手动指定-小时
	  case
        when productcode in (
          'SE0001',
          'SE0107',
          'SE0109',
          'SE0146',
          'SE0089',
          'SE0121',
          'SE0137',
          'SE000201',
          'SE0103',
          'SE0051',
          'SE0152',
          'SE000206'
        ) then '1'
        when productcode = 'SE0004' and limittag = 'SP6' then '1'
        when productcode = 'SE0008' and limittag in('T4', 'T801') then '1'
        else '0' end as is_air_order,
      row_number() over( partition by a.orderid order by a.processversiontm desc) as rank
    from
      (
        tmp_ordi_predict.dm_full_order_dtl_df_by_day 
      ) a
  ) t
where
  t.rank = 1
  and t.iscancel = 0
  -- and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1h)]'
  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
  -- and t.ordertm <= concat('$[time(yyyy-MM-dd HH)]', ':00:00')
  and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 15:00:00')  -- 手动指定-小时
  
   UNION ALL
select
  t.*
from
  (
    select
      a.*,
      concat(srccitycode, '-', destcitycode) as city_flow,
	  -- case when substr('$[time(yyyyMMddHH)]', 9, 2) = '00' then concat('$[time(yyyyMMdd,-1h)]', '24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour,
	  concat('$[time(yyyyMMdd,-1d)]', '16') AS inc_dayhour,  -- 手动指定-小时
	  case
        when productcode in (
          'SE0001',
          'SE0107',
          'SE0109',
          'SE0146',
          'SE0089',
          'SE0121',
          'SE0137',
          'SE000201',
          'SE0103',
          'SE0051',
          'SE0152',
          'SE000206'
        ) then '1'
        when productcode = 'SE0004' and limittag = 'SP6' then '1'
        when productcode = 'SE0008' and limittag in('T4', 'T801') then '1'
        else '0' end as is_air_order,
      row_number() over( partition by a.orderid order by a.processversiontm desc) as rank
    from
      (
        tmp_ordi_predict.dm_full_order_dtl_df_by_day
      ) a
  ) t
where
  t.rank = 1
  and t.iscancel = 0
  -- and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1h)]'
  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
  -- and t.ordertm <= concat('$[time(yyyy-MM-dd HH)]', ':00:00')
  and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 16:00:00')  -- 手动指定-小时
  
   UNION ALL
select
  t.*
from
  (
    select
      a.*,
      concat(srccitycode, '-', destcitycode) as city_flow,
	  -- case when substr('$[time(yyyyMMddHH)]', 9, 2) = '00' then concat('$[time(yyyyMMdd,-1h)]', '24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour,
	  concat('$[time(yyyyMMdd,-1d)]', '17') AS inc_dayhour,  -- 手动指定-小时
	  case
        when productcode in (
          'SE0001',
          'SE0107',
          'SE0109',
          'SE0146',
          'SE0089',
          'SE0121',
          'SE0137',
          'SE000201',
          'SE0103',
          'SE0051',
          'SE0152',
          'SE000206'
        ) then '1'
        when productcode = 'SE0004' and limittag = 'SP6' then '1'
        when productcode = 'SE0008' and limittag in('T4', 'T801') then '1'
        else '0' end as is_air_order,
      row_number() over( partition by a.orderid order by a.processversiontm desc) as rank
    from
      (
        tmp_ordi_predict.dm_full_order_dtl_df_by_day 
      ) a
  ) t
where
  t.rank = 1
  and t.iscancel = 0
  -- and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1h)]'
  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
  -- and t.ordertm <= concat('$[time(yyyy-MM-dd HH)]', ':00:00')
  and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 17:00:00')  -- 手动指定-小时
  
   UNION ALL
select
  t.*
from
  (
    select
      a.*,
      concat(srccitycode, '-', destcitycode) as city_flow,
	  -- case when substr('$[time(yyyyMMddHH)]', 9, 2) = '00' then concat('$[time(yyyyMMdd,-1h)]', '24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour,
	  concat('$[time(yyyyMMdd,-1d)]', '18') AS inc_dayhour,  -- 手动指定-小时
	  case
        when productcode in (
          'SE0001',
          'SE0107',
          'SE0109',
          'SE0146',
          'SE0089',
          'SE0121',
          'SE0137',
          'SE000201',
          'SE0103',
          'SE0051',
          'SE0152',
          'SE000206'
        ) then '1'
        when productcode = 'SE0004' and limittag = 'SP6' then '1'
        when productcode = 'SE0008' and limittag in('T4', 'T801') then '1'
        else '0' end as is_air_order,
      row_number() over( partition by a.orderid order by a.processversiontm desc) as rank
    from
      (
        tmp_ordi_predict.dm_full_order_dtl_df_by_day
      ) a
  ) t
where
  t.rank = 1
  and t.iscancel = 0
  -- and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1h)]'
  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
  -- and t.ordertm <= concat('$[time(yyyy-MM-dd HH)]', ':00:00')
  and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 18:00:00')  -- 手动指定-小时
  
   UNION ALL
select
  t.*
from
  (
    select
      a.*,
      concat(srccitycode, '-', destcitycode) as city_flow,
	  -- case when substr('$[time(yyyyMMddHH)]', 9, 2) = '00' then concat('$[time(yyyyMMdd,-1h)]', '24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour,
	  concat('$[time(yyyyMMdd,-1d)]', '19') AS inc_dayhour,  -- 手动指定-小时
	  case
        when productcode in (
          'SE0001',
          'SE0107',
          'SE0109',
          'SE0146',
          'SE0089',
          'SE0121',
          'SE0137',
          'SE000201',
          'SE0103',
          'SE0051',
          'SE0152',
          'SE000206'
        ) then '1'
        when productcode = 'SE0004' and limittag = 'SP6' then '1'
        when productcode = 'SE0008' and limittag in('T4', 'T801') then '1'
        else '0' end as is_air_order,
      row_number() over( partition by a.orderid order by a.processversiontm desc) as rank
    from
      (
        tmp_ordi_predict.dm_full_order_dtl_df_by_day 
      ) a
  ) t
where
  t.rank = 1
  and t.iscancel = 0
  -- and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1h)]'
  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
  -- and t.ordertm <= concat('$[time(yyyy-MM-dd HH)]', ':00:00')
  and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 19:00:00')  -- 手动指定-小时
  
   UNION ALL
select
  t.*
from
  (
    select
      a.*,
      concat(srccitycode, '-', destcitycode) as city_flow,
	  -- case when substr('$[time(yyyyMMddHH)]', 9, 2) = '00' then concat('$[time(yyyyMMdd,-1h)]', '24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour,
	  concat('$[time(yyyyMMdd,-1d)]', '20') AS inc_dayhour,  -- 手动指定-小时
	  case
        when productcode in (
          'SE0001',
          'SE0107',
          'SE0109',
          'SE0146',
          'SE0089',
          'SE0121',
          'SE0137',
          'SE000201',
          'SE0103',
          'SE0051',
          'SE0152',
          'SE000206'
        ) then '1'
        when productcode = 'SE0004' and limittag = 'SP6' then '1'
        when productcode = 'SE0008' and limittag in('T4', 'T801') then '1'
        else '0' end as is_air_order,
      row_number() over( partition by a.orderid order by a.processversiontm desc) as rank
    from
      (
        tmp_ordi_predict.dm_full_order_dtl_df_by_day 
      ) a
  ) t
where
  t.rank = 1
  and t.iscancel = 0
  -- and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1h)]'
  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
  -- and t.ordertm <= concat('$[time(yyyy-MM-dd HH)]', ':00:00')
  and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 20:00:00')  -- 手动指定-小时
  
   UNION ALL
select
  t.*
from
  (
    select
      a.*,
      concat(srccitycode, '-', destcitycode) as city_flow,
	  -- case when substr('$[time(yyyyMMddHH)]', 9, 2) = '00' then concat('$[time(yyyyMMdd,-1h)]', '24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour,
	  concat('$[time(yyyyMMdd,-1d)]', '21') AS inc_dayhour,  -- 手动指定-小时
	  case
        when productcode in (
          'SE0001',
          'SE0107',
          'SE0109',
          'SE0146',
          'SE0089',
          'SE0121',
          'SE0137',
          'SE000201',
          'SE0103',
          'SE0051',
          'SE0152',
          'SE000206'
        ) then '1'
        when productcode = 'SE0004' and limittag = 'SP6' then '1'
        when productcode = 'SE0008' and limittag in('T4', 'T801') then '1'
        else '0' end as is_air_order,
      row_number() over( partition by a.orderid order by a.processversiontm desc) as rank
    from
      (
        tmp_ordi_predict.dm_full_order_dtl_df_by_day 
      ) a
  ) t
where
  t.rank = 1
  and t.iscancel = 0
  -- and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1h)]'
  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
  -- and t.ordertm <= concat('$[time(yyyy-MM-dd HH)]', ':00:00')
  and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 21:00:00')  -- 手动指定-小时
  
   UNION ALL
select
  t.*
from
  (
    select
      a.*,
      concat(srccitycode, '-', destcitycode) as city_flow,
	  -- case when substr('$[time(yyyyMMddHH)]', 9, 2) = '00' then concat('$[time(yyyyMMdd,-1h)]', '24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour,
	  concat('$[time(yyyyMMdd,-1d)]', '22') AS inc_dayhour,  -- 手动指定-小时
	  case
        when productcode in (
          'SE0001',
          'SE0107',
          'SE0109',
          'SE0146',
          'SE0089',
          'SE0121',
          'SE0137',
          'SE000201',
          'SE0103',
          'SE0051',
          'SE0152',
          'SE000206'
        ) then '1'
        when productcode = 'SE0004' and limittag = 'SP6' then '1'
        when productcode = 'SE0008' and limittag in('T4', 'T801') then '1'
        else '0' end as is_air_order,
      row_number() over( partition by a.orderid order by a.processversiontm desc) as rank
    from
      (
        tmp_ordi_predict.dm_full_order_dtl_df_by_day  
      ) a
  ) t
where
  t.rank = 1
  and t.iscancel = 0
  -- and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1h)]'
  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
  -- and t.ordertm <= concat('$[time(yyyy-MM-dd HH)]', ':00:00')
  and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 22:00:00')  -- 手动指定-小时
  
   UNION ALL
select
  t.*
from
  (
    select
      a.*,
      concat(srccitycode, '-', destcitycode) as city_flow,
	  -- case when substr('$[time(yyyyMMddHH)]', 9, 2) = '00' then concat('$[time(yyyyMMdd,-1h)]', '24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour,
	  concat('$[time(yyyyMMdd,-1d)]', '23') AS inc_dayhour,  -- 手动指定-小时
	  case
        when productcode in (
          'SE0001',
          'SE0107',
          'SE0109',
          'SE0146',
          'SE0089',
          'SE0121',
          'SE0137',
          'SE000201',
          'SE0103',
          'SE0051',
          'SE0152',
          'SE000206'
        ) then '1'
        when productcode = 'SE0004' and limittag = 'SP6' then '1'
        when productcode = 'SE0008' and limittag in('T4', 'T801') then '1'
        else '0' end as is_air_order,
      row_number() over( partition by a.orderid order by a.processversiontm desc) as rank
    from
      (
        tmp_ordi_predict.dm_full_order_dtl_df_by_day  
      ) a
  ) t
where
  t.rank = 1
  and t.iscancel = 0
  -- and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1h)]'
  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
  -- and t.ordertm <= concat('$[time(yyyy-MM-dd HH)]', ':00:00')
  and t.ordertm <= concat('$[time(yyyy-MM-dd,-1d)]', ' 23:00:00')  -- 手动指定-小时
  
   UNION ALL
select
  t.*
from
  (
    select
      a.*,
      concat(srccitycode, '-', destcitycode) as city_flow,
	  -- case when substr('$[time(yyyyMMddHH)]', 9, 2) = '00' then concat('$[time(yyyyMMdd,-1h)]', '24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour,
	  concat('$[time(yyyyMMdd,-1d)]', '24') AS inc_dayhour,  -- 手动指定-小时
	  case
        when productcode in (
          'SE0001',
          'SE0107',
          'SE0109',
          'SE0146',
          'SE0089',
          'SE0121',
          'SE0137',
          'SE000201',
          'SE0103',
          'SE0051',
          'SE0152',
          'SE000206'
        ) then '1'
        when productcode = 'SE0004' and limittag = 'SP6' then '1'
        when productcode = 'SE0008' and limittag in('T4', 'T801') then '1'
        else '0' end as is_air_order,
      row_number() over( partition by a.orderid order by a.processversiontm desc) as rank
    from
      (
        tmp_ordi_predict.dm_full_order_dtl_df_by_day
      ) a
  ) t
where
  t.rank = 1
  and t.iscancel = 0
  -- and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1h)]'
  and to_date(t.ordertm) = '$[time(yyyy-MM-dd,-1d)]'  --手动指定-小时所在天
  -- and t.ordertm <= concat('$[time(yyyy-MM-dd HH)]', ':00:00')
  and t.ordertm <= concat('$[time(yyyy-MM-dd)]', ' 00:00:00') ;  -- 手动指定-小时
  
 /**
 step2  分组计数，写入临时备份结果表
**/
set hive.exec.dynamic.partition= true;
set hive.exec.dynamic.partition.mode= nostrict;
set hive.exec.max.dynamic.partitions=2000;  -- 所有执行节点上最多可以多少个动态分区
set hive.exec.max.dynamic.partitions.pernode = 500;

insert overwrite table dm_ordi_predict.dws_cityflow_dynamic_order_hi_backup20230615 partition (inc_day,inc_dayhour)
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
 from tmp_ordi_predict.tmp_oms_cityflow_dynamic_order_1_byday t
 group by to_date(t.ordertm) 
 ,inc_dayhour
 ,city_flow ) al;
 
 
 /*
 CREATE TABLE `dm_ordi_predict.dws_cityflow_dynamic_order_hi_backup20230615`(
  `order_dt` string COMMENT '收件日期',
  `city_flow` string COMMENT '城市流向对',
  `all_order_num` double COMMENT '总订单量',
  `air_order_num` double COMMENT '航空订单量'
) PARTITIONED BY (
  `inc_day` string COMMENT '下单日期yyyymmdd',
  `inc_dayhour` string COMMENT '打点小时yyyymmddHH'
) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
 STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat';
  */