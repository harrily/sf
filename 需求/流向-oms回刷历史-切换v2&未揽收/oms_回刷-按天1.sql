----------------------开发说明----------------------------
--* 名称:oms订单流向动态统计 -- 回刷历史by天
--* 任务ID:
--* 说明:oms订单流向动态统计-- 回刷历史by天
--* 作者: 01431437
--* 时间: 2024/01/16 
----------------------修改记录----------------------------

----------------------------------------------------------
/**
	1、添加未揽收 
	2、产品code,limit_tag 优先取kafka表，取不到取运单宽表  
		-- oms 补录 20221001 ~ 20221121 air_order_num 都为0 ，解决：产品code,limit_tag 优先取订单，取不到取运单宽表
	3、processversiontm rank优化
	4、去掉订单关联
	5、直接写入治理表，无小时分区，有小时字段。

补录范围：
	20221001 ~ 至今
		20221001 ~ 20230131
		20230201 ~ 20230531
		20230601 ~ 20230930
		20231001 ~ 20240116
**/
-- 刷新历史，关联获取路由代码
drop table if exists tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20240116;
create table if not exists tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20240116 
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
  nvl(productcode,b.product_code) as productcode,	-- 优先取kafka表，取不到取运单宽表
  incomecode,
  lasttime,
  iscancel,
  ispick,  -- 是否揽收 0-未揽收 1-已揽收
  nvl(limittag,b.limit_tag) as limittag,	--  优先取kafka表，取不到取运单宽表
  from_unixtime(cast(cast(processversiontm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss') as processversiontm,
  processversiontm as processversiontm_tmpstamp,	-- 约束时间戳
  b.route_code
from
 (
    select * ,waybillno as waybillno_new from dm_kafka_rdmp.dm_full_order_dtl_df where  inc_day = '$[time(yyyyMMdd,-0d)]'  -- 手动指定-刷新当天
 ) a  -- 刷新历史，使用运单宽表
left join 
(select a1.waybill_no,a1.route_code,a1.product_code,a1.limit_tag from
  (select waybill_no,route_code,product_code,limit_tag,row_number() over(partition by waybill_no) as rn
	from dwd.dwd_waybill_info_dtl_di
	where inc_day between '$[time(yyyyMMdd,-0d)]' and '$[time(yyyyMMdd,+7d)]'      
	and route_code != '' and route_code is not null
  ) a1 where a1.rn = 1
) b on a.waybillno_new = b.waybill_no
  ;
 


set spark.sql.shuffle.partitions=800;    -- 增加并行度  
set runner.executor.memory=24g;    -- 增加内存
set spark.sql.adaptive.skewJoin.enabled=true;

drop table if exists tmp_ordi_predict.tmp_oms_cityflow_dynamic_order_1_tmp20240116;
create table if not exists tmp_ordi_predict.tmp_oms_cityflow_dynamic_order_1_tmp20240116 
stored as parquet as
select t.*
	-- 20230921,动态判断航空件
    ,if(t_hk.product_code is not null 
	and if(t_hk.exclude_route_code is null or t_hk.exclude_route_code = '',true,!array_contains(split(t_hk.exclude_route_code, ',' ), nvl(t.route_code,'')))
	and if(t_hk.limit_tag is null or t_hk.limit_tag = '',true,array_contains(split(t_hk.limit_tag, ',' ), nvl(t.limittag,'')))
	,'1','0') as is_air_order 
from 
(
	select 
		t.*
		,concat(srccitycode,'-',destcitycode) as city_flow
		,concat('$[time(yyyyMMdd,-0d)]', '01') AS inc_dayhour  -- 手动指定-小时
	from 
		(select 
				t1.* 
				,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
			from
			(select 
					a.* 
					,rank() over(partition by a.orderid order by a.processversiontm_tmpstamp desc) as rank    -- 排序按照时间戳
				from 
					(
						select * from tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20240116 
						  where processversiontm < '$[time(yyyy-MM-dd 01:00:00)]'   		-- 手动指定-添加处理时间限制
						  and iscancel=0
						  and to_date(ordertm) = '$[time(yyyy-MM-dd,-0d)]'  	    		-- 手动指定-小时所在天
						  and ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 01:00:00')  -- 手动指定-小时
					) a   
			) t1
			where t1.rank=1
		) t 
		where t.rn=1 
 union all 
	select 
		t.*
		,concat(srccitycode,'-',destcitycode) as city_flow
		,concat('$[time(yyyyMMdd,-0d)]', '02') AS inc_dayhour  -- 手动指定-小时
	from 
		(select 
				t1.* 
				,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
			from
			(select 
					a.* 
					,rank() over(partition by a.orderid order by a.processversiontm_tmpstamp desc) as rank    
				from 
					(
						select * from tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20240116 
						  where processversiontm < '$[time(yyyy-MM-dd 02:00:00)]'   		-- 手动指定-添加处理时间限制
						  and iscancel=0
						  and to_date(ordertm) = '$[time(yyyy-MM-dd,-0d)]'  	    		-- 手动指定-小时所在天
						  and ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 02:00:00')  -- 手动指定-小时
					) a   
			) t1
			where t1.rank=1
		) t 
		where t.rn=1 
 union all 
	select 
		t.*
		,concat(srccitycode,'-',destcitycode) as city_flow
		,concat('$[time(yyyyMMdd,-0d)]', '03') AS inc_dayhour  -- 手动指定-小时
	from 
		(select 
				t1.* 
				,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
			from
			(select 
					a.* 
					,rank() over(partition by a.orderid order by a.processversiontm_tmpstamp desc) as rank    
				from 
					(
						select * from tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20240116 
						  where processversiontm < '$[time(yyyy-MM-dd 03:00:00)]'   		-- 手动指定-添加处理时间限制
						  and iscancel=0
						  and to_date(ordertm) = '$[time(yyyy-MM-dd,-0d)]'  	    		-- 手动指定-小时所在天
						  and ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 03:00:00')  -- 手动指定-小时
					) a   
			) t1
			where t1.rank=1
		) t 
		where t.rn=1
 union all 
	select 
		t.*
		,concat(srccitycode,'-',destcitycode) as city_flow
		,concat('$[time(yyyyMMdd,-0d)]', '04') AS inc_dayhour  -- 手动指定-小时
	from 
		(select 
				t1.* 
				,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
			from
			(select 
					a.* 
					,rank() over(partition by a.orderid order by a.processversiontm_tmpstamp desc) as rank    
				from 
					(
						select * from tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20240116 
						  where processversiontm < '$[time(yyyy-MM-dd 04:00:00)]'   		-- 手动指定-添加处理时间限制
						  and iscancel=0
						  and to_date(ordertm) = '$[time(yyyy-MM-dd,-0d)]'  	    		-- 手动指定-小时所在天
						  and ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 04:00:00')  -- 手动指定-小时
					) a   
			) t1
			where t1.rank=1
		) t 
		where t.rn=1
	 union all 
	select 
		t.*
		,concat(srccitycode,'-',destcitycode) as city_flow
		,concat('$[time(yyyyMMdd,-0d)]', '05') AS inc_dayhour  -- 手动指定-小时
	from 
		(select 
				t1.* 
				,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
			from
			(select 
					a.* 
					,rank() over(partition by a.orderid order by a.processversiontm_tmpstamp desc) as rank    
				from 
					(
						select * from tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20240116 
						  where processversiontm < '$[time(yyyy-MM-dd 05:00:00)]'   		-- 手动指定-添加处理时间限制
						  and iscancel=0
						  and to_date(ordertm) = '$[time(yyyy-MM-dd,-0d)]'  	    		-- 手动指定-小时所在天
						  and ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 05:00:00')  -- 手动指定-小时
					) a   
			) t1
			where t1.rank=1
		) t 
		where t.rn=1
	 union all 
	select 
		t.*
		,concat(srccitycode,'-',destcitycode) as city_flow
		,concat('$[time(yyyyMMdd,-0d)]', '06') AS inc_dayhour  -- 手动指定-小时
	from 
		(select 
				t1.* 
				,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
			from
			(select 
					a.* 
					,rank() over(partition by a.orderid order by a.processversiontm_tmpstamp desc) as rank    
				from 
					(
						select * from tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20240116 
						  where processversiontm < '$[time(yyyy-MM-dd 06:00:00)]'   		-- 手动指定-添加处理时间限制
						  and iscancel=0
						  and to_date(ordertm) = '$[time(yyyy-MM-dd,-0d)]'  	    		-- 手动指定-小时所在天
						  and ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 06:00:00')  -- 手动指定-小时
					) a   
			) t1
			where t1.rank=1
		) t 
		where t.rn=1
	 union all 
	select 
		t.*
		,concat(srccitycode,'-',destcitycode) as city_flow
		,concat('$[time(yyyyMMdd,-0d)]', '07') AS inc_dayhour  -- 手动指定-小时
	from 
		(select 
				t1.* 
				,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
			from
			(select 
					a.* 
					,rank() over(partition by a.orderid order by a.processversiontm_tmpstamp desc) as rank    
				from 
					(
						select * from tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20240116 
						  where processversiontm < '$[time(yyyy-MM-dd 07:00:00)]'   		-- 手动指定-添加处理时间限制
						  and iscancel=0
						  and to_date(ordertm) = '$[time(yyyy-MM-dd,-0d)]'  	    		-- 手动指定-小时所在天
						  and ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 07:00:00')  -- 手动指定-小时
					) a   
			) t1
			where t1.rank=1
		) t 
		where t.rn=1
	 union all 
	select 
		t.*
		,concat(srccitycode,'-',destcitycode) as city_flow
		,concat('$[time(yyyyMMdd,-0d)]', '08') AS inc_dayhour  -- 手动指定-小时
	from 
		(select 
				t1.* 
				,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
			from
			(select 
					a.* 
					,rank() over(partition by a.orderid order by a.processversiontm_tmpstamp desc) as rank    
				from 
					(
						select * from tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20240116 
						  where processversiontm < '$[time(yyyy-MM-dd 08:00:00)]'   		-- 手动指定-添加处理时间限制
						  and iscancel=0
						  and to_date(ordertm) = '$[time(yyyy-MM-dd,-0d)]'  	    		-- 手动指定-小时所在天
						  and ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 08:00:00')  -- 手动指定-小时
					) a   
			) t1
			where t1.rank=1
		) t 
		where t.rn=1
	 union all 
	select 
		t.*
		,concat(srccitycode,'-',destcitycode) as city_flow
		,concat('$[time(yyyyMMdd,-0d)]', '09') AS inc_dayhour  -- 手动指定-小时
	from 
		(select 
				t1.* 
				,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
			from
			(select 
					a.* 
					,rank() over(partition by a.orderid order by a.processversiontm_tmpstamp desc) as rank    
				from 
					(
						select * from tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20240116 
						  where processversiontm < '$[time(yyyy-MM-dd 09:00:00)]'   		-- 手动指定-添加处理时间限制
						  and iscancel=0
						  and to_date(ordertm) = '$[time(yyyy-MM-dd,-0d)]'  	    		-- 手动指定-小时所在天
						  and ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 09:00:00')  -- 手动指定-小时
					) a   
			) t1
			where t1.rank=1
		) t 
		where t.rn=1
	 union all 
	select 
		t.*
		,concat(srccitycode,'-',destcitycode) as city_flow
		,concat('$[time(yyyyMMdd,-0d)]', '10') AS inc_dayhour  -- 手动指定-小时
	from 
		(select 
				t1.* 
				,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
			from
			(select 
					a.* 
					,rank() over(partition by a.orderid order by a.processversiontm_tmpstamp desc) as rank    
				from 
					(
						select * from tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20240116 
						  where processversiontm < '$[time(yyyy-MM-dd 10:00:00)]'   		-- 手动指定-添加处理时间限制
						  and iscancel=0
						  and to_date(ordertm) = '$[time(yyyy-MM-dd,-0d)]'  	    		-- 手动指定-小时所在天
						  and ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 10:00:00')  -- 手动指定-小时
					) a   
			) t1
			where t1.rank=1
		) t 
		where t.rn=1
	 union all 
	select 
		t.*
		,concat(srccitycode,'-',destcitycode) as city_flow
		,concat('$[time(yyyyMMdd,-0d)]', '11') AS inc_dayhour  -- 手动指定-小时
	from 
		(select 
				t1.* 
				,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
			from
			(select 
					a.* 
					,rank() over(partition by a.orderid order by a.processversiontm_tmpstamp desc) as rank    
				from 
					(
						select * from tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20240116 
						  where processversiontm < '$[time(yyyy-MM-dd 11:00:00)]'   		-- 手动指定-添加处理时间限制
						  and iscancel=0
						  and to_date(ordertm) = '$[time(yyyy-MM-dd,-0d)]'  	    		-- 手动指定-小时所在天
						  and ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 11:00:00')  -- 手动指定-小时
					) a   
			) t1
			where t1.rank=1
		) t 
		where t.rn=1
	 union all 
	select 
		t.*
		,concat(srccitycode,'-',destcitycode) as city_flow
		,concat('$[time(yyyyMMdd,-0d)]', '12') AS inc_dayhour  -- 手动指定-小时
	from 
		(select 
				t1.* 
				,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
			from
			(select 
					a.* 
					,rank() over(partition by a.orderid order by a.processversiontm_tmpstamp desc) as rank    
				from 
					(
						select * from tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20240116 
						  where processversiontm < '$[time(yyyy-MM-dd 12:00:00)]'   		-- 手动指定-添加处理时间限制
						  and iscancel=0
						  and to_date(ordertm) = '$[time(yyyy-MM-dd,-0d)]'  	    		-- 手动指定-小时所在天
						  and ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 12:00:00')  -- 手动指定-小时
					) a   
			) t1
			where t1.rank=1
		) t 
		where t.rn=1
	 union all 
	select 
		t.*
		,concat(srccitycode,'-',destcitycode) as city_flow
		,concat('$[time(yyyyMMdd,-0d)]', '13') AS inc_dayhour  -- 手动指定-小时
	from 
		(select 
				t1.* 
				,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
			from
			(select 
					a.* 
					,rank() over(partition by a.orderid order by a.processversiontm_tmpstamp desc) as rank    
				from 
					(
						select * from tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20240116 
						  where processversiontm < '$[time(yyyy-MM-dd 13:00:00)]'   		-- 手动指定-添加处理时间限制
						  and iscancel=0
						  and to_date(ordertm) = '$[time(yyyy-MM-dd,-0d)]'  	    		-- 手动指定-小时所在天
						  and ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 13:00:00')  -- 手动指定-小时
					) a   
			) t1
			where t1.rank=1
		) t 
		where t.rn=1
	 union all 
	select 
		t.*
		,concat(srccitycode,'-',destcitycode) as city_flow
		,concat('$[time(yyyyMMdd,-0d)]', '14') AS inc_dayhour  -- 手动指定-小时
	from 
		(select 
				t1.* 
				,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
			from
			(select 
					a.* 
					,rank() over(partition by a.orderid order by a.processversiontm_tmpstamp desc) as rank    
				from 
					(
						select * from tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20240116 
						  where processversiontm < '$[time(yyyy-MM-dd 14:00:00)]'   		-- 手动指定-添加处理时间限制
						  and iscancel=0
						  and to_date(ordertm) = '$[time(yyyy-MM-dd,-0d)]'  	    		-- 手动指定-小时所在天
						  and ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 14:00:00')  -- 手动指定-小时
					) a   
			) t1
			where t1.rank=1
		) t 
		where t.rn=1
	 union all 
	select 
		t.*
		,concat(srccitycode,'-',destcitycode) as city_flow
		,concat('$[time(yyyyMMdd,-0d)]', '15') AS inc_dayhour  -- 手动指定-小时
	from 
		(select 
				t1.* 
				,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
			from
			(select 
					a.* 
					,rank() over(partition by a.orderid order by a.processversiontm_tmpstamp desc) as rank    
				from 
					(
						select * from tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20240116 
						  where processversiontm < '$[time(yyyy-MM-dd 15:00:00)]'   		-- 手动指定-添加处理时间限制
						  and iscancel=0
						  and to_date(ordertm) = '$[time(yyyy-MM-dd,-0d)]'  	    		-- 手动指定-小时所在天
						  and ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 15:00:00')  -- 手动指定-小时
					) a   
			) t1
			where t1.rank=1
		) t 
		where t.rn=1
	 union all 
	select 
		t.*
		,concat(srccitycode,'-',destcitycode) as city_flow
		,concat('$[time(yyyyMMdd,-0d)]', '16') AS inc_dayhour  -- 手动指定-小时
	from 
		(select 
				t1.* 
				,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
			from
			(select 
					a.* 
					,rank() over(partition by a.orderid order by a.processversiontm_tmpstamp desc) as rank    
				from 
					(
						select * from tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20240116 
						  where processversiontm < '$[time(yyyy-MM-dd 16:00:00)]'   		-- 手动指定-添加处理时间限制
						  and iscancel=0
						  and to_date(ordertm) = '$[time(yyyy-MM-dd,-0d)]'  	    		-- 手动指定-小时所在天
						  and ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 16:00:00')  -- 手动指定-小时
					) a   
			) t1
			where t1.rank=1
		) t 
		where t.rn=1
	 union all 
	select 
		t.*
		,concat(srccitycode,'-',destcitycode) as city_flow
		,concat('$[time(yyyyMMdd,-0d)]', '17') AS inc_dayhour  -- 手动指定-小时
	from 
		(select 
				t1.* 
				,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
			from
			(select 
					a.* 
					,rank() over(partition by a.orderid order by a.processversiontm_tmpstamp desc) as rank    
				from 
					(
						select * from tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20240116 
						  where processversiontm < '$[time(yyyy-MM-dd 17:00:00)]'   		-- 手动指定-添加处理时间限制
						  and iscancel=0
						  and to_date(ordertm) = '$[time(yyyy-MM-dd,-0d)]'  	    		-- 手动指定-小时所在天
						  and ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 17:00:00')  -- 手动指定-小时
					) a   
			) t1
			where t1.rank=1
		) t 
		where t.rn=1
	 union all 
	select 
		t.*
		,concat(srccitycode,'-',destcitycode) as city_flow
		,concat('$[time(yyyyMMdd,-0d)]', '18') AS inc_dayhour  -- 手动指定-小时
	from 
		(select 
				t1.* 
				,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
			from
			(select 
					a.* 
					,rank() over(partition by a.orderid order by a.processversiontm_tmpstamp desc) as rank    
				from 
					(
						select * from tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20240116 
						  where processversiontm < '$[time(yyyy-MM-dd 18:00:00)]'   		-- 手动指定-添加处理时间限制
						  and iscancel=0
						  and to_date(ordertm) = '$[time(yyyy-MM-dd,-0d)]'  	    		-- 手动指定-小时所在天
						  and ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 18:00:00')  -- 手动指定-小时
					) a   
			) t1
			where t1.rank=1
		) t 
		where t.rn=1
	 union all 
	select 
		t.*
		,concat(srccitycode,'-',destcitycode) as city_flow
		,concat('$[time(yyyyMMdd,-0d)]', '19') AS inc_dayhour  -- 手动指定-小时
	from 
		(select 
				t1.* 
				,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
			from
			(select 
					a.* 
					,rank() over(partition by a.orderid order by a.processversiontm_tmpstamp desc) as rank    
				from 
					(
						select * from tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20240116 
						  where processversiontm < '$[time(yyyy-MM-dd 19:00:00)]'   		-- 手动指定-添加处理时间限制
						  and iscancel=0
						  and to_date(ordertm) = '$[time(yyyy-MM-dd,-0d)]'  	    		-- 手动指定-小时所在天
						  and ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 19:00:00')  -- 手动指定-小时
					) a   
			) t1
			where t1.rank=1
		) t 
		where t.rn=1
	 union all 
	select 
		t.*
		,concat(srccitycode,'-',destcitycode) as city_flow
		,concat('$[time(yyyyMMdd,-0d)]', '20') AS inc_dayhour  -- 手动指定-小时
	from 
		(select 
				t1.* 
				,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
			from
			(select 
					a.* 
					,rank() over(partition by a.orderid order by a.processversiontm_tmpstamp desc) as rank    
				from 
					(
						select * from tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20240116 
						  where processversiontm < '$[time(yyyy-MM-dd 20:00:00)]'   		-- 手动指定-添加处理时间限制
						  and iscancel=0
						  and to_date(ordertm) = '$[time(yyyy-MM-dd,-0d)]'  	    		-- 手动指定-小时所在天
						  and ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 20:00:00')  -- 手动指定-小时
					) a   
			) t1
			where t1.rank=1
		) t 
		where t.rn=1
	 union all 
	select 
		t.*
		,concat(srccitycode,'-',destcitycode) as city_flow
		,concat('$[time(yyyyMMdd,-0d)]', '21') AS inc_dayhour  -- 手动指定-小时
	from 
		(select 
				t1.* 
				,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
			from
			(select 
					a.* 
					,rank() over(partition by a.orderid order by a.processversiontm_tmpstamp desc) as rank    
				from 
					(
						select * from tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20240116 
						  where processversiontm < '$[time(yyyy-MM-dd 21:00:00)]'   		-- 手动指定-添加处理时间限制
						  and iscancel=0
						  and to_date(ordertm) = '$[time(yyyy-MM-dd,-0d)]'  	    		-- 手动指定-小时所在天
						  and ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 21:00:00')  -- 手动指定-小时
					) a   
			) t1
			where t1.rank=1
		) t 
		where t.rn=1
	 union all 
	select 
		t.*
		,concat(srccitycode,'-',destcitycode) as city_flow
		,concat('$[time(yyyyMMdd,-0d)]', '22') AS inc_dayhour  -- 手动指定-小时
	from 
		(select 
				t1.* 
				,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
			from
			(select 
					a.* 
					,rank() over(partition by a.orderid order by a.processversiontm_tmpstamp desc) as rank    
				from 
					(
						select * from tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20240116 
						  where processversiontm < '$[time(yyyy-MM-dd 22:00:00)]'   		-- 手动指定-添加处理时间限制
						  and iscancel=0
						  and to_date(ordertm) = '$[time(yyyy-MM-dd,-0d)]'  	    		-- 手动指定-小时所在天
						  and ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 22:00:00')  -- 手动指定-小时
					) a   
			) t1
			where t1.rank=1
		) t 
		where t.rn=1
	 union all 
	select 
		t.*
		,concat(srccitycode,'-',destcitycode) as city_flow
		,concat('$[time(yyyyMMdd,-0d)]', '23') AS inc_dayhour  -- 手动指定-小时
	from 
		(select 
				t1.* 
				,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
			from
			(select 
					a.* 
					,rank() over(partition by a.orderid order by a.processversiontm_tmpstamp desc) as rank    
				from 
					(
						select * from tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20240116 
						  where processversiontm < '$[time(yyyy-MM-dd 23:00:00)]'   		-- 手动指定-添加处理时间限制
						  and iscancel=0
						  and to_date(ordertm) = '$[time(yyyy-MM-dd,-0d)]'  	    		-- 手动指定-小时所在天
						  and ordertm <= concat('$[time(yyyy-MM-dd,-0d)]', ' 23:00:00')  -- 手动指定-小时
					) a   
			) t1
			where t1.rank=1
		) t 
		where t.rn=1
	union all 
	select 
		t.*
		,concat(srccitycode,'-',destcitycode) as city_flow
		,concat('$[time(yyyyMMdd,-0d)]', '24') AS inc_dayhour  -- 手动指定-小时
	from 
		(select 
				t1.* 
				,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
			from
			(select 
					a.* 
					,rank() over(partition by a.orderid order by a.processversiontm_tmpstamp desc) as rank    
				from 
					(
						select * from tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20240116 
						  where processversiontm < concat('$[time(yyyy-MM-dd,+1d)]', ' 00:00:00')  		-- 手动指定-添加处理时间限制
						  and iscancel=0
						  and to_date(ordertm) = '$[time(yyyy-MM-dd,-0d)]'  	    		-- 手动指定-小时所在天
						  and ordertm <= concat('$[time(yyyy-MM-dd,+1d)]', ' 00:00:00')  -- 手动指定-小时
					) a   
			) t1
			where t1.rank=1
		) t 
		where t.rn=1
 )t 
left join
(select 
    concat(t_2.seq,t_in.product_code) as product_code,  -- 辅表均匀膨胀800倍
    t_in.limit_tag,
    t_in.exclude_route_code
 from 
    (select
        product_code,
        sop_label as limit_tag,
        exclude_route_code,
        expiry_dt,
        row_number() over(partition by product_code order by inc_day desc) as rn  
    from
        dm_pass_atp.tm_air_product_config
    where
        inc_day = '20231129' 	-- 手动指定-取最新产品回刷
        and type = 1
        and expiry_dt >= '$[time(yyyy-MM-dd)]'
    ) t_in 
    left join 
    dm_ordi_predict.seq_tool t_2   
    on 1=1
    where t_in.rn = 1
) t_hk
on concat(CAST(ceil(RAND()*800) as INT),t.productcode)=t_hk.product_code  -- 主表关联字段，打800以内随机数

; 


drop table if exists tmp_ordi_predict.tmp_oms_cityflow_dynamic_order_2_tmp20240116;
create table if not exists tmp_ordi_predict.tmp_oms_cityflow_dynamic_order_2_tmp20240116 
stored as parquet as
select 
  to_date(t.ordertm) as order_dt
 ,inc_dayhour
 ,city_flow 
 ,count(orderid) as all_order_num
 ,count(case when is_air_order='1' then orderid end) as air_order_num
 ,count(case when ispick='0' then orderid end) as no_pick_order_num
 ,count(case when is_air_order='1' and ispick='0'then orderid end) as no_pick_air_order_num
 from tmp_ordi_predict.tmp_oms_cityflow_dynamic_order_1_tmp20240116 t
 group by to_date(t.ordertm) 
 ,inc_dayhour
 ,city_flow;


set hive.exec.dynamic.partition= true;
set hive.exec.dynamic.partition.mode= nostrict;
set hive.exec.max.dynamic.partitions=2000;  -- 所有执行节点上最多可以多少个动态分区
set hive.exec.max.dynamic.partitions.pernode = 500;

-- 写入天分区，小时字段。
insert overwrite table dm_ordi_predict.dws_cityflow_dynamic_order_hi_backup20240116 partition (inc_day)
select 
  order_dt
  ,city_flow
  ,nvl(t2.area_code,'') as src_area_code   --添加业务区
  ,nvl(t2.area_name,'') as src_area_name
  ,nvl(t3.area_code,'') as dest_area_code
  ,nvl(t3.area_name,'') as dest_area_name
  ,all_order_num
  ,air_order_num
  ,no_pick_order_num
  ,no_pick_air_order_num
  ,inc_dayhour
  ,regexp_replace(order_dt,'-','') as inc_day
from tmp_ordi_predict.tmp_oms_cityflow_dynamic_order_2_tmp20240116 t1 
left join 
 (select * from
	(select
		city_code   --城市代码
		,area_code  --业务区编码
		,area_name   --业务区名称
		,row_number() over(partition by city_code,area_code,area_name order by inc_day desc) as rn 
	from dm_ordi_predict.dim_city_level_mapping_df 
	where inc_day ='20231128'		-- 手动指定-回刷历史
	and  if_foreign='0'  -- 筛选国内
	) t where t.rn = 1
 ) t2
 on nvl(split(t1.city_flow,'-')[0],'a')=t2.city_code
 left join 
 (select * from
	(select
		city_code   --城市代码
		,area_code  --业务区编码
		,area_name   --业务区名称
		,row_number() over(partition by city_code,area_code,area_name order by inc_day desc) as rn 
	from dm_ordi_predict.dim_city_level_mapping_df 
	where inc_day ='20231128'		-- 手动指定-回刷历史
	and  if_foreign='0'  -- 筛选国内
	) t where t.rn = 1
 ) t3
 on nvl(split(t1.city_flow,'-')[1],'a')=t3.city_code ;
 
 
 /**
CREATE TABLE `dm_ordi_predict.dws_cityflow_dynamic_order_hi_backup20240116`(
  `order_dt` string COMMENT '收件日期',
  `city_flow` string COMMENT '城市流向对',
  `src_area_code` string COMMENT '起始业务区编码',
  `src_area_name` string COMMENT '起始业务区名称',
  `dest_area_code` string COMMENT '目的业务区编码',
  `dest_area_name` string COMMENT '目的业务区名称',
  `all_order_num` double COMMENT '总订单量',
  `air_order_num` double COMMENT '航空订单量',
  `no_pick_order_num` double COMMENT '未揽收订单量',
  `no_pick_air_order_num` double COMMENT '未揽收航空订单量',
  `inc_dayhour` string COMMENT '打点小时yyyymmddHH'
) PARTITIONED BY (`inc_day` string COMMENT '下单日期yyyymmdd') 
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
;
**/