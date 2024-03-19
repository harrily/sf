----------------------开发说明----------------------------
--* 名称:oms订单流向动态统计
--* 任务ID:
--* 说明:oms订单流向动态统计
--* 作者: shenhy
--* 时间: 2023/02/07 
-- 备注：dm_kafka_rdmp.dm_full_order_dtl_df表最早20210520分区
----------------------修改记录----------------------------
--* 修改人   修改时间      修改内容
--* 01431437 20230724   1、航空件调整（1添加新产品 2剔除路由 ） 2、限制processversiontm & 处理srcdeptcode为null
--* 01431437 20230731   1、结果表添加起始业务区  
--* 01431437 20230921   1、临时落表，优化并行度 2、动态判断航空件
----------------------------------------------------------
-- drop table if exists tmp_ordi_predict.tt_waybill_no_to_order_no_tmp1;
-- create table if not exists tmp_ordi_predict.tt_waybill_no_to_order_no_tmp1
-- stored as parquet as
-- select al.waybill_no,al.order_no from 
-- (SELECT waybill_no,order_no ,row_number() over(partition by order_no) as rn 
--     FROM ods_shiva_oms.tt_waybill_no_to_order_no
--     where inc_day between '$[time(yyyyMMdd,-10d)]' and '$[time(yyyyMMdd,-0d)]'
--     and waybill_no != '' and waybill_no is not null
--     and order_no != '' and order_no is not null 
-- ) al where al.rn = 1 
-- ;

-- drop table if exists tmp_ordi_predict.dwd_pis_route_info_v2_df_tmp1;
-- create table if not exists tmp_ordi_predict.dwd_pis_route_info_v2_df_tmp1
-- stored as parquet as
-- select a1.waybillno,a1.route_code from
-- (select waybillno,routeproducttypecode as route_code,row_number() over(partition by waybillno) as rn
--     from dm_ordi_predict.dwd_pis_route_info_v2_df
--     where inc_day between '$[time(yyyyMMdd,-1h)]' and '$[time(yyyyMMdd,+1d)]'
--     and routeproducttypecode != '' and routeproducttypecode is not null
-- ) a1
-- where a1.rn = 1
-- ;

set spark.sql.shuffle.partitions=800;    -- 增加并行度  
set runner.executor.memory=24g;    -- 增加内存
set spark.sql.adaptive.skewJoin.enabled=true;

drop table if exists tmp_ordi_predict.tmp_oms_cityflow_dynamic_order_1;
create table if not exists tmp_ordi_predict.tmp_oms_cityflow_dynamic_order_1 
stored as parquet as
select 
	t.*
	,concat(srccitycode,'-',destcitycode) as city_flow
	,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
	-- 202104之后逻辑
	-- 20230724,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
	-- ,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
	-- 			and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
	-- 	when productcode='SE0004' and limittag = 'SP6' and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
	-- 	when productcode='SE0008' and limittag in('T4','T801') and (nvl(t.route_code,'') not in ('T6','ZT6') or t.route_code is null)  then '1'
	-- else '0' end  as is_air_order
    -- 20230921,动态判断航空件
    ,if(t_hk.product_code is not null 
        and if(t_hk.exclude_route_code is null or t_hk.exclude_route_code = '',true,!array_contains(split(t_hk.exclude_route_code, ',' ), nvl(t.route_code,'')))
        and if(t_hk.limit_tag is null or t_hk.limit_tag = '',true,array_contains(split(t_hk.limit_tag, ',' ), nvl(t.limittag,'')))
        ,'1','0') as is_air_order
from 
    (select 
            t1.* 
            ,row_number() over(partition by t1.orderid ORDER BY if(t1.srcdeptcode = 'null','#',t1.srcdeptcode) desc) AS rn
        from
        (select 
                a.* 
                ,rank() over(partition by a.orderid order by a.processversiontm desc) as rank    
            from 
                (select
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
                            select * from dm_kafka_rdmp.dm_full_order_dtl_df where  inc_day = '$[time(yyyyMMdd,-1h)]'
                            -- 添加处理时间限制  20230724
                            AND from_unixtime(cast(processversiontm AS bigint) / 1000, 'yyyy-MM-dd HH:mm:ss') < '$[time(yyyy-MM-dd HH:00:00)]'
                            AND iscancel = 0
							AND to_date(from_unixtime(cast(cast(ordertmstamp as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss')) = '$[time(yyyy-MM-dd,-1h)]'
							AND from_unixtime(cast(cast(ordertmstamp as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') <=concat('$[time(yyyy-MM-dd HH)]',':00:00') 
                        ) a 
                        left join -- 处理运单为null
                        (
                            tmp_ordi_predict.tt_waybill_no_to_order_no_tmp1
                        ) b 
                        on a.orderno = b.order_no
                    ) a  -- 按小时刷新，使用实时运单表获取路由代码
                    left join 
                    (
                        tmp_ordi_predict.dwd_pis_route_info_v2_df_tmp1
                    ) b on a.waybillno_new = b.waybillno
                ) a   
            ) t1
        where t1.rank=1
        -- and t1.iscancel=0
        -- and to_date(t1.ordertm) ='$[time(yyyy-MM-dd,-1h)]'
        -- and t1.ordertm <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
    ) t
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
        inc_day >= '$[time(yyyyMMdd,-1d)]'
        and type = 1
        and expiry_dt >= '$[time(yyyy-MM-dd)]'
    ) t_in 
    left join 
    dm_ordi_predict.seq_tool t_2   
    on 1=1
    where t_in.rn = 1
) t_hk
on concat(CAST(ceil(RAND()*800) as INT),t.productcode)=t_hk.product_code  -- 主表关联字段，打800以内随机数
where t.rn=1 ; 


drop table if exists tmp_ordi_predict.tmp_oms_cityflow_dynamic_order_2;
create table if not exists tmp_ordi_predict.tmp_oms_cityflow_dynamic_order_2 
stored as parquet as
select 
  to_date(t.ordertm) as order_dt
 ,inc_dayhour
 ,city_flow 
 ,count(orderid) as all_order_num
 ,count(case when is_air_order='1' then orderid end) as air_order_num
 from tmp_ordi_predict.tmp_oms_cityflow_dynamic_order_1 t
 group by to_date(t.ordertm) 
 ,inc_dayhour
 ,city_flow;

/*
drop table dm_ordi_predict.dws_cityflow_dynamic_order_hi;
create table if not exists dm_ordi_predict.dws_cityflow_dynamic_order_hi(
	

	order_dt              string  comment  '收件日期',
    city_flow         string  comment  '城市流向对',
    all_order_num   double     comment  '总订单量',
	air_order_num  double     comment  '航空订单量'
    )
partitioned by (inc_day string comment'下单日期yyyymmdd',inc_dayhour string comment '打点小时yyyymmddHH')
stored as parquet;
*/


set hive.exec.dynamic.partition= true;
set hive.exec.dynamic.partition.mode= nostrict;
set hive.exec.max.dynamic.partitions=2000;  -- 所有执行节点上最多可以多少个动态分区
set hive.exec.max.dynamic.partitions.pernode = 500;

insert overwrite table dm_ordi_predict.dws_cityflow_dynamic_order_hi partition (inc_day,inc_dayhour)
select 
  order_dt
  ,city_flow
  ,nvl(t2.area_code,'') as src_area_code   --添加业务区
  ,nvl(t2.area_name,'') as src_area_name
  ,nvl(t3.area_code,'') as dest_area_code
  ,nvl(t3.area_name,'') as dest_area_name
  ,all_order_num
  ,air_order_num
  ,regexp_replace(order_dt,'-','') as inc_day
  ,inc_dayhour
from tmp_ordi_predict.tmp_oms_cityflow_dynamic_order_2 t1 
left join 
 (select * from
	(select
		city_code   --城市代码
		,area_code  --业务区编码
		,area_name   --业务区名称
		,row_number() over(partition by city_code,area_code,area_name order by inc_day desc) as rn 
	from dm_ordi_predict.dim_city_level_mapping_df 
	where inc_day>='$[time(yyyyMMdd,-1d)]'
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
	where inc_day>='$[time(yyyyMMdd,-1d)]'
	and  if_foreign='0'  -- 筛选国内
	) t where t.rn = 1
 ) t3
 on nvl(split(t1.city_flow,'-')[1],'a')=t3.city_code ;