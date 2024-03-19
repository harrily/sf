/**
    468144/468146 兜底任务
    1、468144/468146 上游失败，使用此任务兜底。
    2、分区示例： 
        inc_day = '20231025'
        hour = '1'  [1 ~ 24] 
    3、	手动传参，指定兜底补录-小时打点数据
        参数示例： 
            参数1 :  ${yyyy_MM_dd_manual} -- 计算日期 (示例:2023-10-25)
            参数2 ： ${H_manual}   	   -- 计算时点 (示例:0,1,2,3,4,5 ... 23 (0~23))
        示例： 需要兜底计算2023-10-25 00:00:01 时点任务， 只需传参 
            $1=2023-10-25 
            $2=0

**/

insert overwrite table tmp_dm_predict.dws_dynamic_cityflow_base_hi_tmp_doudi partition(inc_day,hour)   -- 测试数据
select 
    concat(nvl(pickupcitycode,'#'),'-',nvl(deliverycitycode,'#')) as cityflow,
    pickupday as consignor_date,
    deliveryhqcode as deliver_hq_code,
    nvl(d.area_code,deliveryareacode) as deliver_area_code,
    deliveryfbqcode  as deliver_fbq_code,
    deliverycitycode as deliver_city_code,
    pickuphqcode     as pickup_hq_code,
    nvl(c.area_code,pickupareacode) as pickup_area_code,
    pickupfbqcode    as pickup_fbq_code,
    pickupcitycode   as pickup_city_code,
    -- 拼接时分秒 以及 判断是否是0点。
    sum(if(consignedtime<to_unix_timestamp(if(length('${H_manual}') = 1 ,concat('${yyyy_MM_dd_manual} 0','${H_manual}:00:00'),concat('${yyyy_MM_dd_manual} ','${H_manual}:00:00')),'yyyy-MM-dd HH:mm:ss')*1000 
        AND pickupday=if('${H_manual}' = '0' ,concat(substring(cast(replace('${yyyy_MM_dd_manual}','-','') as int ) - 1,0,4) ,'-',substring(cast(replace('${yyyy_MM_dd_manual}','-','') as int ) - 1,5,2) ,'-',substring(cast(replace('${yyyy_MM_dd_manual}','-','') as int ) - 1,7,2)),'${yyyy_MM_dd_manual}'),meterageweightqty,0)) as pickup_weight,
    sum(if(consignedtime<to_unix_timestamp(if(length('${H_manual}') = 1 ,concat('${yyyy_MM_dd_manual} 0','${H_manual}:00:00'),concat('${yyyy_MM_dd_manual} ','${H_manual}:00:00')),'yyyy-MM-dd HH:mm:ss')*1000 
        AND pickupday=if('${H_manual}' = '0' ,concat(substring(cast(replace('${	}','-','') as int ) - 1,0,4) ,'-',substring(cast(replace('${yyyy_MM_dd_manual}','-','') as int ) - 1,5,2) ,'-',substring(cast(replace('${yyyy_MM_dd_manual}','-','') as int ) - 1,7,2)),'${yyyy_MM_dd_manual}') AND above20kgflag = 'true',meterageweightqty,0)) as pickup_weight_20kg,
    sum(if(consignedtime<to_unix_timestamp(if(length('${H_manual}') = 1 ,concat('${yyyy_MM_dd_manual} 0','${H_manual}:00:00'),concat('${yyyy_MM_dd_manual} ','${H_manual}:00:00')),'yyyy-MM-dd HH:mm:ss')*1000 
        AND pickupday=if('${H_manual}' = '0' ,concat(substring(cast(replace('${yyyy_MM_dd_manual}','-','') as int ) - 1,0,4) ,'-',substring(cast(replace('${yyyy_MM_dd_manual}','-','') as int ) - 1,5,2) ,'-',substring(cast(replace('${yyyy_MM_dd_manual}','-','') as int ) - 1,7,2)),'${yyyy_MM_dd_manual}'),1,0))  as pickup_piece,
    sum(if(consignedtime<to_unix_timestamp(if(length('${H_manual}') = 1 ,concat('${yyyy_MM_dd_manual} 0','${H_manual}:00:00'),concat('${yyyy_MM_dd_manual} ','${H_manual}:00:00')),'yyyy-MM-dd HH:mm:ss')*1000 
        AND pickupday=if('${H_manual}' = '0' ,concat(substring(cast(replace('${yyyy_MM_dd_manual}','-','') as int ) - 1,0,4) ,'-',substring(cast(replace('${yyyy_MM_dd_manual}','-','') as int ) - 1,5,2) ,'-',substring(cast(replace('${yyyy_MM_dd_manual}','-','') as int ) - 1,7,2)),'${yyyy_MM_dd_manual}') AND above20kgflag = 'true',1,0))  as pickup_piece_20kg,
    producttypecode  as product_type_code,
    deliverydeptcode as deliver_dept_code,
    pickupdivisioncode as pickup_division_code,		--收件分部
    pickupdeptcode   as pickup_dept_code,
    deliverydivisioncode as deliver_division_code,	-- 派送分部
    if('${H_manual}' = '0' , cast(replace('${yyyy_MM_dd_manual}','-','') as int ) - 1  ,replace('${yyyy_MM_dd_manual}','-','')) as inc_day,
    if('${H_manual}' = '0' , 24 , '${H_manual}') as hour
from(
    select 
        waybillno,
        pickupdeptcode,
        pickupcitycode,
        pickupareacode,
        pickupfbqcode,
        pickuphqcode,
        deliverydeptcode,
        deliverycitycode,
        deliveryareacode,
        deliveryfbqcode,
        deliveryhqcode,
        producttypecode,
        case
            when incomecode = '1' then '时效'
            when incomecode = '2' then '电商'
            when incomecode = '3' then '国际'
            when incomecode = '4' then '大件'
            when incomecode = '5' then '医药'
            when incomecode = '6' then '其他'
            when incomecode = '8' then '冷运'
            when incomecode = '9' then '丰网'
        end as incomecode,
        consignedtime,			-- 寄件时间
        pickupday,				-- 收件日期
        meterageweightqty,  	-- 计费重量
        above20kgflag,			-- 是否大于20KG(true ,false)
        pickupdivisioncode,		-- 收件分部
        deliverydivisioncode,	-- 派送分部
        row_number()over(partition by waybillno order by versiontm desc) rank
    from dm_kafka_rdmp.dm_rdmp_waybill_simple_dtl_di 
        -- 如果是0点任务，pickupday取前一天日期yyyy-MM-dd
    where pickupday= if('${H_manual}' = '0' ,concat(substring(cast(replace('${yyyy_MM_dd_manual}','-','') as int ) - 1,0,4) ,'-',substring(cast(replace('${yyyy_MM_dd_manual}','-','') as int ) - 1,5,2) ,'-',substring(cast(replace('${yyyy_MM_dd_manual}','-','') as int ) - 1,7,2)),'${yyyy_MM_dd_manual}')
        -- 如果传入时点是0~9 ， 拼接时分秒
    and updatetime< if(length('${H_manual}') = 1 ,concat('${yyyy_MM_dd_manual} 0','${H_manual}:00:00'),concat('${yyyy_MM_dd_manual} ','${H_manual}:00:00'))
        -- 如果是0点任务，pickupday取前一天日期 yyyyMMdd
    and inc_day=if('${H_manual}' = '0' , cast(replace('${yyyy_MM_dd_manual}','-','') as int ) - 1  ,replace('${yyyy_MM_dd_manual}','-',''))
) a
left join (
    select dept_code,
        area_code 
    from dm_ordi_predict.hxh_dim_department_snap 
    where inc_day='$[time(yyyyMMdd,-2d)]'
    and delete_flg='0'
    group by dept_code,
            area_code
) c
on a.pickupdeptcode=c.dept_code
left join (
    select dept_code,
        area_code 
    from dm_ordi_predict.hxh_dim_department_snap 
    where inc_day='$[time(yyyyMMdd,-2d)]'
    and delete_flg='0'
    group by dept_code,
            area_code
) d
on a.deliverydeptcode=d.dept_code
where a.rank=1
and nvl(pickuphqcode,'1')<>'CN39'
and nvl(deliveryhqcode,'1')<>'CN39'
and nvl(incomecode,'1')<>'丰网'
group by 
		pickupday,
		deliveryhqcode,
		nvl(d.area_code,deliveryareacode),
		deliveryfbqcode,
		deliverycitycode,
		pickuphqcode,
		nvl(c.area_code,pickupareacode),
		pickupfbqcode,
		pickupcitycode,
		producttypecode,
		deliverydeptcode,
		pickupdivisioncode,		-- 收件分部
		pickupdeptcode,
		deliverydivisioncode	-- 派送分部	
    

/**
    -- 测试手动传参，动态调整日期格式是否正常 
-- drop table if exists tmp_ordi_predict.dws_dynamic_cityflow_base_hi_tmp_doudi_1;
-- create table if not exists tmp_ordi_predict.dws_dynamic_cityflow_base_hi_tmp_doudi_1 
insert into table tmp_ordi_predict.dws_dynamic_cityflow_base_hi_tmp_doudi_1 
select 
	if(length('${H_manual}') = 1 ,concat('${yyyy_MM_dd_manual} 0','${H_manual}:00:00'),concat('${yyyy_MM_dd_manual} ','${H_manual}:00:00')) as consignedtime,
	if('${H_manual}' = '0' ,concat(substring(cast(replace('${yyyy_MM_dd_manual}','-','') as int ) - 1,0,4) ,'-',substring(cast(replace('${yyyy_MM_dd_manual}','-','') as int ) - 1,5,2) ,'-',substring(cast(replace('${yyyy_MM_dd_manual}','-','') as int ) - 1,7,2)),'${yyyy_MM_dd_manual}') as pickupday,
	if('${H_manual}' = '0' , cast(replace('${yyyy_MM_dd_manual}','-','') as int ) - 1  ,replace('${yyyy_MM_dd_manual}','-','')) as inc_day,
	if('${H_manual}' = '0' , 24 , '${H_manual}') as hour,
	if('${H_manual}' = '0' ,concat(substring(cast(replace('${yyyy_MM_dd_manual}','-','') as int ) - 1,0,4) ,'-',substring(cast(replace('${yyyy_MM_dd_manual}','-','') as int ) - 1,5,2) ,'-',substring(cast(replace('${yyyy_MM_dd_manual}','-','') as int ) - 1,7,2)),'${yyyy_MM_dd_manual}') as pickupday_2,
	if(length('${H_manual}') = 1 ,concat('${yyyy_MM_dd_manual} 0','${H_manual}:00:00'),concat('${yyyy_MM_dd_manual} ','${H_manual}:00:00')) as consignedtime_2,
	if('${H_manual}' = '0' , cast(replace('${yyyy_MM_dd_manual}','-','') as int ) - 1  ,replace('${yyyy_MM_dd_manual}','-','')) as inc_day_2
from dm_ordi_predict.dim_city_level_mapping_df
where inc_day = '20231025'
limit 1 
**/