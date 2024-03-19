/**

	-- 高峰  468144/468146  兜底逻辑，线上版本数（按调度小时执行）
**/	

insert overwrite table dm_ordi_predict.dws_dynamic_cityflow_base_hi partition(inc_day,hour)   -- 测试数据
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
	sum(if(consignedtime<to_unix_timestamp('$[time(yyyy-MM-dd HH:00:00)]','yyyy-MM-dd HH:mm:ss')*1000 AND pickupday='$[time(yyyy-MM-dd,-1h)]',meterageweightqty,0))  as pickup_weight,
	sum(if(consignedtime<to_unix_timestamp('$[time(yyyy-MM-dd HH:00:00)]','yyyy-MM-dd HH:mm:ss')*1000 AND pickupday='$[time(yyyy-MM-dd,-1h)]' AND above20kgflag = 'true',meterageweightqty,0))  as pickup_weight_20kg,
	sum(if(consignedtime<to_unix_timestamp('$[time(yyyy-MM-dd HH:00:00)]','yyyy-MM-dd HH:mm:ss')*1000 AND pickupday='$[time(yyyy-MM-dd,-1h)]',1,0))  as pickup_piece,
	sum(if(consignedtime<to_unix_timestamp('$[time(yyyy-MM-dd HH:00:00)]','yyyy-MM-dd HH:mm:ss')*1000 AND pickupday='$[time(yyyy-MM-dd,-1h)]' AND above20kgflag = 'true',1,0))  as pickup_piece_20kg,
    producttypecode  as product_type_code,
    deliverydeptcode as deliver_dept_code,
    pickupdivisioncode as pickup_division_code,		--收件分部
    pickupdeptcode   as pickup_dept_code,
    deliverydivisioncode as deliver_division_code,	-- 派送分部
	'$[time(yyyyMMdd,-1h)]' as inc_day,
    if('$[time(H)]'='0','24','$[time(H)]') as hour
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
    where pickupday= '$[time(yyyy-MM-dd,-1h)]'
    and updatetime< '$[time(yyyy-MM-dd HH:00:00)]'
    and inc_day='$[time(yyyyMMdd,-1h)]'
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
;
