
/**
	-、按天更新，T-1D
	-、更新近20天数据(20230609)
	
	1、新增剔除路由代码规则 + 新增产品SE0153,SE0005
	2、dm_ordi_predict.cf_air_list 切换为dm_ordi_predict.cf_air_list_backup20230627(后续使用回刷数据的原表)
	3、dim.dim_prod_base_info_df 切换  【目前未切换，后续切换】
	4、2021-04之前，is_air_waybill  -- √  上游表只有近20天数据 ，不影响
	5、20210415放开只取母单条件   --  √  上游表只有近20天数据，不影响
	
**/
drop table if exists tmp_dm_predict.rmdp_waybill_basedata_tmp20230629;
create table if not exists tmp_dm_predict.rmdp_waybill_basedata_tmp20230629 
stored as parquet as
select * from dm_kafka_rdmp.rmdp_waybill_basedata
-- where inc_day ='$[time(yyyyMMdd,-1h)]'
where inc_day ='$[time(yyyyMMdd,-1d)]' ; --手动指定


-- set spark.sql.shuffle.partitions=1000; 
set hive.auto.convert.join=true;  -- 预防倾斜

drop table if exists tmp_dm_predict.tmp_dws_hk_dynamic_waybill_1_tmp20230629;
create table if not exists tmp_dm_predict.tmp_dws_hk_dynamic_waybill_1_tmp20230629 
stored as parquet as
select waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,meterageweightqty   --计费重量
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
/*
      ,case   --注意：2021.04.01前规则需要变化 SE0146
		when t1.productcode = 'SE0146' and t1.limittypecode ='T4'       then '1'
		when t1.productcode = 'SE000201' and t1.limittypecode ='T105'       then '1'
		when t1.productcode = 'SE0001'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0107'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0089'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0109'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0008'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0137'       and t1.limittypecode ='T69'    then '1'
		when t1.productcode = 'SE0121'       and t1.limittypecode ='SP619'  then '1'
		when t1.productcode = 'SE002401'     and t1.limittypecode ='T55'    then '1'
		when t1.productcode = 'SE0004'       and t1.limittypecode ='T6'   and t1.limittag = 'SP6' then '1'
else '0' end as is_air_waybill */
-- 20221018修改新航空件逻辑
-- 20230403增加SE0152、SE000206两个航空产品
-- 20230627新增 SE0153,SE0005 两个航空产品  
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005') then '1'
            when productcode='SE0004' and limittag = 'SP6' then '1'
            when productcode='SE0008' and limittag in('T4','T801') then '1'
      else '0' end  as is_air_waybill
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
, concat('$[time(yyyyMMdd,-1d)]', '01') AS inc_dayhour  -- 手动指定
from(select  
        waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,case when meterageweightqty>=5000000 then 0 else meterageweightqty end as meterageweightqty   --计费重量  20230220修改bug
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,case when pickuptm='0' 
            then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
            else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss') 
       end as pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
      ,cancelstatus
      ,row_number() over(partition by waybillno order by processversiontm desc) rn
     from tmp_dm_predict.rmdp_waybill_basedata_tmp20230629 a
    -- where inc_day ='$[time(yyyyMMdd,-1h)]'
	where inc_day ='$[time(yyyyMMdd,-1d)]'  --手动指定
    and to_date(case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
               -- ) ='$[time(yyyy-MM-dd,-1h)]'
			   ) ='$[time(yyyy-MM-dd,-1d)]'  --手动指定
    and (case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
              -- ) <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
			   ) <=concat('$[time(yyyy-MM-dd,-1d)]',' 01:00:00')  --手动指定
     and waybillStatus='1'-- 20210415放开只取母单条件  -- 去除子单  1为母单 2为子单 3为废单
	 -- 202104之后 版本
	 -- 航空产品，剔除'T6','ZT6'数据   -- 20230627新增
     and 
     (
            (
                nvl(a.productcode,'') in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                and (nvl(a.routecode,'') not in ('T6','ZT6') or a.routecode is null) 
            )
        or 
            (
                nvl(a.productcode,'') not in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                or a.productcode is null
            )
     )
     ) t1
where rn=1
and nvl(cancelstatus,'a')<>'1'

union all 
select waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,meterageweightqty   --计费重量
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
/*
      ,case   --注意：2021.04.01前规则需要变化 SE0146
		when t1.productcode = 'SE0146' and t1.limittypecode ='T4'       then '1'
		when t1.productcode = 'SE000201' and t1.limittypecode ='T105'       then '1'
		when t1.productcode = 'SE0001'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0107'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0089'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0109'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0008'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0137'       and t1.limittypecode ='T69'    then '1'
		when t1.productcode = 'SE0121'       and t1.limittypecode ='SP619'  then '1'
		when t1.productcode = 'SE002401'     and t1.limittypecode ='T55'    then '1'
		when t1.productcode = 'SE0004'       and t1.limittypecode ='T6'   and t1.limittag = 'SP6' then '1'
else '0' end as is_air_waybill */
-- 20221018修改新航空件逻辑
-- 20230403增加SE0152、SE000206两个航空产品
-- 20230627新增 SE0153,SE0005 两个航空产品  
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005') then '1'
            when productcode='SE0004' and limittag = 'SP6' then '1'
            when productcode='SE0008' and limittag in('T4','T801') then '1'
      else '0' end  as is_air_waybill
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
, concat('$[time(yyyyMMdd,-1d)]', '02') AS inc_dayhour  -- 手动指定
from(select  
        waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,case when meterageweightqty>=5000000 then 0 else meterageweightqty end as meterageweightqty   --计费重量  20230220修改bug
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,case when pickuptm='0' 
            then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
            else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss') 
       end as pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
      ,cancelstatus
      ,row_number() over(partition by waybillno order by processversiontm desc) rn
     from tmp_dm_predict.rmdp_waybill_basedata_tmp20230629 a
    -- where inc_day ='$[time(yyyyMMdd,-1h)]'
	where inc_day ='$[time(yyyyMMdd,-1d)]'  --手动指定
    and to_date(case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
               -- ) ='$[time(yyyy-MM-dd,-1h)]'
			   ) ='$[time(yyyy-MM-dd,-1d)]'  --手动指定
    and (case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
              -- ) <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
			   ) <=concat('$[time(yyyy-MM-dd,-1d)]',' 02:00:00')  --手动指定
     and waybillStatus='1'-- 20210415放开只取母单条件  -- 去除子单  1为母单 2为子单 3为废单
	 -- 202104之后 版本
	 -- 航空产品，剔除'T6','ZT6'数据   -- 20230627新增
     and 
     (
            (
                nvl(a.productcode,'') in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                and (nvl(a.routecode,'') not in ('T6','ZT6') or a.routecode is null) 
            )
        or 
            (
                nvl(a.productcode,'') not in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                or a.productcode is null
            )
     )
     ) t1
where rn=1
and nvl(cancelstatus,'a')<>'1'

union all 
select waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,meterageweightqty   --计费重量
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
/*
      ,case   --注意：2021.04.01前规则需要变化 SE0146
		when t1.productcode = 'SE0146' and t1.limittypecode ='T4'       then '1'
		when t1.productcode = 'SE000201' and t1.limittypecode ='T105'       then '1'
		when t1.productcode = 'SE0001'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0107'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0089'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0109'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0008'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0137'       and t1.limittypecode ='T69'    then '1'
		when t1.productcode = 'SE0121'       and t1.limittypecode ='SP619'  then '1'
		when t1.productcode = 'SE002401'     and t1.limittypecode ='T55'    then '1'
		when t1.productcode = 'SE0004'       and t1.limittypecode ='T6'   and t1.limittag = 'SP6' then '1'
else '0' end as is_air_waybill */
-- 20221018修改新航空件逻辑
-- 20230403增加SE0152、SE000206两个航空产品
-- 20230627新增 SE0153,SE0005 两个航空产品  
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005') then '1'
            when productcode='SE0004' and limittag = 'SP6' then '1'
            when productcode='SE0008' and limittag in('T4','T801') then '1'
      else '0' end  as is_air_waybill
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
, concat('$[time(yyyyMMdd,-1d)]', '03') AS inc_dayhour  -- 手动指定
from(select  
        waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,case when meterageweightqty>=5000000 then 0 else meterageweightqty end as meterageweightqty   --计费重量  20230220修改bug
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,case when pickuptm='0' 
            then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
            else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss') 
       end as pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
      ,cancelstatus
      ,row_number() over(partition by waybillno order by processversiontm desc) rn
     from tmp_dm_predict.rmdp_waybill_basedata_tmp20230629 a
    -- where inc_day ='$[time(yyyyMMdd,-1h)]'
	where inc_day ='$[time(yyyyMMdd,-1d)]'  --手动指定
    and to_date(case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
               -- ) ='$[time(yyyy-MM-dd,-1h)]'
			   ) ='$[time(yyyy-MM-dd,-1d)]'  --手动指定
    and (case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
              -- ) <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
			   ) <=concat('$[time(yyyy-MM-dd,-1d)]',' 03:00:00')  --手动指定
     and waybillStatus='1'-- 20210415放开只取母单条件  -- 去除子单  1为母单 2为子单 3为废单
	 -- 202104之后 版本
	 -- 航空产品，剔除'T6','ZT6'数据   -- 20230627新增
     and 
     (
            (
                nvl(a.productcode,'') in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                and (nvl(a.routecode,'') not in ('T6','ZT6') or a.routecode is null) 
            )
        or 
            (
                nvl(a.productcode,'') not in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                or a.productcode is null
            )
     )
     ) t1
where rn=1
and nvl(cancelstatus,'a')<>'1'

union all 
select waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,meterageweightqty   --计费重量
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
/*
      ,case   --注意：2021.04.01前规则需要变化 SE0146
		when t1.productcode = 'SE0146' and t1.limittypecode ='T4'       then '1'
		when t1.productcode = 'SE000201' and t1.limittypecode ='T105'       then '1'
		when t1.productcode = 'SE0001'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0107'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0089'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0109'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0008'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0137'       and t1.limittypecode ='T69'    then '1'
		when t1.productcode = 'SE0121'       and t1.limittypecode ='SP619'  then '1'
		when t1.productcode = 'SE002401'     and t1.limittypecode ='T55'    then '1'
		when t1.productcode = 'SE0004'       and t1.limittypecode ='T6'   and t1.limittag = 'SP6' then '1'
else '0' end as is_air_waybill */
-- 20221018修改新航空件逻辑
-- 20230403增加SE0152、SE000206两个航空产品
-- 20230627新增 SE0153,SE0005 两个航空产品  
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005') then '1'
            when productcode='SE0004' and limittag = 'SP6' then '1'
            when productcode='SE0008' and limittag in('T4','T801') then '1'
      else '0' end  as is_air_waybill
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
, concat('$[time(yyyyMMdd,-1d)]', '04') AS inc_dayhour  -- 手动指定
from(select  
        waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,case when meterageweightqty>=5000000 then 0 else meterageweightqty end as meterageweightqty   --计费重量  20230220修改bug
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,case when pickuptm='0' 
            then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
            else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss') 
       end as pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
      ,cancelstatus
      ,row_number() over(partition by waybillno order by processversiontm desc) rn
     from tmp_dm_predict.rmdp_waybill_basedata_tmp20230629 a
    -- where inc_day ='$[time(yyyyMMdd,-1h)]'
	where inc_day ='$[time(yyyyMMdd,-1d)]'  --手动指定
    and to_date(case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
               -- ) ='$[time(yyyy-MM-dd,-1h)]'
			   ) ='$[time(yyyy-MM-dd,-1d)]'  --手动指定
    and (case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
              -- ) <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
			   ) <=concat('$[time(yyyy-MM-dd,-1d)]',' 04:00:00')  --手动指定
     and waybillStatus='1'-- 20210415放开只取母单条件  -- 去除子单  1为母单 2为子单 3为废单
	 -- 202104之后 版本
	 -- 航空产品，剔除'T6','ZT6'数据   -- 20230627新增
     and 
     (
            (
                nvl(a.productcode,'') in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                and (nvl(a.routecode,'') not in ('T6','ZT6') or a.routecode is null) 
            )
        or 
            (
                nvl(a.productcode,'') not in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                or a.productcode is null
            )
     )
     ) t1
where rn=1
and nvl(cancelstatus,'a')<>'1'

union all 
select waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,meterageweightqty   --计费重量
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
/*
      ,case   --注意：2021.04.01前规则需要变化 SE0146
		when t1.productcode = 'SE0146' and t1.limittypecode ='T4'       then '1'
		when t1.productcode = 'SE000201' and t1.limittypecode ='T105'       then '1'
		when t1.productcode = 'SE0001'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0107'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0089'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0109'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0008'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0137'       and t1.limittypecode ='T69'    then '1'
		when t1.productcode = 'SE0121'       and t1.limittypecode ='SP619'  then '1'
		when t1.productcode = 'SE002401'     and t1.limittypecode ='T55'    then '1'
		when t1.productcode = 'SE0004'       and t1.limittypecode ='T6'   and t1.limittag = 'SP6' then '1'
else '0' end as is_air_waybill */
-- 20221018修改新航空件逻辑
-- 20230403增加SE0152、SE000206两个航空产品
-- 20230627新增 SE0153,SE0005 两个航空产品  
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005') then '1'
            when productcode='SE0004' and limittag = 'SP6' then '1'
            when productcode='SE0008' and limittag in('T4','T801') then '1'
      else '0' end  as is_air_waybill
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
, concat('$[time(yyyyMMdd,-1d)]', '05') AS inc_dayhour  -- 手动指定
from(select  
        waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,case when meterageweightqty>=5000000 then 0 else meterageweightqty end as meterageweightqty   --计费重量  20230220修改bug
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,case when pickuptm='0' 
            then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
            else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss') 
       end as pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
      ,cancelstatus
      ,row_number() over(partition by waybillno order by processversiontm desc) rn
     from tmp_dm_predict.rmdp_waybill_basedata_tmp20230629 a
    -- where inc_day ='$[time(yyyyMMdd,-1h)]'
	where inc_day ='$[time(yyyyMMdd,-1d)]'  --手动指定
    and to_date(case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
               -- ) ='$[time(yyyy-MM-dd,-1h)]'
			   ) ='$[time(yyyy-MM-dd,-1d)]'  --手动指定
    and (case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
              -- ) <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
			   ) <=concat('$[time(yyyy-MM-dd,-1d)]',' 05:00:00')  --手动指定
     and waybillStatus='1'-- 20210415放开只取母单条件  -- 去除子单  1为母单 2为子单 3为废单
	 -- 202104之后 版本
	 -- 航空产品，剔除'T6','ZT6'数据   -- 20230627新增
     and 
     (
            (
                nvl(a.productcode,'') in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                and (nvl(a.routecode,'') not in ('T6','ZT6') or a.routecode is null) 
            )
        or 
            (
                nvl(a.productcode,'') not in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                or a.productcode is null
            )
     )
     ) t1
where rn=1
and nvl(cancelstatus,'a')<>'1'

union all 
select waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,meterageweightqty   --计费重量
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
/*
      ,case   --注意：2021.04.01前规则需要变化 SE0146
		when t1.productcode = 'SE0146' and t1.limittypecode ='T4'       then '1'
		when t1.productcode = 'SE000201' and t1.limittypecode ='T105'       then '1'
		when t1.productcode = 'SE0001'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0107'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0089'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0109'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0008'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0137'       and t1.limittypecode ='T69'    then '1'
		when t1.productcode = 'SE0121'       and t1.limittypecode ='SP619'  then '1'
		when t1.productcode = 'SE002401'     and t1.limittypecode ='T55'    then '1'
		when t1.productcode = 'SE0004'       and t1.limittypecode ='T6'   and t1.limittag = 'SP6' then '1'
else '0' end as is_air_waybill */
-- 20221018修改新航空件逻辑
-- 20230403增加SE0152、SE000206两个航空产品
-- 20230627新增 SE0153,SE0005 两个航空产品  
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005') then '1'
            when productcode='SE0004' and limittag = 'SP6' then '1'
            when productcode='SE0008' and limittag in('T4','T801') then '1'
      else '0' end  as is_air_waybill
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
, concat('$[time(yyyyMMdd,-1d)]', '06') AS inc_dayhour  -- 手动指定
from(select  
        waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,case when meterageweightqty>=5000000 then 0 else meterageweightqty end as meterageweightqty   --计费重量  20230220修改bug
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,case when pickuptm='0' 
            then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
            else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss') 
       end as pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
      ,cancelstatus
      ,row_number() over(partition by waybillno order by processversiontm desc) rn
     from tmp_dm_predict.rmdp_waybill_basedata_tmp20230629 a
    -- where inc_day ='$[time(yyyyMMdd,-1h)]'
	where inc_day ='$[time(yyyyMMdd,-1d)]'  --手动指定
    and to_date(case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
               -- ) ='$[time(yyyy-MM-dd,-1h)]'
			   ) ='$[time(yyyy-MM-dd,-1d)]'  --手动指定
    and (case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
              -- ) <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
			   ) <=concat('$[time(yyyy-MM-dd,-1d)]',' 06:00:00')  --手动指定
     and waybillStatus='1'-- 20210415放开只取母单条件  -- 去除子单  1为母单 2为子单 3为废单
	 -- 202104之后 版本
	 -- 航空产品，剔除'T6','ZT6'数据   -- 20230627新增
     and 
     (
            (
                nvl(a.productcode,'') in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                and (nvl(a.routecode,'') not in ('T6','ZT6') or a.routecode is null) 
            )
        or 
            (
                nvl(a.productcode,'') not in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                or a.productcode is null
            )
     )
     ) t1
where rn=1
and nvl(cancelstatus,'a')<>'1'

union all 
select waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,meterageweightqty   --计费重量
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
/*
      ,case   --注意：2021.04.01前规则需要变化 SE0146
		when t1.productcode = 'SE0146' and t1.limittypecode ='T4'       then '1'
		when t1.productcode = 'SE000201' and t1.limittypecode ='T105'       then '1'
		when t1.productcode = 'SE0001'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0107'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0089'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0109'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0008'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0137'       and t1.limittypecode ='T69'    then '1'
		when t1.productcode = 'SE0121'       and t1.limittypecode ='SP619'  then '1'
		when t1.productcode = 'SE002401'     and t1.limittypecode ='T55'    then '1'
		when t1.productcode = 'SE0004'       and t1.limittypecode ='T6'   and t1.limittag = 'SP6' then '1'
else '0' end as is_air_waybill */
-- 20221018修改新航空件逻辑
-- 20230403增加SE0152、SE000206两个航空产品
-- 20230627新增 SE0153,SE0005 两个航空产品  
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005') then '1'
            when productcode='SE0004' and limittag = 'SP6' then '1'
            when productcode='SE0008' and limittag in('T4','T801') then '1'
      else '0' end  as is_air_waybill
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
, concat('$[time(yyyyMMdd,-1d)]', '07') AS inc_dayhour  -- 手动指定
from(select  
        waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,case when meterageweightqty>=5000000 then 0 else meterageweightqty end as meterageweightqty   --计费重量  20230220修改bug
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,case when pickuptm='0' 
            then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
            else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss') 
       end as pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
      ,cancelstatus
      ,row_number() over(partition by waybillno order by processversiontm desc) rn
     from tmp_dm_predict.rmdp_waybill_basedata_tmp20230629 a
    -- where inc_day ='$[time(yyyyMMdd,-1h)]'
	where inc_day ='$[time(yyyyMMdd,-1d)]'  --手动指定
    and to_date(case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
               -- ) ='$[time(yyyy-MM-dd,-1h)]'
			   ) ='$[time(yyyy-MM-dd,-1d)]'  --手动指定
    and (case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
              -- ) <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
			   ) <=concat('$[time(yyyy-MM-dd,-1d)]',' 07:00:00')  --手动指定
     and waybillStatus='1'-- 20210415放开只取母单条件  -- 去除子单  1为母单 2为子单 3为废单
	 -- 202104之后 版本
	 -- 航空产品，剔除'T6','ZT6'数据   -- 20230627新增
     and 
     (
            (
                nvl(a.productcode,'') in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                and (nvl(a.routecode,'') not in ('T6','ZT6') or a.routecode is null) 
            )
        or 
            (
                nvl(a.productcode,'') not in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                or a.productcode is null
            )
     )
     ) t1
where rn=1
and nvl(cancelstatus,'a')<>'1'

union all 
select waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,meterageweightqty   --计费重量
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
/*
      ,case   --注意：2021.04.01前规则需要变化 SE0146
		when t1.productcode = 'SE0146' and t1.limittypecode ='T4'       then '1'
		when t1.productcode = 'SE000201' and t1.limittypecode ='T105'       then '1'
		when t1.productcode = 'SE0001'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0107'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0089'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0109'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0008'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0137'       and t1.limittypecode ='T69'    then '1'
		when t1.productcode = 'SE0121'       and t1.limittypecode ='SP619'  then '1'
		when t1.productcode = 'SE002401'     and t1.limittypecode ='T55'    then '1'
		when t1.productcode = 'SE0004'       and t1.limittypecode ='T6'   and t1.limittag = 'SP6' then '1'
else '0' end as is_air_waybill */
-- 20221018修改新航空件逻辑
-- 20230403增加SE0152、SE000206两个航空产品
-- 20230627新增 SE0153,SE0005 两个航空产品  
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005') then '1'
            when productcode='SE0004' and limittag = 'SP6' then '1'
            when productcode='SE0008' and limittag in('T4','T801') then '1'
      else '0' end  as is_air_waybill
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
, concat('$[time(yyyyMMdd,-1d)]', '08') AS inc_dayhour  -- 手动指定
from(select  
        waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,case when meterageweightqty>=5000000 then 0 else meterageweightqty end as meterageweightqty   --计费重量  20230220修改bug
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,case when pickuptm='0' 
            then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
            else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss') 
       end as pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
      ,cancelstatus
      ,row_number() over(partition by waybillno order by processversiontm desc) rn
     from tmp_dm_predict.rmdp_waybill_basedata_tmp20230629 a
    -- where inc_day ='$[time(yyyyMMdd,-1h)]'
	where inc_day ='$[time(yyyyMMdd,-1d)]'  --手动指定
    and to_date(case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
               -- ) ='$[time(yyyy-MM-dd,-1h)]'
			   ) ='$[time(yyyy-MM-dd,-1d)]'  --手动指定
    and (case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
              -- ) <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
			   ) <=concat('$[time(yyyy-MM-dd,-1d)]',' 08:00:00')  --手动指定
     and waybillStatus='1'-- 20210415放开只取母单条件  -- 去除子单  1为母单 2为子单 3为废单
	 -- 202104之后 版本
	 -- 航空产品，剔除'T6','ZT6'数据   -- 20230627新增
     and 
     (
            (
                nvl(a.productcode,'') in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                and (nvl(a.routecode,'') not in ('T6','ZT6') or a.routecode is null) 
            )
        or 
            (
                nvl(a.productcode,'') not in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                or a.productcode is null
            )
     )
     ) t1
where rn=1
and nvl(cancelstatus,'a')<>'1'

union all 
select waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,meterageweightqty   --计费重量
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
/*
      ,case   --注意：2021.04.01前规则需要变化 SE0146
		when t1.productcode = 'SE0146' and t1.limittypecode ='T4'       then '1'
		when t1.productcode = 'SE000201' and t1.limittypecode ='T105'       then '1'
		when t1.productcode = 'SE0001'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0107'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0089'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0109'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0008'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0137'       and t1.limittypecode ='T69'    then '1'
		when t1.productcode = 'SE0121'       and t1.limittypecode ='SP619'  then '1'
		when t1.productcode = 'SE002401'     and t1.limittypecode ='T55'    then '1'
		when t1.productcode = 'SE0004'       and t1.limittypecode ='T6'   and t1.limittag = 'SP6' then '1'
else '0' end as is_air_waybill */
-- 20221018修改新航空件逻辑
-- 20230403增加SE0152、SE000206两个航空产品
-- 20230627新增 SE0153,SE0005 两个航空产品  
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005') then '1'
            when productcode='SE0004' and limittag = 'SP6' then '1'
            when productcode='SE0008' and limittag in('T4','T801') then '1'
      else '0' end  as is_air_waybill
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
, concat('$[time(yyyyMMdd,-1d)]', '09') AS inc_dayhour  -- 手动指定
from(select  
        waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,case when meterageweightqty>=5000000 then 0 else meterageweightqty end as meterageweightqty   --计费重量  20230220修改bug
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,case when pickuptm='0' 
            then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
            else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss') 
       end as pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
      ,cancelstatus
      ,row_number() over(partition by waybillno order by processversiontm desc) rn
     from tmp_dm_predict.rmdp_waybill_basedata_tmp20230629 a
    -- where inc_day ='$[time(yyyyMMdd,-1h)]'
	where inc_day ='$[time(yyyyMMdd,-1d)]'  --手动指定
    and to_date(case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
               -- ) ='$[time(yyyy-MM-dd,-1h)]'
			   ) ='$[time(yyyy-MM-dd,-1d)]'  --手动指定
    and (case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
              -- ) <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
			   ) <=concat('$[time(yyyy-MM-dd,-1d)]',' 09:00:00')  --手动指定
     and waybillStatus='1'-- 20210415放开只取母单条件  -- 去除子单  1为母单 2为子单 3为废单
	 -- 202104之后 版本
	 -- 航空产品，剔除'T6','ZT6'数据   -- 20230627新增
     and 
     (
            (
                nvl(a.productcode,'') in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                and (nvl(a.routecode,'') not in ('T6','ZT6') or a.routecode is null) 
            )
        or 
            (
                nvl(a.productcode,'') not in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                or a.productcode is null
            )
     )
     ) t1
where rn=1
and nvl(cancelstatus,'a')<>'1'

union all 
select waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,meterageweightqty   --计费重量
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
/*
      ,case   --注意：2021.04.01前规则需要变化 SE0146
		when t1.productcode = 'SE0146' and t1.limittypecode ='T4'       then '1'
		when t1.productcode = 'SE000201' and t1.limittypecode ='T105'       then '1'
		when t1.productcode = 'SE0001'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0107'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0089'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0109'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0008'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0137'       and t1.limittypecode ='T69'    then '1'
		when t1.productcode = 'SE0121'       and t1.limittypecode ='SP619'  then '1'
		when t1.productcode = 'SE002401'     and t1.limittypecode ='T55'    then '1'
		when t1.productcode = 'SE0004'       and t1.limittypecode ='T6'   and t1.limittag = 'SP6' then '1'
else '0' end as is_air_waybill */
-- 20221018修改新航空件逻辑
-- 20230403增加SE0152、SE000206两个航空产品
-- 20230627新增 SE0153,SE0005 两个航空产品  
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005') then '1'
            when productcode='SE0004' and limittag = 'SP6' then '1'
            when productcode='SE0008' and limittag in('T4','T801') then '1'
      else '0' end  as is_air_waybill
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
, concat('$[time(yyyyMMdd,-1d)]', '10') AS inc_dayhour  -- 手动指定
from(select  
        waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,case when meterageweightqty>=5000000 then 0 else meterageweightqty end as meterageweightqty   --计费重量  20230220修改bug
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,case when pickuptm='0' 
            then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
            else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss') 
       end as pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
      ,cancelstatus
      ,row_number() over(partition by waybillno order by processversiontm desc) rn
     from tmp_dm_predict.rmdp_waybill_basedata_tmp20230629 a
    -- where inc_day ='$[time(yyyyMMdd,-1h)]'
	where inc_day ='$[time(yyyyMMdd,-1d)]'  --手动指定
    and to_date(case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
               -- ) ='$[time(yyyy-MM-dd,-1h)]'
			   ) ='$[time(yyyy-MM-dd,-1d)]'  --手动指定
    and (case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
              -- ) <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
			   ) <=concat('$[time(yyyy-MM-dd,-1d)]',' 10:00:00')  --手动指定
     and waybillStatus='1'-- 20210415放开只取母单条件  -- 去除子单  1为母单 2为子单 3为废单
	 -- 202104之后 版本
	 -- 航空产品，剔除'T6','ZT6'数据   -- 20230627新增
     and 
     (
            (
                nvl(a.productcode,'') in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                and (nvl(a.routecode,'') not in ('T6','ZT6') or a.routecode is null) 
            )
        or 
            (
                nvl(a.productcode,'') not in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                or a.productcode is null
            )
     )
     ) t1
where rn=1
and nvl(cancelstatus,'a')<>'1'

union all 
select waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,meterageweightqty   --计费重量
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
/*
      ,case   --注意：2021.04.01前规则需要变化 SE0146
		when t1.productcode = 'SE0146' and t1.limittypecode ='T4'       then '1'
		when t1.productcode = 'SE000201' and t1.limittypecode ='T105'       then '1'
		when t1.productcode = 'SE0001'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0107'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0089'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0109'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0008'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0137'       and t1.limittypecode ='T69'    then '1'
		when t1.productcode = 'SE0121'       and t1.limittypecode ='SP619'  then '1'
		when t1.productcode = 'SE002401'     and t1.limittypecode ='T55'    then '1'
		when t1.productcode = 'SE0004'       and t1.limittypecode ='T6'   and t1.limittag = 'SP6' then '1'
else '0' end as is_air_waybill */
-- 20221018修改新航空件逻辑
-- 20230403增加SE0152、SE000206两个航空产品
-- 20230627新增 SE0153,SE0005 两个航空产品  
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005') then '1'
            when productcode='SE0004' and limittag = 'SP6' then '1'
            when productcode='SE0008' and limittag in('T4','T801') then '1'
      else '0' end  as is_air_waybill
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
, concat('$[time(yyyyMMdd,-1d)]', '11') AS inc_dayhour  -- 手动指定
from(select  
        waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,case when meterageweightqty>=5000000 then 0 else meterageweightqty end as meterageweightqty   --计费重量  20230220修改bug
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,case when pickuptm='0' 
            then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
            else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss') 
       end as pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
      ,cancelstatus
      ,row_number() over(partition by waybillno order by processversiontm desc) rn
     from tmp_dm_predict.rmdp_waybill_basedata_tmp20230629 a
    -- where inc_day ='$[time(yyyyMMdd,-1h)]'
	where inc_day ='$[time(yyyyMMdd,-1d)]'  --手动指定
    and to_date(case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
               -- ) ='$[time(yyyy-MM-dd,-1h)]'
			   ) ='$[time(yyyy-MM-dd,-1d)]'  --手动指定
    and (case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
              -- ) <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
			   ) <=concat('$[time(yyyy-MM-dd,-1d)]',' 11:00:00')  --手动指定
     and waybillStatus='1'-- 20210415放开只取母单条件  -- 去除子单  1为母单 2为子单 3为废单
	 -- 202104之后 版本
	 -- 航空产品，剔除'T6','ZT6'数据   -- 20230627新增
     and 
     (
            (
                nvl(a.productcode,'') in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                and (nvl(a.routecode,'') not in ('T6','ZT6') or a.routecode is null) 
            )
        or 
            (
                nvl(a.productcode,'') not in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                or a.productcode is null
            )
     )
     ) t1
where rn=1
and nvl(cancelstatus,'a')<>'1'

union all 
select waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,meterageweightqty   --计费重量
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
/*
      ,case   --注意：2021.04.01前规则需要变化 SE0146
		when t1.productcode = 'SE0146' and t1.limittypecode ='T4'       then '1'
		when t1.productcode = 'SE000201' and t1.limittypecode ='T105'       then '1'
		when t1.productcode = 'SE0001'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0107'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0089'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0109'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0008'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0137'       and t1.limittypecode ='T69'    then '1'
		when t1.productcode = 'SE0121'       and t1.limittypecode ='SP619'  then '1'
		when t1.productcode = 'SE002401'     and t1.limittypecode ='T55'    then '1'
		when t1.productcode = 'SE0004'       and t1.limittypecode ='T6'   and t1.limittag = 'SP6' then '1'
else '0' end as is_air_waybill */
-- 20221018修改新航空件逻辑
-- 20230403增加SE0152、SE000206两个航空产品
-- 20230627新增 SE0153,SE0005 两个航空产品  
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005') then '1'
            when productcode='SE0004' and limittag = 'SP6' then '1'
            when productcode='SE0008' and limittag in('T4','T801') then '1'
      else '0' end  as is_air_waybill
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
, concat('$[time(yyyyMMdd,-1d)]', '12') AS inc_dayhour  -- 手动指定
from(select  
        waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,case when meterageweightqty>=5000000 then 0 else meterageweightqty end as meterageweightqty   --计费重量  20230220修改bug
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,case when pickuptm='0' 
            then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
            else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss') 
       end as pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
      ,cancelstatus
      ,row_number() over(partition by waybillno order by processversiontm desc) rn
     from tmp_dm_predict.rmdp_waybill_basedata_tmp20230629 a
    -- where inc_day ='$[time(yyyyMMdd,-1h)]'
	where inc_day ='$[time(yyyyMMdd,-1d)]'  --手动指定
    and to_date(case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
               -- ) ='$[time(yyyy-MM-dd,-1h)]'
			   ) ='$[time(yyyy-MM-dd,-1d)]'  --手动指定
    and (case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
              -- ) <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
			   ) <=concat('$[time(yyyy-MM-dd,-1d)]',' 12:00:00')  --手动指定
     and waybillStatus='1'-- 20210415放开只取母单条件  -- 去除子单  1为母单 2为子单 3为废单
	 -- 202104之后 版本
	 -- 航空产品，剔除'T6','ZT6'数据   -- 20230627新增
     and 
     (
            (
                nvl(a.productcode,'') in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                and (nvl(a.routecode,'') not in ('T6','ZT6') or a.routecode is null) 
            )
        or 
            (
                nvl(a.productcode,'') not in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                or a.productcode is null
            )
     )
     ) t1
where rn=1
and nvl(cancelstatus,'a')<>'1'

union all 
select waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,meterageweightqty   --计费重量
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
/*
      ,case   --注意：2021.04.01前规则需要变化 SE0146
		when t1.productcode = 'SE0146' and t1.limittypecode ='T4'       then '1'
		when t1.productcode = 'SE000201' and t1.limittypecode ='T105'       then '1'
		when t1.productcode = 'SE0001'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0107'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0089'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0109'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0008'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0137'       and t1.limittypecode ='T69'    then '1'
		when t1.productcode = 'SE0121'       and t1.limittypecode ='SP619'  then '1'
		when t1.productcode = 'SE002401'     and t1.limittypecode ='T55'    then '1'
		when t1.productcode = 'SE0004'       and t1.limittypecode ='T6'   and t1.limittag = 'SP6' then '1'
else '0' end as is_air_waybill */
-- 20221018修改新航空件逻辑
-- 20230403增加SE0152、SE000206两个航空产品
-- 20230627新增 SE0153,SE0005 两个航空产品  
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005') then '1'
            when productcode='SE0004' and limittag = 'SP6' then '1'
            when productcode='SE0008' and limittag in('T4','T801') then '1'
      else '0' end  as is_air_waybill
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
, concat('$[time(yyyyMMdd,-1d)]', '13') AS inc_dayhour  -- 手动指定
from(select  
        waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,case when meterageweightqty>=5000000 then 0 else meterageweightqty end as meterageweightqty   --计费重量  20230220修改bug
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,case when pickuptm='0' 
            then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
            else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss') 
       end as pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
      ,cancelstatus
      ,row_number() over(partition by waybillno order by processversiontm desc) rn
     from tmp_dm_predict.rmdp_waybill_basedata_tmp20230629 a
    -- where inc_day ='$[time(yyyyMMdd,-1h)]'
	where inc_day ='$[time(yyyyMMdd,-1d)]'  --手动指定
    and to_date(case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
               -- ) ='$[time(yyyy-MM-dd,-1h)]'
			   ) ='$[time(yyyy-MM-dd,-1d)]'  --手动指定
    and (case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
              -- ) <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
			   ) <=concat('$[time(yyyy-MM-dd,-1d)]',' 13:00:00')  --手动指定
     and waybillStatus='1'-- 20210415放开只取母单条件  -- 去除子单  1为母单 2为子单 3为废单
	 -- 202104之后 版本
	 -- 航空产品，剔除'T6','ZT6'数据   -- 20230627新增
     and 
     (
            (
                nvl(a.productcode,'') in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                and (nvl(a.routecode,'') not in ('T6','ZT6') or a.routecode is null) 
            )
        or 
            (
                nvl(a.productcode,'') not in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                or a.productcode is null
            )
     )
     ) t1
where rn=1
and nvl(cancelstatus,'a')<>'1'

union all 
select waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,meterageweightqty   --计费重量
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
/*
      ,case   --注意：2021.04.01前规则需要变化 SE0146
		when t1.productcode = 'SE0146' and t1.limittypecode ='T4'       then '1'
		when t1.productcode = 'SE000201' and t1.limittypecode ='T105'       then '1'
		when t1.productcode = 'SE0001'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0107'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0089'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0109'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0008'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0137'       and t1.limittypecode ='T69'    then '1'
		when t1.productcode = 'SE0121'       and t1.limittypecode ='SP619'  then '1'
		when t1.productcode = 'SE002401'     and t1.limittypecode ='T55'    then '1'
		when t1.productcode = 'SE0004'       and t1.limittypecode ='T6'   and t1.limittag = 'SP6' then '1'
else '0' end as is_air_waybill */
-- 20221018修改新航空件逻辑
-- 20230403增加SE0152、SE000206两个航空产品
-- 20230627新增 SE0153,SE0005 两个航空产品  
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005') then '1'
            when productcode='SE0004' and limittag = 'SP6' then '1'
            when productcode='SE0008' and limittag in('T4','T801') then '1'
      else '0' end  as is_air_waybill
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
, concat('$[time(yyyyMMdd,-1d)]', '14') AS inc_dayhour  -- 手动指定
from(select  
        waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,case when meterageweightqty>=5000000 then 0 else meterageweightqty end as meterageweightqty   --计费重量  20230220修改bug
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,case when pickuptm='0' 
            then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
            else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss') 
       end as pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
      ,cancelstatus
      ,row_number() over(partition by waybillno order by processversiontm desc) rn
     from tmp_dm_predict.rmdp_waybill_basedata_tmp20230629 a
    -- where inc_day ='$[time(yyyyMMdd,-1h)]'
	where inc_day ='$[time(yyyyMMdd,-1d)]'  --手动指定
    and to_date(case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
               -- ) ='$[time(yyyy-MM-dd,-1h)]'
			   ) ='$[time(yyyy-MM-dd,-1d)]'  --手动指定
    and (case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
              -- ) <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
			   ) <=concat('$[time(yyyy-MM-dd,-1d)]',' 14:00:00')  --手动指定
     and waybillStatus='1'-- 20210415放开只取母单条件  -- 去除子单  1为母单 2为子单 3为废单
	 -- 202104之后 版本
	 -- 航空产品，剔除'T6','ZT6'数据   -- 20230627新增
     and 
     (
            (
                nvl(a.productcode,'') in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                and (nvl(a.routecode,'') not in ('T6','ZT6') or a.routecode is null) 
            )
        or 
            (
                nvl(a.productcode,'') not in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                or a.productcode is null
            )
     )
     ) t1
where rn=1
and nvl(cancelstatus,'a')<>'1'

union all 
select waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,meterageweightqty   --计费重量
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
/*
      ,case   --注意：2021.04.01前规则需要变化 SE0146
		when t1.productcode = 'SE0146' and t1.limittypecode ='T4'       then '1'
		when t1.productcode = 'SE000201' and t1.limittypecode ='T105'       then '1'
		when t1.productcode = 'SE0001'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0107'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0089'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0109'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0008'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0137'       and t1.limittypecode ='T69'    then '1'
		when t1.productcode = 'SE0121'       and t1.limittypecode ='SP619'  then '1'
		when t1.productcode = 'SE002401'     and t1.limittypecode ='T55'    then '1'
		when t1.productcode = 'SE0004'       and t1.limittypecode ='T6'   and t1.limittag = 'SP6' then '1'
else '0' end as is_air_waybill */
-- 20221018修改新航空件逻辑
-- 20230403增加SE0152、SE000206两个航空产品
-- 20230627新增 SE0153,SE0005 两个航空产品  
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005') then '1'
            when productcode='SE0004' and limittag = 'SP6' then '1'
            when productcode='SE0008' and limittag in('T4','T801') then '1'
      else '0' end  as is_air_waybill
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
, concat('$[time(yyyyMMdd,-1d)]', '15') AS inc_dayhour  -- 手动指定
from(select  
        waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,case when meterageweightqty>=5000000 then 0 else meterageweightqty end as meterageweightqty   --计费重量  20230220修改bug
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,case when pickuptm='0' 
            then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
            else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss') 
       end as pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
      ,cancelstatus
      ,row_number() over(partition by waybillno order by processversiontm desc) rn
     from tmp_dm_predict.rmdp_waybill_basedata_tmp20230629 a
    -- where inc_day ='$[time(yyyyMMdd,-1h)]'
	where inc_day ='$[time(yyyyMMdd,-1d)]'  --手动指定
    and to_date(case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
               -- ) ='$[time(yyyy-MM-dd,-1h)]'
			   ) ='$[time(yyyy-MM-dd,-1d)]'  --手动指定
    and (case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
              -- ) <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
			   ) <=concat('$[time(yyyy-MM-dd,-1d)]',' 15:00:00')  --手动指定
     and waybillStatus='1'-- 20210415放开只取母单条件  -- 去除子单  1为母单 2为子单 3为废单
	 -- 202104之后 版本
	 -- 航空产品，剔除'T6','ZT6'数据   -- 20230627新增
     and 
     (
            (
                nvl(a.productcode,'') in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                and (nvl(a.routecode,'') not in ('T6','ZT6') or a.routecode is null) 
            )
        or 
            (
                nvl(a.productcode,'') not in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                or a.productcode is null
            )
     )
     ) t1
where rn=1
and nvl(cancelstatus,'a')<>'1'

union all 
select waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,meterageweightqty   --计费重量
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
/*
      ,case   --注意：2021.04.01前规则需要变化 SE0146
		when t1.productcode = 'SE0146' and t1.limittypecode ='T4'       then '1'
		when t1.productcode = 'SE000201' and t1.limittypecode ='T105'       then '1'
		when t1.productcode = 'SE0001'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0107'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0089'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0109'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0008'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0137'       and t1.limittypecode ='T69'    then '1'
		when t1.productcode = 'SE0121'       and t1.limittypecode ='SP619'  then '1'
		when t1.productcode = 'SE002401'     and t1.limittypecode ='T55'    then '1'
		when t1.productcode = 'SE0004'       and t1.limittypecode ='T6'   and t1.limittag = 'SP6' then '1'
else '0' end as is_air_waybill */
-- 20221018修改新航空件逻辑
-- 20230403增加SE0152、SE000206两个航空产品
-- 20230627新增 SE0153,SE0005 两个航空产品  
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005') then '1'
            when productcode='SE0004' and limittag = 'SP6' then '1'
            when productcode='SE0008' and limittag in('T4','T801') then '1'
      else '0' end  as is_air_waybill
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
, concat('$[time(yyyyMMdd,-1d)]', '16') AS inc_dayhour  -- 手动指定
from(select  
        waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,case when meterageweightqty>=5000000 then 0 else meterageweightqty end as meterageweightqty   --计费重量  20230220修改bug
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,case when pickuptm='0' 
            then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
            else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss') 
       end as pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
      ,cancelstatus
      ,row_number() over(partition by waybillno order by processversiontm desc) rn
     from tmp_dm_predict.rmdp_waybill_basedata_tmp20230629 a
    -- where inc_day ='$[time(yyyyMMdd,-1h)]'
	where inc_day ='$[time(yyyyMMdd,-1d)]'  --手动指定
    and to_date(case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
               -- ) ='$[time(yyyy-MM-dd,-1h)]'
			   ) ='$[time(yyyy-MM-dd,-1d)]'  --手动指定
    and (case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
              -- ) <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
			   ) <=concat('$[time(yyyy-MM-dd,-1d)]',' 16:00:00')  --手动指定
     and waybillStatus='1'-- 20210415放开只取母单条件  -- 去除子单  1为母单 2为子单 3为废单
	 -- 202104之后 版本
	 -- 航空产品，剔除'T6','ZT6'数据   -- 20230627新增
     and 
     (
            (
                nvl(a.productcode,'') in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                and (nvl(a.routecode,'') not in ('T6','ZT6') or a.routecode is null) 
            )
        or 
            (
                nvl(a.productcode,'') not in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                or a.productcode is null
            )
     )
     ) t1
where rn=1
and nvl(cancelstatus,'a')<>'1'

union all 
select waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,meterageweightqty   --计费重量
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
/*
      ,case   --注意：2021.04.01前规则需要变化 SE0146
		when t1.productcode = 'SE0146' and t1.limittypecode ='T4'       then '1'
		when t1.productcode = 'SE000201' and t1.limittypecode ='T105'       then '1'
		when t1.productcode = 'SE0001'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0107'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0089'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0109'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0008'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0137'       and t1.limittypecode ='T69'    then '1'
		when t1.productcode = 'SE0121'       and t1.limittypecode ='SP619'  then '1'
		when t1.productcode = 'SE002401'     and t1.limittypecode ='T55'    then '1'
		when t1.productcode = 'SE0004'       and t1.limittypecode ='T6'   and t1.limittag = 'SP6' then '1'
else '0' end as is_air_waybill */
-- 20221018修改新航空件逻辑
-- 20230403增加SE0152、SE000206两个航空产品
-- 20230627新增 SE0153,SE0005 两个航空产品  
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005') then '1'
            when productcode='SE0004' and limittag = 'SP6' then '1'
            when productcode='SE0008' and limittag in('T4','T801') then '1'
      else '0' end  as is_air_waybill
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
, concat('$[time(yyyyMMdd,-1d)]', '17') AS inc_dayhour  -- 手动指定
from(select  
        waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,case when meterageweightqty>=5000000 then 0 else meterageweightqty end as meterageweightqty   --计费重量  20230220修改bug
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,case when pickuptm='0' 
            then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
            else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss') 
       end as pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
      ,cancelstatus
      ,row_number() over(partition by waybillno order by processversiontm desc) rn
     from tmp_dm_predict.rmdp_waybill_basedata_tmp20230629 a
    -- where inc_day ='$[time(yyyyMMdd,-1h)]'
	where inc_day ='$[time(yyyyMMdd,-1d)]'  --手动指定
    and to_date(case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
               -- ) ='$[time(yyyy-MM-dd,-1h)]'
			   ) ='$[time(yyyy-MM-dd,-1d)]'  --手动指定
    and (case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
              -- ) <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
			   ) <=concat('$[time(yyyy-MM-dd,-1d)]',' 17:00:00')  --手动指定
     and waybillStatus='1'-- 20210415放开只取母单条件  -- 去除子单  1为母单 2为子单 3为废单
	 -- 202104之后 版本
	 -- 航空产品，剔除'T6','ZT6'数据   -- 20230627新增
     and 
     (
            (
                nvl(a.productcode,'') in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                and (nvl(a.routecode,'') not in ('T6','ZT6') or a.routecode is null) 
            )
        or 
            (
                nvl(a.productcode,'') not in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                or a.productcode is null
            )
     )
     ) t1
where rn=1
and nvl(cancelstatus,'a')<>'1'

union all 
select waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,meterageweightqty   --计费重量
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
/*
      ,case   --注意：2021.04.01前规则需要变化 SE0146
		when t1.productcode = 'SE0146' and t1.limittypecode ='T4'       then '1'
		when t1.productcode = 'SE000201' and t1.limittypecode ='T105'       then '1'
		when t1.productcode = 'SE0001'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0107'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0089'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0109'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0008'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0137'       and t1.limittypecode ='T69'    then '1'
		when t1.productcode = 'SE0121'       and t1.limittypecode ='SP619'  then '1'
		when t1.productcode = 'SE002401'     and t1.limittypecode ='T55'    then '1'
		when t1.productcode = 'SE0004'       and t1.limittypecode ='T6'   and t1.limittag = 'SP6' then '1'
else '0' end as is_air_waybill */
-- 20221018修改新航空件逻辑
-- 20230403增加SE0152、SE000206两个航空产品
-- 20230627新增 SE0153,SE0005 两个航空产品  
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005') then '1'
            when productcode='SE0004' and limittag = 'SP6' then '1'
            when productcode='SE0008' and limittag in('T4','T801') then '1'
      else '0' end  as is_air_waybill
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
, concat('$[time(yyyyMMdd,-1d)]', '18') AS inc_dayhour  -- 手动指定
from(select  
        waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,case when meterageweightqty>=5000000 then 0 else meterageweightqty end as meterageweightqty   --计费重量  20230220修改bug
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,case when pickuptm='0' 
            then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
            else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss') 
       end as pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
      ,cancelstatus
      ,row_number() over(partition by waybillno order by processversiontm desc) rn
     from tmp_dm_predict.rmdp_waybill_basedata_tmp20230629 a
    -- where inc_day ='$[time(yyyyMMdd,-1h)]'
	where inc_day ='$[time(yyyyMMdd,-1d)]'  --手动指定
    and to_date(case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
               -- ) ='$[time(yyyy-MM-dd,-1h)]'
			   ) ='$[time(yyyy-MM-dd,-1d)]'  --手动指定
    and (case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
              -- ) <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
			   ) <=concat('$[time(yyyy-MM-dd,-1d)]',' 18:00:00')  --手动指定
     and waybillStatus='1'-- 20210415放开只取母单条件  -- 去除子单  1为母单 2为子单 3为废单
	 -- 202104之后 版本
	 -- 航空产品，剔除'T6','ZT6'数据   -- 20230627新增
     and 
     (
            (
                nvl(a.productcode,'') in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                and (nvl(a.routecode,'') not in ('T6','ZT6') or a.routecode is null) 
            )
        or 
            (
                nvl(a.productcode,'') not in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                or a.productcode is null
            )
     )
     ) t1
where rn=1
and nvl(cancelstatus,'a')<>'1'

union all 
select waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,meterageweightqty   --计费重量
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
/*
      ,case   --注意：2021.04.01前规则需要变化 SE0146
		when t1.productcode = 'SE0146' and t1.limittypecode ='T4'       then '1'
		when t1.productcode = 'SE000201' and t1.limittypecode ='T105'       then '1'
		when t1.productcode = 'SE0001'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0107'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0089'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0109'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0008'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0137'       and t1.limittypecode ='T69'    then '1'
		when t1.productcode = 'SE0121'       and t1.limittypecode ='SP619'  then '1'
		when t1.productcode = 'SE002401'     and t1.limittypecode ='T55'    then '1'
		when t1.productcode = 'SE0004'       and t1.limittypecode ='T6'   and t1.limittag = 'SP6' then '1'
else '0' end as is_air_waybill */
-- 20221018修改新航空件逻辑
-- 20230403增加SE0152、SE000206两个航空产品
-- 20230627新增 SE0153,SE0005 两个航空产品  
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005') then '1'
            when productcode='SE0004' and limittag = 'SP6' then '1'
            when productcode='SE0008' and limittag in('T4','T801') then '1'
      else '0' end  as is_air_waybill
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
, concat('$[time(yyyyMMdd,-1d)]', '19') AS inc_dayhour  -- 手动指定
from(select  
        waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,case when meterageweightqty>=5000000 then 0 else meterageweightqty end as meterageweightqty   --计费重量  20230220修改bug
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,case when pickuptm='0' 
            then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
            else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss') 
       end as pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
      ,cancelstatus
      ,row_number() over(partition by waybillno order by processversiontm desc) rn
     from tmp_dm_predict.rmdp_waybill_basedata_tmp20230629 a
    -- where inc_day ='$[time(yyyyMMdd,-1h)]'
	where inc_day ='$[time(yyyyMMdd,-1d)]'  --手动指定
    and to_date(case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
               -- ) ='$[time(yyyy-MM-dd,-1h)]'
			   ) ='$[time(yyyy-MM-dd,-1d)]'  --手动指定
    and (case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
              -- ) <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
			   ) <=concat('$[time(yyyy-MM-dd,-1d)]',' 19:00:00')  --手动指定
     and waybillStatus='1'-- 20210415放开只取母单条件  -- 去除子单  1为母单 2为子单 3为废单
	 -- 202104之后 版本
	 -- 航空产品，剔除'T6','ZT6'数据   -- 20230627新增
     and 
     (
            (
                nvl(a.productcode,'') in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                and (nvl(a.routecode,'') not in ('T6','ZT6') or a.routecode is null) 
            )
        or 
            (
                nvl(a.productcode,'') not in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                or a.productcode is null
            )
     )
     ) t1
where rn=1
and nvl(cancelstatus,'a')<>'1'

union all 
select waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,meterageweightqty   --计费重量
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
/*
      ,case   --注意：2021.04.01前规则需要变化 SE0146
		when t1.productcode = 'SE0146' and t1.limittypecode ='T4'       then '1'
		when t1.productcode = 'SE000201' and t1.limittypecode ='T105'       then '1'
		when t1.productcode = 'SE0001'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0107'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0089'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0109'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0008'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0137'       and t1.limittypecode ='T69'    then '1'
		when t1.productcode = 'SE0121'       and t1.limittypecode ='SP619'  then '1'
		when t1.productcode = 'SE002401'     and t1.limittypecode ='T55'    then '1'
		when t1.productcode = 'SE0004'       and t1.limittypecode ='T6'   and t1.limittag = 'SP6' then '1'
else '0' end as is_air_waybill */
-- 20221018修改新航空件逻辑
-- 20230403增加SE0152、SE000206两个航空产品
-- 20230627新增 SE0153,SE0005 两个航空产品  
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005') then '1'
            when productcode='SE0004' and limittag = 'SP6' then '1'
            when productcode='SE0008' and limittag in('T4','T801') then '1'
      else '0' end  as is_air_waybill
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
, concat('$[time(yyyyMMdd,-1d)]', '20') AS inc_dayhour  -- 手动指定
from(select  
        waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,case when meterageweightqty>=5000000 then 0 else meterageweightqty end as meterageweightqty   --计费重量  20230220修改bug
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,case when pickuptm='0' 
            then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
            else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss') 
       end as pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
      ,cancelstatus
      ,row_number() over(partition by waybillno order by processversiontm desc) rn
     from tmp_dm_predict.rmdp_waybill_basedata_tmp20230629 a
    -- where inc_day ='$[time(yyyyMMdd,-1h)]'
	where inc_day ='$[time(yyyyMMdd,-1d)]'  --手动指定
    and to_date(case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
               -- ) ='$[time(yyyy-MM-dd,-1h)]'
			   ) ='$[time(yyyy-MM-dd,-1d)]'  --手动指定
    and (case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
              -- ) <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
			   ) <=concat('$[time(yyyy-MM-dd,-1d)]',' 20:00:00')  --手动指定
     and waybillStatus='1'-- 20210415放开只取母单条件  -- 去除子单  1为母单 2为子单 3为废单
	 -- 202104之后 版本
	 -- 航空产品，剔除'T6','ZT6'数据   -- 20230627新增
     and 
     (
            (
                nvl(a.productcode,'') in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                and (nvl(a.routecode,'') not in ('T6','ZT6') or a.routecode is null) 
            )
        or 
            (
                nvl(a.productcode,'') not in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                or a.productcode is null
            )
     )
     ) t1
where rn=1
and nvl(cancelstatus,'a')<>'1'

union all 
select waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,meterageweightqty   --计费重量
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
/*
      ,case   --注意：2021.04.01前规则需要变化 SE0146
		when t1.productcode = 'SE0146' and t1.limittypecode ='T4'       then '1'
		when t1.productcode = 'SE000201' and t1.limittypecode ='T105'       then '1'
		when t1.productcode = 'SE0001'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0107'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0089'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0109'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0008'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0137'       and t1.limittypecode ='T69'    then '1'
		when t1.productcode = 'SE0121'       and t1.limittypecode ='SP619'  then '1'
		when t1.productcode = 'SE002401'     and t1.limittypecode ='T55'    then '1'
		when t1.productcode = 'SE0004'       and t1.limittypecode ='T6'   and t1.limittag = 'SP6' then '1'
else '0' end as is_air_waybill */
-- 20221018修改新航空件逻辑
-- 20230403增加SE0152、SE000206两个航空产品
-- 20230627新增 SE0153,SE0005 两个航空产品  
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005') then '1'
            when productcode='SE0004' and limittag = 'SP6' then '1'
            when productcode='SE0008' and limittag in('T4','T801') then '1'
      else '0' end  as is_air_waybill
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
, concat('$[time(yyyyMMdd,-1d)]', '21') AS inc_dayhour  -- 手动指定
from(select  
        waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,case when meterageweightqty>=5000000 then 0 else meterageweightqty end as meterageweightqty   --计费重量  20230220修改bug
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,case when pickuptm='0' 
            then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
            else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss') 
       end as pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
      ,cancelstatus
      ,row_number() over(partition by waybillno order by processversiontm desc) rn
     from tmp_dm_predict.rmdp_waybill_basedata_tmp20230629 a
    -- where inc_day ='$[time(yyyyMMdd,-1h)]'
	where inc_day ='$[time(yyyyMMdd,-1d)]'  --手动指定
    and to_date(case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
               -- ) ='$[time(yyyy-MM-dd,-1h)]'
			   ) ='$[time(yyyy-MM-dd,-1d)]'  --手动指定
    and (case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
              -- ) <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
			   ) <=concat('$[time(yyyy-MM-dd,-1d)]',' 21:00:00')  --手动指定
     and waybillStatus='1'-- 20210415放开只取母单条件  -- 去除子单  1为母单 2为子单 3为废单
	 -- 202104之后 版本
	 -- 航空产品，剔除'T6','ZT6'数据   -- 20230627新增
     and 
     (
            (
                nvl(a.productcode,'') in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                and (nvl(a.routecode,'') not in ('T6','ZT6') or a.routecode is null) 
            )
        or 
            (
                nvl(a.productcode,'') not in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                or a.productcode is null
            )
     )
     ) t1
where rn=1
and nvl(cancelstatus,'a')<>'1'

union all 
select waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,meterageweightqty   --计费重量
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
/*
      ,case   --注意：2021.04.01前规则需要变化 SE0146
		when t1.productcode = 'SE0146' and t1.limittypecode ='T4'       then '1'
		when t1.productcode = 'SE000201' and t1.limittypecode ='T105'       then '1'
		when t1.productcode = 'SE0001'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0107'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0089'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0109'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0008'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0137'       and t1.limittypecode ='T69'    then '1'
		when t1.productcode = 'SE0121'       and t1.limittypecode ='SP619'  then '1'
		when t1.productcode = 'SE002401'     and t1.limittypecode ='T55'    then '1'
		when t1.productcode = 'SE0004'       and t1.limittypecode ='T6'   and t1.limittag = 'SP6' then '1'
else '0' end as is_air_waybill */
-- 20221018修改新航空件逻辑
-- 20230403增加SE0152、SE000206两个航空产品
-- 20230627新增 SE0153,SE0005 两个航空产品  
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005') then '1'
            when productcode='SE0004' and limittag = 'SP6' then '1'
            when productcode='SE0008' and limittag in('T4','T801') then '1'
      else '0' end  as is_air_waybill
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
, concat('$[time(yyyyMMdd,-1d)]', '22') AS inc_dayhour  -- 手动指定
from(select  
        waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,case when meterageweightqty>=5000000 then 0 else meterageweightqty end as meterageweightqty   --计费重量  20230220修改bug
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,case when pickuptm='0' 
            then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
            else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss') 
       end as pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
      ,cancelstatus
      ,row_number() over(partition by waybillno order by processversiontm desc) rn
     from tmp_dm_predict.rmdp_waybill_basedata_tmp20230629 a
    -- where inc_day ='$[time(yyyyMMdd,-1h)]'
	where inc_day ='$[time(yyyyMMdd,-1d)]'  --手动指定
    and to_date(case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
               -- ) ='$[time(yyyy-MM-dd,-1h)]'
			   ) ='$[time(yyyy-MM-dd,-1d)]'  --手动指定
    and (case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
              -- ) <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
			   ) <=concat('$[time(yyyy-MM-dd,-1d)]',' 22:00:00')  --手动指定
     and waybillStatus='1'-- 20210415放开只取母单条件  -- 去除子单  1为母单 2为子单 3为废单
	 -- 202104之后 版本
	 -- 航空产品，剔除'T6','ZT6'数据   -- 20230627新增
     and 
     (
            (
                nvl(a.productcode,'') in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                and (nvl(a.routecode,'') not in ('T6','ZT6') or a.routecode is null) 
            )
        or 
            (
                nvl(a.productcode,'') not in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                or a.productcode is null
            )
     )
     ) t1
where rn=1
and nvl(cancelstatus,'a')<>'1'

union all 
select waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,meterageweightqty   --计费重量
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
/*
      ,case   --注意：2021.04.01前规则需要变化 SE0146
		when t1.productcode = 'SE0146' and t1.limittypecode ='T4'       then '1'
		when t1.productcode = 'SE000201' and t1.limittypecode ='T105'       then '1'
		when t1.productcode = 'SE0001'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0107'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0089'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0109'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0008'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0137'       and t1.limittypecode ='T69'    then '1'
		when t1.productcode = 'SE0121'       and t1.limittypecode ='SP619'  then '1'
		when t1.productcode = 'SE002401'     and t1.limittypecode ='T55'    then '1'
		when t1.productcode = 'SE0004'       and t1.limittypecode ='T6'   and t1.limittag = 'SP6' then '1'
else '0' end as is_air_waybill */
-- 20221018修改新航空件逻辑
-- 20230403增加SE0152、SE000206两个航空产品
-- 20230627新增 SE0153,SE0005 两个航空产品  
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005') then '1'
            when productcode='SE0004' and limittag = 'SP6' then '1'
            when productcode='SE0008' and limittag in('T4','T801') then '1'
      else '0' end  as is_air_waybill
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
, concat('$[time(yyyyMMdd,-1d)]', '23') AS inc_dayhour  -- 手动指定
from(select  
        waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,case when meterageweightqty>=5000000 then 0 else meterageweightqty end as meterageweightqty   --计费重量  20230220修改bug
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,case when pickuptm='0' 
            then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
            else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss') 
       end as pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
      ,cancelstatus
      ,row_number() over(partition by waybillno order by processversiontm desc) rn
     from tmp_dm_predict.rmdp_waybill_basedata_tmp20230629 a
    -- where inc_day ='$[time(yyyyMMdd,-1h)]'
	where inc_day ='$[time(yyyyMMdd,-1d)]'  --手动指定
    and to_date(case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
               -- ) ='$[time(yyyy-MM-dd,-1h)]'
			   ) ='$[time(yyyy-MM-dd,-1d)]'  --手动指定
    and (case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
              -- ) <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
			   ) <=concat('$[time(yyyy-MM-dd,-1d)]',' 23:00:00')  --手动指定
     and waybillStatus='1'-- 20210415放开只取母单条件  -- 去除子单  1为母单 2为子单 3为废单
	 -- 202104之后 版本
	 -- 航空产品，剔除'T6','ZT6'数据   -- 20230627新增
     and 
     (
            (
                nvl(a.productcode,'') in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                and (nvl(a.routecode,'') not in ('T6','ZT6') or a.routecode is null) 
            )
        or 
            (
                nvl(a.productcode,'') not in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                or a.productcode is null
            )
     )
     ) t1
where rn=1
and nvl(cancelstatus,'a')<>'1'

union all 
select waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,meterageweightqty   --计费重量
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
/*
      ,case   --注意：2021.04.01前规则需要变化 SE0146
		when t1.productcode = 'SE0146' and t1.limittypecode ='T4'       then '1'
		when t1.productcode = 'SE000201' and t1.limittypecode ='T105'       then '1'
		when t1.productcode = 'SE0001'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0107'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0089'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0109'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0008'       and t1.limittypecode ='T4'     then '1'
		when t1.productcode = 'SE0137'       and t1.limittypecode ='T69'    then '1'
		when t1.productcode = 'SE0121'       and t1.limittypecode ='SP619'  then '1'
		when t1.productcode = 'SE002401'     and t1.limittypecode ='T55'    then '1'
		when t1.productcode = 'SE0004'       and t1.limittypecode ='T6'   and t1.limittag = 'SP6' then '1'
else '0' end as is_air_waybill */
-- 20221018修改新航空件逻辑
-- 20230403增加SE0152、SE000206两个航空产品
-- 20230627新增 SE0153,SE0005 两个航空产品  
,case when productcode in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005') then '1'
            when productcode='SE0004' and limittag = 'SP6' then '1'
            when productcode='SE0008' and limittag in('T4','T801') then '1'
      else '0' end  as is_air_waybill
-- ,case when substr('$[time(yyyyMMddHH)]',9,2)='00' then concat('$[time(yyyyMMdd,-1h)]','24') else '$[time(yyyyMMddHH)]' end AS inc_dayhour
, concat('$[time(yyyyMMdd,-1d)]', '24') AS inc_dayhour  -- 手动指定
from(select  
        waybillno --运单号
      ,productcode --产品代码
      ,realweightqty  --实际重量
      ,case when meterageweightqty>=5000000 then 0 else meterageweightqty end as meterageweightqty   --计费重量  20230220修改bug
      ,weightrangestatus  --重量归段
      ,pickupdeptcode --收件网点
      ,pickupcitycode --收件城市
      ,pickupareacode --收件业务区
      ,case when pickuptm='0' 
            then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
            else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss') 
       end as pickuptm --收件时间
      ,deliverdeptcode --派送网点
      ,delivercitycode --派送城市
      ,deliverhqcode --派件大区
      ,pickuphqcode --收件大区
      ,routecode --路由代码
      ,mainwaybillno --母运单号
      ,customeracctcode --月结卡号
      ,limittag --SOP标签
      ,limittypecode
      ,quantity
      ,limitplate
      ,cancelstatus
      ,row_number() over(partition by waybillno order by processversiontm desc) rn
     from tmp_dm_predict.rmdp_waybill_basedata_tmp20230629 a
    -- where inc_day ='$[time(yyyyMMdd,-1h)]'
	where inc_day ='$[time(yyyyMMdd,-1d)]'  --手动指定
    and to_date(case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
               -- ) ='$[time(yyyy-MM-dd,-1h)]'
			   ) ='$[time(yyyy-MM-dd,-1d)]'  --手动指定
    and (case when pickuptm='0' 
                     then from_unixtime(cast(cast(consignedtm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     else from_unixtime(cast(cast(pickuptm as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss')
                     end
              -- ) <=concat('$[time(yyyy-MM-dd HH)]',':00:00')
			   ) <=concat('$[time(yyyy-MM-dd)]',' 00:00:00')  --手动指定
     and waybillStatus='1'-- 20210415放开只取母单条件  -- 去除子单  1为母单 2为子单 3为废单
	 -- 202104之后 版本
	 -- 航空产品，剔除'T6','ZT6'数据   -- 20230627新增
     and 
     (
            (
                nvl(a.productcode,'') in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                and (nvl(a.routecode,'') not in ('T6','ZT6') or a.routecode is null) 
            )
        or 
            (
                nvl(a.productcode,'') not in 
                ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
                or a.productcode is null
            )
     )
     ) t1
where rn=1
and nvl(cancelstatus,'a')<>'1' ;



-- step2 
drop table if exists tmp_dm_predict.tmp_dws_hk_dynamic_waybill_2_tmp20230629;
create table if not exists tmp_dm_predict.tmp_dws_hk_dynamic_waybill_2_tmp20230629 
stored as parquet as
select
    to_date(pickuptm) as days
    ,pickupcitycode
    ,delivercitycode
    ,productcode
    ,case 
	  when meterageweightqty >= 20  then '>=20KG'
	  when meterageweightqty >= 15 and meterageweightqty <20 then '[5KG,20KG)'
	  when meterageweightqty >= 10 and meterageweightqty <15 then '[10KG,15KG)'
	  when meterageweightqty >= 3 and meterageweightqty < 10 then '[3KG,10KG)'
	  else '<3KG'
	end as weight_type_code 
    ,count(distinct mainwaybillno) as all_mainwaybill_num
    ,sum(quantity) as all_quantity_num
    ,count(distinct case when t2.is_air_waybill ='1' then mainwaybillno end) as air_mainwaybill_num
    ,sum(case when t2.is_air_waybill ='1' then quantity end) as air_quantity_num
    ,sum(meterageweightqty) as all_wgt_num
    ,sum(case when t2.is_air_waybill ='1' then meterageweightqty end) as air_wgt_num
    ,inc_dayhour
-- from dm_ordi_predict.cf_air_list t1
 from dm_ordi_predict.cf_air_list_backup20230627 t1 -- 20230627新增 
join tmp_dm_predict.tmp_dws_hk_dynamic_waybill_1_tmp20230629 t2
on concat(t2.pickupcitycode,'-',t2.delivercitycode)=t1.cityflow
group by  to_date(pickuptm)
    ,pickupcitycode
    ,delivercitycode
    ,productcode
    ,case 
	  when meterageweightqty >= 20  then '>=20KG'
	  when meterageweightqty >= 15 and meterageweightqty <20 then '[5KG,20KG)'
	  when meterageweightqty >= 10 and meterageweightqty <15 then '[10KG,15KG)'
	  when meterageweightqty >= 3 and meterageweightqty < 10 then '[3KG,10KG)'
	  else '<3KG'
	end
    ,inc_dayhour;



-- step3 写入产品结果表  临时表
set hive.exec.dynamic.partition= true;
set hive.exec.dynamic.partition.mode= nostrict;
set hive.exec.max.dynamic.partitions=2000;  -- 所有执行节点上最多可以多少个动态分区
set hive.exec.max.dynamic.partitions.pernode = 500;

insert overwrite table dm_ordi_predict.dws_air_flow_dynamic_pro_qty_hi_backup20230620 partition (inc_day,inc_dayhour)
select 
  days                 
,concat(pickupcitycode,'-',delivercitycode) as city_flow            
,pickupcitycode as src_city_code        
,delivercitycode as dest_city_code       
,productcode as product_code         
,weight_type_code     
,all_mainwaybill_num  
,all_quantity_num     
,air_mainwaybill_num  
,air_quantity_num     
,all_wgt_num          
,air_wgt_num 
,regexp_replace(days,'-','') as inc_day
,inc_dayhour
from  tmp_dm_predict.tmp_dws_hk_dynamic_waybill_2_tmp20230629 ;




-- step4 
------------------------------------收入版块维度汇总
drop table if exists tmp_dm_predict.tmp_dws_hk_dynamic_waybill_3_tmp20230629;
create table if not exists tmp_dm_predict.tmp_dws_hk_dynamic_waybill_3_tmp20230629 
stored as parquet as
select
     days
    ,pickupcitycode
    ,delivercitycode
    ,case when t1.income_code is not null
          then t1.income_code
          else '其他'
    end as income_code
    ,weight_type_code 
    ,sum(all_mainwaybill_num) as all_mainwaybill_num
    ,sum(all_quantity_num   ) as all_quantity_num
    ,sum(air_mainwaybill_num) as air_mainwaybill_num
    ,sum(air_quantity_num   ) as air_quantity_num
    ,sum(all_wgt_num        ) as all_wgt_num
    ,sum(air_wgt_num        ) as   air_wgt_num  
    ,inc_dayhour
from (
	  select
      prod_code as product_code,
      case
        when income_code = '1' then '时效'
        when income_code = '2' then '电商'
        when income_code = '3' then '国际'
        when income_code = '4' then '大件'
        when income_code = '5' then '医药'
        when income_code = '6' then '其他'
        when income_code = '8' then '冷运'
        when income_code = '9' then '丰网'
      end as income_code
    from
      dim.dim_prod_base_info_df   -- 手动切换 待
    where
      inc_day = '20230315'
      and (invalid_date >= '2023-03-15' or invalid_date is null)
      and income_code in ('1', '2', '3', '4', '5', '6', '8', '9')
      group by prod_code,
      case
        when income_code = '1' then '时效'
        when income_code = '2' then '电商'
        when income_code = '3' then '国际'
        when income_code = '4' then '大件'
        when income_code = '5' then '医药'
        when income_code = '6' then '其他'
        when income_code = '8' then '冷运'
        when income_code = '9' then '丰网'
      end
	) t1
right join  
 tmp_dm_predict.tmp_dws_hk_dynamic_waybill_2_tmp20230629 t2
 on t1.product_code=t2.productcode
 group by days
    ,pickupcitycode
    ,delivercitycode
    ,case when t1.income_code is not null
          then t1.income_code
          else '其他'
    end
    ,weight_type_code 
    ,inc_dayhour;



-- step5 写入收入板块结果表  临时表

set hive.exec.dynamic.partition= true;
set hive.exec.dynamic.partition.mode= nostrict;
set hive.exec.max.dynamic.partitions=2000;  -- 所有执行节点上最多可以多少个动态分区
set hive.exec.max.dynamic.partitions.pernode = 500;

insert overwrite table dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi_backup20230620 partition (inc_day,inc_dayhour)
 select 
  days                 
,concat(pickupcitycode,'-',delivercitycode) as city_flow            
,pickupcitycode as src_city_code        
,delivercitycode as dest_city_code       
,income_code       
,weight_type_code     
,all_mainwaybill_num  
,all_quantity_num     
,air_mainwaybill_num  
,air_quantity_num     
,all_wgt_num          
,air_wgt_num 
,regexp_replace(days,'-','') as inc_day
,inc_dayhour
from tmp_dm_predict.tmp_dws_hk_dynamic_waybill_3_tmp20230629;



