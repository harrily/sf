----------------------开发说明----------------------------
-- 重刷流向底表 20220627更新代码
-- 1、添加剔除路由代码
-- 2、切换air_list表
-- 3、2021-04之后
-- 4、切换income表， 2023-05-16 ~ 2023-06-28 
----------------------修改记录----------------------------
--* 修改人   修改时间      修改内容
/*
*/
----------------------------------------------------------
----------------------hive调优参数列表----------------------
-- 动态分区
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;

----------------------脚本逻辑----------------------------
--输入表： dwd.dwd_pub_waybill_info_mid_di,tmp_dm_predict.dm_fact_air_waybill_no
--输出表： tmp_dm_predict.dws_static_city_flow_2022_tmp_v2 
--从运单表关联真实航空流向表数据，汇总流向数据
----------------------------------------------------------

drop table if exists tmp_dm_predict.dws_static_city_flow_2022_tmp_v20220627;  -- 历史数据
create table tmp_dm_predict.dws_static_city_flow_2022_tmp_v20220627 stored as parquet as 
--收件  
select 
    concat(nvl(a.src_dist_code,'#'),'-',nvl(a.dest_dist_code,'#')) as cityflow
	,substr(a.consigned_tm,1,10) as days  
	,'pickup' as type  
    ,product_code
	,case 
	       when meterage_weight_qty >= 20  then '>=20KG'
	       when meterage_weight_qty >= 15 and meterage_weight_qty <20 then '[15KG,20KG)'
	       when meterage_weight_qty >= 10 and meterage_weight_qty <15 then '[10KG,15KG)'
	       when meterage_weight_qty >= 3 and meterage_weight_qty < 10 then '[3KG,10KG)'
	    else '<3KG'
	end as weight_type_code 
	,src_dist_code as dist_code 
    ,src_area_code as area_code_org
    ,src_hq_code as hq_code
    ,route_code  -- 20230627 新增路由代码
    ,limit_type_code
    ,cargo_type_code	
    ,limit_tag
    ,sum(volume) as volume

	,count(waybill_no) as all_waybill_num  
    ,sum(quantity) as all_quantity  --件量
	,sum(meterage_weight_qty) weight
	,sum(fact_air_waybill) as fact_air_waybill_num
	--,sum(special_econ_waybill) as special_econ_waybill_num
	,sum(fact_air_quantity) as fact_air_quantity
	--,sum(special_econ_quantity) as special_econ_quantity
	,sum(fact_air_weight) as fact_air_weight
	--,sum(special_econ_weight) as special_econ_weight
	,sum(fact_air_volume) as fact_air_volume
	--,sum(special_econ_volume) as special_econ_volume
    
from
(	select 
		-- a.src_dist_code,
		-- a.dest_dist_code,
        case 
            when a.src_dist_code  in ('410','413') then '024'
            when a.src_dist_code  = '910' then '029'
            when a.src_dist_code  = '731' then '7311'
            when a.src_dist_code  = '733' then '7313'
            when a.src_dist_code  = '899' then '8981'
            when a.src_dist_code  = '732' then '7312'
            when a.src_dist_code  = '888' then '088'
        else a.src_dist_code end  as src_dist_code,   -- 20230627修改，替换过期city_code
        case 
            when a.dest_dist_code  in ('410','413') then '024'
            when a.dest_dist_code  = '910' then '029'
            when a.dest_dist_code  = '731' then '7311'
            when a.dest_dist_code  = '733' then '7313'
            when a.dest_dist_code  = '899' then '8981'
            when a.dest_dist_code  = '732' then '7312'
            when a.dest_dist_code  = '888' then '088'
        else a.dest_dist_code end as dest_dist_code,  -- 20230627修改，替换过期city_code
		a.consigned_tm,
		a.product_code,
		round(case when upper(a.unit_weight)='LBS' then a.meterage_weight_qty*0.45359237
                   when upper(a.unit_weight)='G' then a.meterage_weight_qty/1000 
                   else a.meterage_weight_qty 
        end,3) as meterage_weight_qty,
		a.src_area_code,
		a.src_hq_code,
        a.route_code,  -- 20230627 新增路由代码
		a.limit_type_code,
		a.cargo_type_code,
        a.limit_tag,
		a.volume,
		a.waybill_no,
		a.quantity,
		if(b.waybill_no is not null ,1,0) as fact_air_waybill,
		--if(c.waybill_no is not null ,1,0) as special_econ_waybill,
		if(b.waybill_no is not null ,a.quantity,0) as fact_air_quantity,
		--if(c.waybill_no is not null ,a.quantity,0) as special_econ_quantity,
		if(b.waybill_no is not null ,a.meterage_weight_qty,0) as fact_air_weight,
		--if(c.waybill_no is not null ,a.meterage_weight_qty_kg,0) as special_econ_weight,
		if(b.waybill_no is not null ,a.volume,0) as fact_air_volume
		--if(c.waybill_no is not null ,a.volume,0) as special_econ_volume
	from
	dwd.dwd_waybill_info_dtl_di a
	left join (select waybill_no 
				-- from tmp_dm_predict.dm_fact_air_waybill_no
              from  dm_ordi_predict.dm_fact_air_waybill_no
              where inc_day  between '$[time(yyyyMMdd,-45d)]' and '$[time(yyyyMMdd,-1d)]' --  月初月末  -- 手动回刷历史数据 
              group by waybill_no
                ) b 
	on a.waybill_no = b.waybill_no
--      left join tmp_dm_predict.dws_fact_special_econ_dtl_tmp c on a.waybill_no = c.waybill_no
	where 
         a.inc_day>='$[time(yyyyMMdd,-45d)]' and inc_day<='$[time(yyyyMMdd,-1d)]' and   --  月初月末  -- 手动回刷历史数据 
	 substr(a.consigned_tm,0,10) between '$[time(yyyy-MM-dd,-45d)]' and '$[time(yyyy-MM-dd,-1d)]'     -- 月初月末  -- 手动回刷历史数据 

     and (a.src_hq_code<>'CN39' or a.src_hq_code is null)           -- 去丰网
    --  and income_code<>'丰网'                            -- 去丰网
    --  and nvl(a.source_zone_code,'') not in (select distinct dept_code from tmp_ordi_predict.dim_department where dept_code is not null) -- 去丰网
     and (nvl(a.source_zone_code,'') in (select dept_code from dim.dim_department where hq_code<>'CN39' and dept_code is not null group by dept_code ) or a.source_zone_code is null )  -- 去丰网 20230608修改
        ) a 
group by  
	 concat(nvl(a.src_dist_code,'#'),'-',nvl(a.dest_dist_code,'#')) 
	,substr(a.consigned_tm,1,10)
	,product_code
	,case 
	       when meterage_weight_qty >= 20  then '>=20KG'
	       when meterage_weight_qty >= 15 and meterage_weight_qty <20 then '[15KG,20KG)'
	       when meterage_weight_qty >= 10 and meterage_weight_qty <15 then '[10KG,15KG)'
	       when meterage_weight_qty >= 3 and meterage_weight_qty < 10 then '[3KG,10KG)'
	    else '<3KG'
	end
	,src_dist_code
    ,src_area_code
    ,src_hq_code
    ,route_code  -- 20230627 新增路由代码
    ,limit_type_code
    ,cargo_type_code
    ,limit_tag
; 

-----------------------------------------------  step3 ---------------------------------------------------------------





----------------------开发说明----------------------------
--* 名称:流向新底盘-step2
--* 任务ID:
--* 说明:流向新底盘-step2
--* 作者: 张长发
--* 时间: 2022/9/02 15:45
----------------------修改记录----------------------------
--* 修改人   修改时间      修改内容
--* 雷超     20230424     新建任务
----------------------------------------------------------
-- 动态分区
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;


----------------------脚本逻辑----------------------------
--输入表： tmp_dm_predict.dws_static_city_flow_2022_tmp_v2
--输出表：  dm_ordi_predict.dws_static_his_cityflow_v2
--根据流向逻辑，汇总流向数据，同时关联产品板块，业务区等维表，添加维度
----------------------------------------------------------

drop table if exists  tmp_dm_predict.dws_static_his_cityflow_2022_tmp_v2_20220627;
create table tmp_dm_predict.dws_static_his_cityflow_2022_tmp_v2_20220627 stored as parquet as 
select 
    a.days
    ,a.cityflow
    ,a.type 
    -- ,nvl(a.income_code,'其他') as income_code 
    -- 20230630 切换income表，指定产品数据，刷新历史判断指定income
    ,case when nvl(a.product_code,'') in ('SE0101','SE0100') then '大件'
	 else nvl(a.income_code,'其他') end as income_code 
    ,a.product_code 
    ,a.weight_type_code 
    ,a.dist_code 
    ,a.area_code_jg 
    ,a.hq_code
    ,a.route_code  -- 20230627 新增路由代码
    ,a.limit_type_code
    ,a.limit_tag
    ,a.all_waybill_num 
    ,a.all_quantity 
    ,a.weight 
    ,nvl(a.volume,0) as volume
    ,a.fact_air_waybill_num
	,a.fact_air_quantity
	,a.fact_air_weight
	,a.fact_air_volume
    --,a.special_econ_weight
    --,a.special_econ_quantity
    --,a.special_econ_waybill_num
	--,a.special_econ_volume
   -- 202104之后
   -- 20230629 航空件新增剔除路由代码 ('T6','ZT6')   
   ,case   
        when a.product_code = 'SE000201' and b.cityflow is not null and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
		when a.product_code = 'SE0001' and b.cityflow is not null and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
		when a.product_code = 'SE0107' and b.cityflow is not null and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
        when a.product_code = 'SE0103' and b.cityflow is not null and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
        when a.product_code = 'SE0051' and b.cityflow is not null and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
		when a.product_code = 'SE0089' and b.cityflow is not null and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
		when a.product_code = 'SE0109' and b.cityflow is not null and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
		when a.product_code = 'SE0008' and b.cityflow is not null and (a.limit_tag ='T4' or a.limit_tag ='T801') and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
		when a.product_code = 'SE0137' and b.cityflow is not null and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
		when a.product_code = 'SE0121' and b.cityflow is not null and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
		when a.product_code = 'SE0004' and a.limit_tag = 'SP6' and b.cityflow is not null and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
        when a.product_code = 'SE0146' and b.cityflow is not null and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
        when a.product_code = 'SE0152' and b.cityflow is not null and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'  --20230403 新增两个航空产品
        when a.product_code = 'SE000206' and b.cityflow is not null and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
        when a.product_code = 'SE0153' and b.cityflow is not null and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'  -- 20230627 新增航空产品
        when a.product_code = 'SE0005' and b.cityflow is not null and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'  -- 20230627 新增航空产品
		else '0' 
    end as is_air
     /* 
     ,case     --202104前
        when a.product_code = 'SE000201' and b.cityflow is not null and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
		when a.product_code = 'SE0001' and b.cityflow is not null and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
		when a.product_code = 'SE0107' and b.cityflow is not null and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
        when a.product_code = 'SE0103' and b.cityflow is not null and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
        when a.product_code = 'SE0051' and b.cityflow is not null and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
		when a.product_code = 'SE0089' and b.cityflow is not null and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
		when a.product_code = 'SE0109' and b.cityflow is not null and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)  then '1'
		when a.product_code = 'SE0008' and b.cityflow is not null and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
		when a.product_code = 'SE0137' and b.cityflow is not null and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
		when a.product_code = 'SE0121' and b.cityflow is not null and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
        when a.product_code = 'SE002401' and b.cityflow is not null and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
        when a.product_code = 'SE0003' and b.cityflow is not null and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
		else '0' 
     end as is_air
     */
    ,if(b.cityflow is not null,1,0) as is_air_flow

      --202104之后
      -- 20230629 航空件新增剔除路由代码 ('T6','ZT6')   
	,case   
        when a.product_code = 'SE000201' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
		when a.product_code = 'SE0001' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
		when a.product_code = 'SE0107' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
        when a.product_code = 'SE0103' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
        when a.product_code = 'SE0051' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
		when a.product_code = 'SE0089' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
		when a.product_code = 'SE0109' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
		when a.product_code = 'SE0008' and (a.limit_tag ='T4' or a.limit_tag ='T801') and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
		when a.product_code = 'SE0137' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
		when a.product_code = 'SE0121' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
		when a.product_code = 'SE0004' and a.limit_tag = 'SP6' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
        when a.product_code = 'SE0146' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
        when a.product_code = 'SE0152' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1' --20230403 新增两个航空产品
        when a.product_code = 'SE000206' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
        when a.product_code = 'SE0153' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1' -- 20230627 新增航空产品
        when a.product_code = 'SE0005' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1' -- 20230627 新增航空产品
		else '0' 
     end as is_air_waybill
    /*
     ,case    --202104前
        when a.product_code = 'SE000201' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
		when a.product_code = 'SE0001' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
		when a.product_code = 'SE0107' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
        when a.product_code = 'SE0103' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
        when a.product_code = 'SE0051' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
		when a.product_code = 'SE0089' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
		when a.product_code = 'SE0109' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
		when a.product_code = 'SE0008' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
		when a.product_code = 'SE0137' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
		when a.product_code = 'SE0121' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
        when a.product_code = 'SE002401' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
        when a.product_code = 'SE0003' and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) then '1'
		else '0' 
     end as is_air_waybill
     */
from 
(   
    select 
        cityflow --流向代码
        ,days  --日期
        ,type  --收派类型
        ,a.product_code --产品代码
        ,weight_type_code  --重量段
        ,dist_code --城市代码
        ,a.hq_code
        ,case 
            when a.hq_code = 'CN06' then 'CN06' 
            when a.hq_code = 'CN07' then 'CN07'
            -- when a.hq_code = 'CN39' then a.area_code_org  -- 去丰网 
            when a.hq_code = 'HQOP' then a.area_code_org
            when b.area_code is not null then b.area_code
            when b.area_code is null then a.area_code_org
            else 'other' end as area_code_jg --经管业务区
        ,z.income_code as income_code  --产品名称
        ,a.route_code  -- 20230627 新增路由代码
        ,a.limit_type_code
        ,a.cargo_type_code	
        ,a.limit_tag
        ,all_waybill_num  --票量
        ,all_quantity  --件量
        ,weight
        ,volume
        ,fact_air_waybill_num
	    ,fact_air_quantity
	    ,fact_air_weight
	    ,fact_air_volume
        --,special_econ_weight
        --,special_econ_quantity
        --,special_econ_waybill_num
	    --,special_econ_volume
    from 
        tmp_dm_predict.dws_static_city_flow_2022_tmp_v20220627 a  
    left join 
    (
        -- 切换新的income表
		select product_code,income_code from dm_predict.dim_prod_base_config_full  -- 20230630 使用新的income表
		where inc_day='9999'
    )z
        on a.product_code= z.product_code 
    left join 
    (
        select 
            * 
        from 
           dm_fin_itp.itp_vs_rel_city_bg_area_mpp 
            -- tmp_ordi_predict.itp_vs_rel_city_bg_area_mpp  -- 当时没权限，使用临时表
        where 
            BG_code='SF001' and to_tm='9999-12-31' and area_code not in ('852Y','886Y') and hq_version = 'YJ'
    ) b  
        on a.dist_code = b.city_code     
)a
left join 
(
    -- select * from dm_ordi_predict.cf_air_list
    -- 20230627 切换使用业务提供航空流向表数据
    select * from dm_ordi_predict.cf_air_list_backup20230627
)b
    on a.cityflow = b.cityflow
;
        


SET hive.exec.max.dynamic.partitions.pernode =1000;  
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.dynamic.partition.mode=nonstrict;

      insert overwrite table dm_ordi_predict.dws_static_his_cityflow_backup20230620 partition(inc_day) 
select
    a.days
    ,a.cityflow 
    ,a.type 
    ,CASE WHEN product_code = 'SE0088' AND weight_type_code = '>=20KG' THEN '大件'
          WHEN product_code = 'SE0088' AND weight_type_code != '>=20KG' THEN '电商'  ELSE income_code END AS income_code
    ,a.product_code
    ,a.weight_type_code 
    ,a.dist_code 
    ,a.area_code_jg
	,a.hq_code
    ,a.limit_type_code
    ,a.limit_tag
	,a.is_air_flow
    ,a.is_air
    ,a.is_air_waybill
    ,sum(a.all_waybill_num) as  all_waybill_num
    ,sum(a.all_quantity) as all_quantity 
    ,sum(a.weight) as weight 
    ,sum(a.volume) as volume
    ,sum(a.fact_air_waybill_num) as fact_air_waybill_num
	,sum(a.fact_air_quantity) as fact_air_quantity
	,sum(a.fact_air_weight) as fact_air_weight
	,sum(a.fact_air_volume) as fact_air_volume
    --,special_econ_weight
    --,special_econ_quantity
    --,special_econ_waybill_num
    --,special_econ_volume
    ,regexp_replace(days,'-','') as inc_day
from
    tmp_dm_predict.dws_static_his_cityflow_2022_tmp_v2_20220627 a 
    where   !(nvl(a.income_code,'')='丰网' or nvl(a.hq_code,'')='CN39')  -- 去丰网  incomecode  手动回刷历史数据 去丰网
group by   a.days
    ,a.cityflow 
    ,a.type 
    ,CASE WHEN product_code = 'SE0088' AND weight_type_code = '>=20KG' THEN '大件'
          WHEN product_code = 'SE0088' AND weight_type_code != '>=20KG' THEN '电商'  ELSE income_code END
    ,a.product_code
    ,a.weight_type_code 
    ,a.dist_code 
    ,a.area_code_jg
	,a.hq_code
    ,a.limit_type_code
    ,a.limit_tag
	,a.is_air_flow
    ,a.is_air
    ,a.is_air_waybill;
	
	
	
	
	-----------------------------------------------  step4444 ---------------------------------------------------------------
	
	
	
	drop table tmp_ordi_predict.dws_static_cityflow_base_2022_tmp_v1_20220627;
create table tmp_ordi_predict.dws_static_cityflow_base_2022_tmp_v1_20220627 as 
select 
    days  --日期
    ,cityflow --网点代码
    ,is_air
	,income_code
	,weight_type_code
	,sum(all_waybill_num) as all_waybill_num
    ,sum(all_quantity) as all_quantity
    ,sum(if(is_air='1',all_waybill_num,0)) as all_waybill_num_air
    ,sum(if(is_air='1',all_quantity,0)) as all_quantity_air
	,sum(weight) as weight 
    ,sum(volume) as volume
    ,sum(fact_air_waybill_num) as fact_air_waybill_num
	,sum(fact_air_quantity) as fact_air_quantity
	,sum(fact_air_weight) as fact_air_weight
	,sum(fact_air_volume) as fact_air_volume
    --,sum(special_econ_weight) as special_econ_weight
    --,sum(special_econ_quantity) as special_econ_quantity
    --,sum(special_econ_waybill_num) as special_econ_waybill_num
	--,sum(special_econ_volume) as special_econ_volume
    ,inc_day
from 
  dm_ordi_predict.dws_static_his_cityflow_backup20230620 
where 
    inc_day between '$[time(yyyyMMdd,-45d)]' and '$[time(yyyyMMdd,-1d)]'  -- 月初月末   手动回刷历史数据 
     and !(nvl(income_code,'')='丰网' or nvl(hq_code,'')='CN39')  -- 去丰网  incomecode
group by 
    days
    ,type
    ,cityflow
    ,income_code
    ,weight_type_code
    ,is_air
    ,inc_day
;


-- 20220916增加字段 修改脚本  修改人：shenhy 01379968

/*
alter table dm_ordi_predict.dws_static_cityflow_base add columns (src_province string comment '始发省份') cascade;
alter table dm_ordi_predict.dws_static_cityflow_base add columns (src_distribution_name string comment '始发分拨区') cascade;
alter table dm_ordi_predict.dws_static_cityflow_base add columns (dest_province string comment '目的省份') cascade;
alter table dm_ordi_predict.dws_static_cityflow_base add columns (dest_distribution_name string comment '目的分拨区') cascade;
*/

SET hive.exec.max.dynamic.partitions.pernode =1000;  
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table dm_ordi_predict.dws_static_cityflow_base_backup20230620 partition(inc_day) 
select
    days,
    --type,
    cityflow,
    is_air,
    income_code,
    weight_type_code,
    all_waybill_num,
    all_quantity,
    all_waybill_num_air,
    all_quantity_air,
    weight,
    volume
    ,fact_air_waybill_num
	,fact_air_quantity
	,fact_air_weight
	,fact_air_volume
    ,if(t2.province is not null,t2.province,'海外') as src_province
    ,t2.distribution_name as src_distribution_name
	,if(t3.province is not null,t3.province,'海外') as dest_province
    ,t3.distribution_name as dest_distribution_name
    ,regexp_replace(days,'-','') as inc_day
from
    tmp_ordi_predict.dws_static_cityflow_base_2022_tmp_v1_20220627 t1
    left join
	(select
		city_code   --城市代码
		,city_name  --城市名称
		,province   --省份名称
		,distribution_name  -- 分拨区名称
	from dm_ordi_predict.dim_city_level_mapping_df 
	where inc_day='$[time(yyyyMMdd,-1d)]'
	and  if_foreign='0'  -- 筛选国内
	) t2
	on nvl(split(t1.cityflow,'-')[0],'a')=t2.city_code
	left join
	(select
		city_code   --城市代码
		,city_name  --城市名称
		,province   --省份名称
		,distribution_name  -- 分拨区名称
	from dm_ordi_predict.dim_city_level_mapping_df 
	where inc_day='$[time(yyyyMMdd,-1d)]'
	and  if_foreign='0'  -- 筛选国内
	) t3
	on nvl(split(t1.cityflow,'-')[1],'a')=t3.city_code;


/*
SET hive.exec.max.dynamic.partitions.pernode =1000;  
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dm_ordi_predict.dws_static_cityflow_base partition(inc_day) 
select
    days,
    --type,
    cityflow,
    is_air,
    income_code,
    weight_type_code,
    all_waybill_num,
    all_quantity,
    all_waybill_num_air,
    all_quantity_air,
    weight,
    volume
    ,fact_air_waybill_num
	,fact_air_quantity
	,fact_air_weight
	,fact_air_volume
    --,special_econ_weight
    --,special_econ_quantity
    --,special_econ_waybill_num
	--,special_econ_volume
    ,regexp_replace(days,'-','') as inc_day
from
    tmp_ordi_predict.dws_static_cityflow_base_2022_tmp_v1;*/