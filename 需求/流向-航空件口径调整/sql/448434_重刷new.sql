--step1 获取航空流向运单明细
-- 20220908修改来源表
	----------------------------------- ID：448409----------------------------------
drop table if exists tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp1_backup20230629;
create table tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp1_backup20230629 
stored as parquet as 
select waybill_no 
,source_zone_code 
,dest_zone_code	 
,concat(src_dist_code,'-',dest_dist_code) as city_flow
-- ,addressee_dept_code 
,src_dist_code	--	原寄地区域代码
-- ,src_city_code	--	源寄地城市代码
-- ,src_county	--	源寄地区/县/镇
,src_division_code	--	源寄地分部
,src_area_code	--	源寄地区部
,src_type_code	--	源寄地网点类型
,dest_dist_code	--	目的地区域代码
-- ,dest_city_code	--	目的地城市代码
,dest_area_code	--	目的地区部
,dest_type_code	--	目的地网点类型
,case when meterage_weight_qty>=100000 then 0 else meterage_weight_qty end as meterage_weight_qty	--	计费总重量
,real_weight_qty	--	实际重量
,quantity	--	包裹总件数
,consigned_tm	--	寄件时间
-- ,signin_tm	--	签收时间
,limit_type_code	--	时效类型(维表：dim.dim_tml_type_info_df)
-- ,distance_type_code	--	区域类型(维表：dim.dim_pub_distance_type_info_df)
,express_type_code 
-- ,ackbill_type_code
,volume	--	总体积
-- ,bill_long	--	总长
-- ,bill_width	--	总宽
-- ,bill_high	--	总高
-- ,version_no	--	版本号
,product_code	--	产品代码 （维表：dim.dim_product_info）
,freight_monthly_acct_code	--	运费月结账号
-- ,src_province	--	源寄地省份
-- ,dest_province	--	目的地省份
-- ,meterage_weight_qty_kg	--	计费总重量（kg）
-- ,src_area_name	--	源寄地区部名称
-- ,dest_area_name	--	目的地区部名称
,limit_tag 
,route_code  -- 20230629修改 新增路由代码 
,cons_name
,concat_ws(',',cons_name) as cons_name_concat
,inc_day
from  dwd.dwd_waybill_info_dtl_di t1
-- dwd.dwd_pub_waybill_info_mid_di t1
-- dwd.dwd_waybill_info_dtl_di t1
where inc_day between  '$[time(yyyyMMdd,-45d)]' and '$[time(yyyyMMdd,-1d)]' 
-- and inc_day>='20210401'  
;


------------------------------------ ID:448419 ------------------------------------
   
   
   
   
set hive.auto.convert.join=true;  -- 预防倾斜
set spark.sql.extensions=org.apache.kyuubi.sql.KyuubiSparkSQLExtension; -- 合并小文件

-- 只取航空流向、判断是否为航空件
drop table if exists tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp2_backup20230629;
create table tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp2_backup20230629 
stored as parquet as  
select
	t2.*
       -- 20210401之前  
	   -- 使用之前逻辑，重刷仅添加剔除路由逻辑
   /* ,case when product_code in ('SE0001','SE000201','SE0003','SE0107','SE0109','SE0008','SE0089','SE0121','SE0137','SE0103','SE0051','SE002401')
		and (nvl(t2.route_code,'') not in ('T6','ZT6') or t2.route_code is null)
           then '1'
      else '0'
      end as is_air
      */
	 -- 20210401之后
	 -- 20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
     ,case when product_code in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
		and (nvl(t2.route_code,'') not in ('T6','ZT6') or t2.route_code is null) then '1' --20230403 增加两个航空产品
            when product_code='SE0004' and limit_tag = 'SP6' and (nvl(t2.route_code,'') not in ('T6','ZT6') or t2.route_code is null) then '1'
            when product_code = 'SE0008' and limit_tag in('T4','T801') and (nvl(t2.route_code,'') not in ('T6','ZT6') or t2.route_code is null) then '1'
      else '0'
     end as is_air 
	 
-- from dm_ordi_predict.cf_air_list t1
 from dm_ordi_predict.cf_air_list_backup20230627 t1 -- 20230629新增 
join 
tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp1_backup20230629 t2
on t1.cityflow=t2.city_flow;

-- 判断是否特色经济
drop table if exists tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp3_backup20230629;
create table tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp3_backup20230629
stored as parquet as  
select
	t1.*
	,t2.pro_name
	,t2.level_1_type
	,t2.level_2_type
	,case when t3.pro_name is not null then '1' else '0' end as if_econ_fresh
from tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp2_backup20230629 t1
left join 
(select waybill_no,pro_name,level_1_type,level_2_type 
    from (select waybill_no,pro_name,level_1_type,level_2_type,
            row_number() over(partition by waybill_no order by inc_day desc) as rn  
        from (select 
                    waybill_no,pro_name,level_1_type,level_2_type,inc_day 
                from 
                    dm_bie.bie_fact_special_econ_dely_dtl_2022new   -- 2023-05-18  替换新表
                where inc_day between  '$[time(yyyyMMdd,-45d)]' and '$[time(yyyyMMdd,-11d)]'
                union all
                select 
                    waybill_no,pro_name,level_1_type,level_2_type,inc_day 
                from 
                    dm_ordi_predict.dws_spec_eco_pro_waybill_day  
              where inc_day between '$[time(yyyyMMdd,-10d)]' and  '$[time(yyyyMMdd,-1d)]'
            ) tmp1 
    ) tmp2 where tmp2.rn = 1   -- 2023-05-18 添加去重
)t2  -- 20220816添加
on t1.waybill_no=t2.waybill_no
left join 
dm_ordi_predict.dm_special_econ_dely_fresh_dtl t3
on t2.pro_name=t3.pro_name
where t1.is_air='1';

------------------------- id 448704  ----------------------------------------


set hive.execution.engine=tez; 
set tez.queue.name=${queue};
set hive.auto.convert.join=true;  -- 预防倾斜


-- 判断是否大客户中的鞋服客户
drop table if exists tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp4_backup20230629;
create table tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp4_backup20230629
stored as parquet as  
select   
	t1.*
	,t2.companyabbr
	,t2.custtypename
	,case when t3.cust_name is not null then '1' else '0' end as if_shoes_cust
from tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp3_backup20230629 t1
left join 
(	
select 
	 companyabbr
	,companycustno
	,custtypename  	
from dm_kadm.kadm_dim_cust_mapping a  	
where status='1'     
and custtypename like '%KA%'  
group by companyabbr
	,companycustno
	,custtypename) t2
on nvl(t1.freight_monthly_acct_code,'a') =t2.companycustno
left join 
dm_ordi_predict.dm_shoes_clothing_cust_name_dtl t3
on t2.companyabbr =t3.cust_name; 

------------------------- id 472795 ----------------------------------------



drop table if exists tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp9_backup20230629;
create table if not exists tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp9_backup20230629 
stored as parquet as
select
  plan_send_dt,
  send_province,
  arrive_province,
  operation_type,
  dist_code,
  inc_day,
  sum(real_weight) as sum_real_weight,
  sum(waybill_cnt) as sum_waybill_cnt,
  sum(real_weight) / sum(waybill_cnt) as avg_weight
from
  (
    select
      plan_send_dt,
      send_province,
      arrive_province,
      case when operation_type ='特快系列' -- 20221206修改 由于上游11.17日改名
	  	   then '特快'
           when operation_type ='经济快件'
           then '新空配'
		   else operation_type
	 end as operation_type,
      real_weight,
      waybill_cnt,
      avg_weight,
      zone_code,
      inc_day,
      t2.dist_code
    from
      dm_ops.dm_zz_air_waybill_avg_weight t1   -- 2021-04之前数据没有？
      left join (
        select
          dept_code,
          dist_code
        from
          dim.dim_department
      ) t2 on t1.zone_code = t2.dept_code
    where
      inc_day between  '$[time(yyyyMMdd,-45d)]' and '$[time(yyyyMMdd,-1d)]' 
  ) t
group by
  plan_send_dt,
  send_province,
  arrive_province,
  operation_type,
  dist_code,
  inc_day;



----------------------------- ID : 448431 ------------------------------------------





set hive.auto.convert.join=true;  -- 预防倾斜
set hive.groupby.mapaggr.checkinterval=100000;
set hive.groupby.skewindata=true;

set spark.sql.extensions=org.apache.kyuubi.sql.KyuubiSparkSQLExtension;


set hive.exec.dynamic.partition= true;
set hive.exec.dynamic.partition.mode= nostrict;
set hive.exec.max.dynamic.partitions=2000;  -- 所有执行节点上最多可以多少个动态分区
set hive.exec.max.dynamic.partitions.pernode = 500;

-- insert overwrite table dm_ordi_predict.dws_air_flow_six_dims_waybill_dtl partition (inc_day)
insert overwrite table dm_ordi_predict.dws_air_flow_six_dims_waybill_dtl_backup20230620  partition (inc_day)
select 
 consigned_tm                  
,waybill_no                    
,quantity                      
,case when meterage_weight_qty>=1000000
     then 0 
     else meterage_weight_qty 
end as    meterage_weight_qty 
,product_code                  
,limit_type_code               
,limit_tag                     
,if_shoes_cust                 
,if_econ_fresh                 
,pro_name                      
,level_1_type                  
,level_2_type                  
,companyabbr                   
,custtypename                  
,freight_monthly_acct_code     
,city_flow                     
,t2.dept_name as src_city_name                 
,t3.dept_name as dest_city_name                
,case when product_code = 'SE0089' and limit_type_code ='T4'  then '顺丰空配' 
      when  product_code='SE0146' and limit_type_code ='T4'  then 'SE0146' --20220906添加
		  when product_code='SE0107' and limit_type_code ='T4'  then '特快包裹'
          when product_code='SE0152'  then '特快包裹'  --20230403 新增航空产品
		  when product_code='SE0004'  and limit_type_code ='T6' and limit_tag='SP6' then '陆升航'
		  when if_shoes_cust='1' then '鞋服大客户'
		  when if_econ_fresh='1' then '特色经济'
		  else '特快'
	end as type  
,regexp_replace(to_date(consigned_tm),'-','') as  inc_day                      
-- from tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp4 t1
from tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp4_backup20230629 t1
left join 
(select 
	dept_code
	,dept_name
from dim.dim_department
where dept_code=dist_code) t2
on split(city_flow,'-')[0]=t2.dept_code
left join 
(select 
	dept_code
	,dept_name
from dim.dim_department
where dept_code=dist_code) t3
on split(city_flow,'-')[1]=t3.dept_code;


drop table if exists tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp5_backup20230629;
create table tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp5_backup20230629
stored as parquet as  
select 
	to_date(consigned_tm) as consigned_dt
	,city_flow
	,t2.dept_name as src_city_name
	,t3.dept_name as dest_city_name
	,count(waybill_no) as air_waybill_num
	,sum(quantity)     as air_quantity_num
	,sum(meterage_weight_qty) as air_sum_weight
	,avg(meterage_weight_qty) as air_avg_weight
	,percentile_approx(meterage_weight_qty,0.5) as air_mid_weight
	,case when product_code in ('SE0089','SE0146') and limit_type_code ='T4'  then '顺丰空配'
		  when product_code='SE0107' and limit_type_code ='T4'  then '特快包裹'
          when product_code='SE0152'  then '特快包裹'  --20230403 新增航空产品
		  when product_code='SE0004'  and limit_type_code ='T6' and limit_tag='SP6' then '陆升航'
		  when if_shoes_cust='1' then '鞋服大客户'
		  when if_econ_fresh='1' then '特色经济'
		  else '特快'
	end as type
-- from tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp4 t1
from tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp4_backup20230629 t1
left join 
(select 
	dept_code
	,dept_name
from dim.dim_department
where dept_code=dist_code) t2
on split(city_flow,'-')[0]=t2.dept_code
left join 
(select 
	dept_code
	,dept_name
from dim.dim_department
where dept_code=dist_code) t3
on split(city_flow,'-')[1]=t3.dept_code
group by to_date(consigned_tm)
	,city_flow
	,t2.dept_name
	,t3.dept_name
	,case when product_code in ('SE0089','SE0146') and limit_type_code ='T4'  then '顺丰空配'
		   when product_code='SE0107' and limit_type_code ='T4'  then '特快包裹'
           when product_code='SE0152'  then '特快包裹'  --20230403 新增航空产品
		   when product_code='SE0004'  and limit_type_code ='T6' and limit_tag='SP6' then '陆升航'
		   when if_shoes_cust='1' then '鞋服大客户'
		   when if_econ_fresh='1' then '特色经济'
		  else '特快'
	end;


set hive.exec.dynamic.partition= true;
set hive.exec.dynamic.partition.mode= nostrict;
set hive.exec.max.dynamic.partitions=2000;  -- 所有执行节点上最多可以多少个动态分区
set hive.exec.max.dynamic.partitions.pernode = 500;

-- insert overwrite table dm_ordi_predict.dws_air_flow_six_dims_day partition (inc_day)
insert overwrite table dm_ordi_predict.dws_air_flow_six_dims_day_backup20230620 partition (inc_day)
select 
	 consigned_dt  as days            
    ,city_flow        
    ,src_city_name    
    ,dest_city_name   
    ,type             
    ,air_waybill_num  
    ,air_quantity_num 
    ,air_sum_weight   
    ,air_avg_weight   
    ,air_mid_weight  
	,regexp_replace(consigned_dt,'-','') as inc_day
-- from  tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp5;
   from  tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp5_backup20230629;



drop table if exists tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp6_backup20230629;
create table tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp6_backup20230629    
stored as parquet as
select 
	 consigned_dt           
    ,city_flow        
    ,src_city_name    
    ,dest_city_name   
    ,case when type in ('特快包裹','顺丰空配')
		  then '新空配'
		  else type
	end as type            
    ,sum(air_waybill_num) as   air_waybill_num
    ,sum(air_quantity_num ) as air_quantity_num
    ,sum(air_sum_weight) as   air_sum_weight
    ,sum(air_avg_weight) as   air_avg_weight
    ,sum(air_mid_weight) as  air_mid_weight
-- from  tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp5
   from  tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp5_backup20230629
group by consigned_dt            
    ,city_flow        
    ,src_city_name    
    ,dest_city_name   
    ,case when type in ('特快包裹','顺丰空配')
		  then '新空配'
		  else type
	end;



drop table if exists tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp6_0_backup20230629;
create table if not exists tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp6_0_backup20230629 
stored as parquet as
select 
     consigned_dt            
    ,city_flow        
    ,src_city_name    
    ,dest_city_name   
    ,type            
    ,air_waybill_num
    ,air_quantity_num
    ,air_sum_weight
    ,air_avg_weight
    ,air_mid_weight
    ,t2.province as src_province
    ,t3.province as dest_province
-- from tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp6 t1
from tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp6_backup20230629 t1
left join 
dm_ops.dim_city_area_distribution t2
on split(t1.city_flow,'-')[0]=t2.city_code
left join 
dm_ops.dim_city_area_distribution t3
on split(t1.city_flow,'-')[1]=t3.city_code;

-- 均重
drop table if exists tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp6_1_backup20230629;
create table if not exists tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp6_1_backup20230629
stored as parquet as
select 
    consigned_dt            
    ,city_flow        
    ,src_city_name    
    ,dest_city_name   
    ,type            
    ,air_waybill_num
    ,air_quantity_num
    ,air_sum_weight
    ,air_avg_weight
    ,air_mid_weight
    ,src_province
    ,dest_province
    ,t2.avg_weight  
	,t1.air_quantity_num*t2.avg_weight as sum_avg_weight
	,inc_day
-- from tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp6_0  t1
from tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp6_0_backup20230629  t1
left join 
-- tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp9 t2
   tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp9_backup20230629 t2
on t1.src_province=t2.send_province
and t1.dest_province=t2.arrive_province
and split(t1.city_flow,'-')[0]=t2.dist_code
and t1.consigned_dt=t2.plan_send_dt
and t1.type=t2.operation_type;



-- insert overwrite table dm_ordi_predict.dws_air_flow_six_dims_day_newhb partition (inc_day)
insert overwrite table dm_ordi_predict.dws_air_flow_six_dims_day_newhb_backup20230620 partition (inc_day)
select 
	 consigned_dt  as days            
    ,city_flow        
    ,src_city_name    
    ,dest_city_name   
    ,type            
    ,air_waybill_num
    ,air_quantity_num
    ,air_sum_weight
    ,air_avg_weight
    ,air_mid_weight
    ,src_province
    ,dest_province
    ,avg_weight
    ,sum_avg_weight
	,regexp_replace(consigned_dt,'-','') as inc_day
--from  tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp6_1;
from  tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp6_1_backup20230629;









