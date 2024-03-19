---------------------------- id： 471306 ----------------------------
set hive.auto.convert.join=true;  -- 预防倾斜

-- 关联取航空流向、判断是否为航空件
drop table if exists tmp_ordi_predict.tmp_shenhy_air_special_econ_tmp2_backup20230629;
create table tmp_ordi_predict.tmp_shenhy_air_special_econ_tmp2_backup20230629 
stored as parquet as  
select
	t2.*
	/* ,case   --注意：2021.04.01前规则需要变化
        when t2.product_code = 'SE0146'   and t2.limit_type_code ='T4'  and t1.cityflow is not null then '1'
        when t2.product_code = 'SE000201' and t2.limit_type_code ='T105'  and t1.cityflow is not null then '1'
		when t2.product_code = 'SE0001'   and t2.limit_type_code ='T4'    and t1.cityflow is not null then '1'
		when t2.product_code = 'SE0107'   and t2.limit_type_code ='T4'    and t1.cityflow is not null then '1'
		when t2.product_code = 'SE0089'   and t2.limit_type_code ='T4'    and t1.cityflow is not null then '1'
		when t2.product_code = 'SE0109'   and t2.limit_type_code ='T4'    and t1.cityflow is not null then '1'
		when t2.product_code = 'SE0008'   and t2.limit_type_code ='T4'    and t1.cityflow is not null then '1'
		when t2.product_code = 'SE0137'   and t2.limit_type_code ='T69'   and t1.cityflow is not null then '1'
		when t2.product_code = 'SE0121'   and t2.limit_type_code ='SP619' and t1.cityflow is not null then '1'
		when t2.product_code = 'SE002401' and t2.limit_type_code ='T55'   and t1.cityflow is not null then '1'
		when t2.product_code = 'SE0004'   and t2.limit_type_code ='T6'    and t1.cityflow is not null and t2.limit_tag = 'SP6' and t2.city_flow is not null then '1'
		else '0' end as is_air_waybill */  -- 是否航空件
		
		 -- 20230629新增,新增 SE0153,SE0005 两个航空产品 ，剔除路由代码T6,ZT6
       ,case when t2.product_code in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005') 
		and t1.cityflow is not null and (nvl(t2.route_code,'') not in ('T6','ZT6') or t2.route_code is null) then '1'
            when t2.product_code='SE0004' and t2.limit_tag = 'SP6' and t1.cityflow is not null and (nvl(t2.route_code,'') not in ('T6','ZT6') or t2.route_code is null) then '1'
            when t2.product_code = 'SE0008' and t2.limit_tag in('T4','T801') and t1.cityflow is not null and (nvl(t2.route_code,'') not in ('T6','ZT6') or t2.route_code is null) then '1'
      else '0'
     end as is_air_waybill
	,case when t1.cityflow is not null
		  then '1'
		  else '0'
	end as is_air
-- from dm_ordi_predict.cf_air_list t1
 from dm_ordi_predict.cf_air_list_backup20230627 t1 -- 20230629新增 
right join 
tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp1_backup20230629 t2
on t1.cityflow=t2.city_flow;


---------------------------- id： 762589 （依赖448419）----------------------------


set hive.auto.convert.join=true;  -- 预防倾斜


-- 判断是否特色经济
-- 1、去掉临时落表 2、修改引擎为fspark
drop table if exists tmp_ordi_predict.tmp_shenhy_air_special_econ_tmp4_backup20230629;
create table tmp_ordi_predict.tmp_shenhy_air_special_econ_tmp4_backup20230629 
stored as parquet as  
select
	t1.*
	,t4.pro_name
	,t4.level_1_type
	,t4.level_2_type
	,t2.dept_name as src_city_name
	,t3.dept_name as dest_city_name
-- from tmp_ordi_predict.tmp_shenhy_air_special_econ_tmp2 t1
from tmp_ordi_predict.tmp_shenhy_air_special_econ_tmp2_backup20230629 t1
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
)t4 on t1.waybill_no=t4.waybill_no
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




---------------------------- id： 452264  ----------------------------



set hive.execution.engine=tez; 
set tez.queue.name=${queue};
set hive.auto.convert.join=true;  -- 预防倾斜

drop table if exists tmp_ordi_predict.tmp_shenhy_air_special_econ_tmp5_backup20230629;
create table tmp_ordi_predict.tmp_shenhy_air_special_econ_tmp5_backup20230629 
stored as parquet as  
select
	to_date(consigned_tm) as consigned_dt
	,city_flow
	,src_city_name
	,dest_city_name
	,is_air
	,pro_name
	,level_1_type
	,level_2_type
	,count(waybill_no) as all_waybill_num
	,sum(quantity)     as all_quantity_num
	,sum(meterage_weight_qty) as all_weight
	,avg(meterage_weight_qty) as avg_weight
	,percentile_approx(meterage_weight_qty,0.5) as mid_weight
    
    ,sum(volume) as all_volume_num
	,avg(volume) as avg_volume
	,percentile_approx(volume,0.5) as mid_volume


-- from tmp_ordi_predict.tmp_shenhy_air_special_econ_tmp4
from tmp_ordi_predict.tmp_shenhy_air_special_econ_tmp4_backup20230629
-- where pro_name is not null
group by to_date(consigned_tm)  
	,city_flow
	,src_city_name
	,dest_city_name
	,is_air
	,pro_name
	,level_1_type
	,level_2_type;
	
	
drop table if exists tmp_ordi_predict.tmp_shenhy_air_special_econ_tmp6_backup20230629;
create table tmp_ordi_predict.tmp_shenhy_air_special_econ_tmp6_backup20230629 
stored as parquet as  
select
	to_date(consigned_tm) as consigned_dt
	,city_flow
	,src_city_name
	,dest_city_name
	,is_air
	,pro_name
	,level_1_type
	,level_2_type
	,count(distinct waybill_no) as air_waybill_num
	,sum(quantity)     as air_quantity_num
	,sum(meterage_weight_qty) as air_sum_weight
	,avg(meterage_weight_qty) as air_avg_weight
	,percentile_approx(meterage_weight_qty,0.5) as air_mid_weight
    ,sum(volume) as air_volume_num
	,avg(volume) as air_avg_volume
	,percentile_approx(volume,0.5) as air_mid_volume
-- from tmp_ordi_predict.tmp_shenhy_air_special_econ_tmp4
from tmp_ordi_predict.tmp_shenhy_air_special_econ_tmp4_backup20230629
where is_air_waybill='1'
-- and pro_name is not null
group by to_date(consigned_tm)  
	,city_flow
	,src_city_name
	,dest_city_name
	,is_air
	,pro_name
	,level_1_type
	,level_2_type;
	
drop table if exists tmp_ordi_predict.tmp_shenhy_air_special_econ_tmp7_backup20230629;
create table tmp_ordi_predict.tmp_shenhy_air_special_econ_tmp7_backup20230629 
stored as parquet as  
select
	t1.*
	,nvl(t2.air_waybill_num,0) as air_waybill_num
	,nvl(t2.air_quantity_num,0) as air_quantity_num
	,nvl(t2.air_sum_weight,0)   as air_sum_weight
	,nvl(t2.air_avg_weight,0)   as air_avg_weight
	,nvl(t2.air_mid_weight,0)   as air_mid_weight

    ,nvl(t2.air_volume_num,0)   as air_volume_num
	,nvl(t2.air_avg_volume,0)   as air_avg_volume
	,nvl(t2.air_mid_volume,0)   as air_mid_volume

-- from tmp_ordi_predict.tmp_shenhy_air_special_econ_tmp5  t1
from tmp_ordi_predict.tmp_shenhy_air_special_econ_tmp5_backup20230629  t1
left join 
-- tmp_ordi_predict.tmp_shenhy_air_special_econ_tmp6 t2
tmp_ordi_predict.tmp_shenhy_air_special_econ_tmp6_backup20230629 t2
on t1.consigned_dt=t2.consigned_dt
and t1.city_flow=t2.city_flow
and  nvl(t1.pro_name,'a')=nvl(t2.pro_name,'a')
and t1.level_1_type=t2.level_1_type
and t1.level_2_type=t2.level_2_type;



set hive.exec.dynamic.partition= true;
set hive.exec.dynamic.partition.mode= nostrict;
set hive.exec.max.dynamic.partitions=2000;  -- 所有执行节点上最多可以多少个动态分区
set hive.exec.max.dynamic.partitions.pernode = 500;

-- insert overwrite table dm_ordi_predict.dws_static_cityflow_special_econ_day partition (inc_day)
insert overwrite table dm_ordi_predict.dws_static_cityflow_special_econ_day_backup20230620 partition (inc_day)
select 
	 consigned_dt as days              
	 ,city_flow         
	 ,src_city_name     
	 ,dest_city_name    
	 ,is_air            
	 ,pro_name          
	 ,level_1_type      
	 ,level_2_type      
	 ,all_waybill_num   
	 ,all_quantity_num  
	 ,all_weight        
	 ,avg_weight        
	 ,mid_weight        
	 ,air_waybill_num   
	 ,air_quantity_num  
	 ,air_sum_weight    
	 ,air_avg_weight    
	 ,air_mid_weight
     
     ,all_volume_num
     ,avg_volume
     ,mid_volume

     ,air_volume_num
     ,air_avg_volume
     ,air_mid_volume
	,regexp_replace(consigned_dt,'-','') as inc_day
from  tmp_ordi_predict.tmp_shenhy_air_special_econ_tmp7_backup20230629;



---------------------------- id： 465148  ----------------------------





----- 航空五维度去掉特色经济部分----------------



set hive.execution.engine=tez;
-- 队列
set tez.queue.name=${queue};
-- 并行
set hive.exec.parallel=true;

drop table if exists tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp7_backup20230629;
create table tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp7_backup20230629    
stored as parquet as
select  
     t1.consigned_dt  as days            
    ,t1.city_flow        
    ,src_city_name    
    ,dest_city_name   
    ,type  
    ,case when  t2.city_flow is not null and type='特色经济'
          then  t1.air_waybill_num-t2.air_waybill_num
          else t1.air_waybill_num
    end as   air_waybill_num
     ,case when  t2.city_flow is not null and type='特色经济'
          then  t1.air_quantity_num-t2.air_quantity_num
          else t1.air_quantity_num
    end as   air_quantity_num
    ,case when  t2.city_flow is not null and type='特色经济'
          then  t1.air_sum_weight-t2.air_sum_weight
          else t1.air_sum_weight
    end as   air_sum_weight
    ,case when  t2.city_flow is not null and type='特色经济'
          then  (t1.air_avg_weight+t2.air_avg_weight)/2
          else t1.air_avg_weight
    end as   air_avg_weight
  ,case when  t2.city_flow is not null and type='特色经济'
          then  (t1.air_mid_weight+t2.air_mid_weight)/2
          else t1.air_mid_weight
    end as   air_mid_weight
    ,src_province
    ,dest_province
    ,t1.avg_weight
from   tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp6_1_backup20230629 t1
left join 
(select
      consigned_dt as days              
	 ,t2.city_flow             
	 ,sum(all_waybill_num) as all_waybill_num   
	 ,sum(all_quantity_num) as all_quantity_num
	 ,sum(all_weight) as     all_weight
	 ,avg(avg_weight)  as avg_weight      
	 ,avg(mid_weight) as        mid_weight 
	 ,sum(air_waybill_num) as    air_waybill_num
	 ,sum(air_quantity_num) as   air_quantity_num
	 ,sum(air_sum_weight) as air_sum_weight    
	 ,avg(air_avg_weight) as     air_avg_weight
	 ,avg(air_mid_weight) as air_mid_weight
from dm_ordi_predict.dm_spec_eco_pro_dtl t1                -- 特色经济项目表
 -- join tmp_ordi_predict.tmp_shenhy_air_special_econ_tmp7 t2   -- 特色经济表
  join tmp_ordi_predict.tmp_shenhy_air_special_econ_tmp7_backup20230629 t2   -- 特色经济表 
on t1.pre_month=substr(t2.consigned_dt,6,2)
and t1.pro_name =(case when t2.pro_name='闸蟹项目'
                        then '蟹项目'
                        else t2.pro_name
                        end) 
join (select cityflow from dm_ordi_predict.xss_special_air_rate a where inc_day='20220808' group by cityflow) t3   -- 20220808添加，大闸蟹只预测这些流向
on t2.city_flow=t3.cityflow
where t2.is_air='1'
group by consigned_dt          
	 ,t2.city_flow             
) t2
on t1.consigned_dt=t2.days
and t1.city_flow=t2.city_flow;



drop table if exists tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp8_backup20230629;
create table if not exists tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp8_backup20230629 
stored as parquet as
select
	days            
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
	,air_quantity_num*avg_weight as sum_avg_weight
from tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp7_backup20230629  t1; 

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=10000;	


insert overwrite table dm_ordi_predict.dws_air_flow_five_dims_day_sub_backup20230620 partition (inc_day)
select 
	 days            
    ,city_flow        
    ,src_city_name    
    ,dest_city_name   
    ,type            
    ,air_waybill_num
    ,air_quantity_num
    ,air_sum_weight
    ,air_avg_weight
    ,air_mid_weight
	,src_province as src_province_code
    ,dest_province as dest_province_code
	,avg_weight  
	,sum_avg_weight
	,regexp_replace(days,'-','') as inc_day
from  tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp8_backup20230629;


