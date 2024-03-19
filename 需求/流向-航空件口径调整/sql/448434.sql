
 ------------------------------------ ID:448409 ------------------------------------
--step1 获取航空流向运单明细
-- 20220908修改来源表
	-- ID：448409
drop table if exists tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp1_20230629;
create table tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp1_20230629 
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
and inc_day>='20210401';  -- ？？？


 ------------------------------------ ID:470197  -拆分托寄物------------------------------------
set hive.auto.convert.join=true;
 --托寄物去重
 set spark.sql.shuffle.partitions=600;
 set spark.sql.adaptive.enabled=false;
drop table if exists tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp2_20230629;
create table tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp2_20230629  
stored as parquet as
select  
	cons_name_concat
	,upper(cons_name_concat) as cons_name_concat_up
	,min(inc_day) as inc_day 
from tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp1_20230629
where inc_day between '$[time(yyyyMMdd,-10d)]' and '$[time(yyyyMMdd,-1d)]'
group by 
	cons_name_concat
	,upper(cons_name_concat);
	
------------------------------------ ID:496536  -- 关联配置表------------------------------------

drop table if exists tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp3_20230629;
create table if not exists tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp3_20230629
stored as parquet as
select 
       t.cons_name_concat,
       fetch_data,
       cast(c.priority_no as int)   priority_no,            --优先级
       c.broad_heading,                                     --大类
       subdivision                                          --小类
  from  dm_bie.tm_consign_info c  
  join tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp2_20230629 t
  -- join dm_bie.tm_consign_info c 
  where  c.fetch_type = '包含' 
  and instr(t.cons_name_concat_up,upper(c.fetch_data)) > 0
  union all 
  select 
       t.cons_name_concat,
       fetch_data,
       cast(c.priority_no as int)   priority_no,            --优先级
       c.broad_heading,                                     --大类
       subdivision                                          --小类
  from dm_bie.tm_consign_info c 
  join 
  tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp2_20230629  t
  -- join dm_bie.tm_consign_info c 
  on  c.fetch_type = '等于' 
  and cons_name_concat_up = upper(c.fetch_data);
  
  
  
   ------------------------------------ ID:470204 ------------------------------------
  
-----配置完插入中间表

set hive.execution.engine=tez;
-- 队列
set tez.queue.name=root.predict;

drop table if exists tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp4_20230629;
create table tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp4_20230629  
stored as parquet as
select 
	   cons_name_concat	,
      fetch_data,
      priority_no,
      broad_heading	,
      subdivision 
from tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp3_20230629 t
where t.fetch_data not in ('苹果','小米')
 or (t.fetch_data='苹果' 
 and upper(cons_name_concat) 
 not rlike '手机|电脑|U盘|配件|膜|智能|数码|电器|数据线|无线｜AIR|IPHONE|IAPD|PLUS|APPLE|6|7|8|9|10|11|白色|金色|银色|绿色|蓝色|橘黄色|黑色|粉色')
or (t.fetch_data='小米' and substr(cons_name_concat,-2)='小米');


drop table if exists tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp5_0_20230629;
create table tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp5_0_20230629
stored as parquet as
select t.waybill_no,                                        --运单号    
       t.product_code,
       t.source_zone_code,                                  --原寄件网点
	   t.dest_zone_code,
       t.consigned_tm,                                      --寄件时间
       t.cons_name,                                         --托寄物内容
       t.cons_name_concat,
       fetch_data,
       priority_no,                                         --优先级
       broad_heading,                                       --大类
       case when t.product_code='SE0034' and subdivision is null then '蟹类' else subdivision end as subdivision, --小类
       meterage_weight_qty,                                  --计费总重量
	   quantity,                                             --包裹总件数
	   freight_monthly_acct_code as monthly_acct_code,               --运费月结账号
    --     signin_tm
	--    t2.area_code
    t.src_area_code as area_code
  from tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp1_20230629  t  -- tmp_shenhy_special_eco_waybill_ref_tmp1
left join tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp4_20230629  b 
on trim(t.cons_name_concat)=trim(b.cons_name_concat) 
where t.inc_day between  '$[time(yyyyMMdd,-10d)]' and '$[time(yyyyMMdd,-1d)]' ;


set hive.exec.dynamic.partition= true;
set hive.exec.dynamic.partition.mode= nostrict;
set hive.exec.max.dynamic.partitions=2000;  -- 所有执行节点上最多可以多少个动态分区
set hive.exec.max.dynamic.partitions.pernode = 500;
-- 20230629 写入临时表
insert overwrite table dm_ordi_predict.dws_spec_consign_waybill_day_backup20230620 partition (inc_day)  
select 
  waybill_no                  
 ,product_code 	           
 ,source_zone_code            
 ,dest_zone_code				
 ,consigned_tm                
 ,cons_name                   
 ,cons_name_concat				
 ,fetch_data					
 ,priority_no                 
 ,broad_heading               
 ,subdivision                 
 ,meterage_weight_qty         
 ,quantity                    
 ,monthly_acct_code           
 ,'' as signin_tm 	               
 ,area_code 
,regexp_replace(to_date(consigned_tm),'-','') as inc_day 
from  tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp5_0_20230629
where subdivision is not null;



  
   ------------------------------------ ID:470206 ------------------------------------
   
   
   
set hive.execution.engine=tez;
-- 队列
set tez.queue.name=root.predict;
set hive.auto.convert.join=true;

---------------------关联项目信息----------------------
drop table if exists tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp6_backup20230620;
create table tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp6_backup20230620
stored as parquet as
select 
       a.waybill_no,                                        --运单号    
       a.source_zone_code,                                  --原寄件网点
	   a.area_code,
       b.PRO_NAME,
       b.LEVEL_1_TYPE,
       b.LEVEL_2_TYPE,
       b.LEVEL_3_TYPE,
      --  b.PRO_START,
      --  b.PRO_END,
       a.consigned_tm,                                      --寄件时间
       a.cons_name,                                         --托寄物内容
       cons_name_concat,
       fetch_data,
       a.priority_no,                                         --托寄物优先级
       broad_heading,                                       --大类
       subdivision,                                         --小类
	 --   b.estimate_inc,
	 --   b.inc_unit,
	   b.priority_no as pro_prio_no,                        --项目优先级
	   a.dest_zone_code,
       meterage_weight_qty,                                  --计费总重量
	   quantity,                                             --包裹总件数
	   monthly_acct_code,                                    --运费月结账号
       product_code,
       signin_tm,
       inc_day
 from (select 
            area_code
            ,PRO_NAME
            ,LEVEL_2_TYPE
            ,priority_no
            ,inc_unit
            ,LEVEL_1_TYPE
            ,LEVEL_3_TYPE
 from dm_bie.special_eco_pro 
 where pro_year='2022'
 group by area_code
            ,PRO_NAME
            ,priority_no
            ,inc_unit
            ,LEVEL_1_TYPE
            ,LEVEL_2_TYPE
            ,LEVEL_3_TYPE)b 
 join dm_ordi_predict.dws_spec_consign_waybill_day_backup20230620 a
 -- tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp5 a
-- inner join  
on a.area_code=b.area_code 
and a.subdivision=b.LEVEL_2_TYPE
where a.subdivision is not null 
-- and a.inc_day between '20180701' and '20180901'
and a.inc_day between '$[time(yyyyMMdd,-10d)]' and  '$[time(yyyyMMdd,-1d)]';


   ------------------------------------ ID:470207 ------------------------------------


set hive.execution.engine=tez;
-- 队列
set tez.queue.name=root.predict;
set hive.auto.convert.join=true;

drop table if exists tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp7_backup20230620;
create table tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp7_backup20230620
stored as parquet as
select 
	t1.*
from (
select 
	t1.*
	 ,row_number() over(partition by waybill_no order by cast(pro_prio_no as int)) rn  
from tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp6_backup20230620 t1) t1
where rn=1;

  
set hive.exec.dynamic.partition= true;
set hive.exec.dynamic.partition.mode= nostrict;
set hive.exec.max.dynamic.partitions=2000;  -- 所有执行节点上最多可以多少个动态分区
set hive.exec.max.dynamic.partitions.pernode = 500;
insert overwrite table dm_ordi_predict.dws_spec_eco_pro_waybill_day_backup20230620 partition (inc_day)
select 
      waybill_no,                                        --运单号    
      source_zone_code,                                  --原寄件网点
      PRO_NAME,
      LEVEL_1_TYPE,
      LEVEL_2_TYPE,
      LEVEL_3_TYPE,
      inc_day                             
from tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp7_backup20230620;



   ------------------------------------ ID:448419 ------------------------------------
   
   
   
   