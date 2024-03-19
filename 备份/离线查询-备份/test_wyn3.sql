SELECT  data_version,  feature_version,  model_version,  predict_datetime,  origin_code,  dest_code,  origin_name,  dest_name,  object_type_code,  object_type,  weight_level,  product_type,  predict_quantity,  predict_waybill,  predict_weight,  predict_volume,  record_time,  '' as operation_type,  src_area_code,  src_fbq_code,  src_hq_code,  dest_area_code,  dest_fbq_code,  dest_hq_code,  partition_key
FROM  dm_predict.bfms_fc_predict_real_flow_snapshot2022
where  snapshot_time = '20230701'  and cast(predict_datetime as varchar) = '2023-07-01' 
-- and partition_key = '20230701'  -- 614803  -- 89208  -- 913  -- 1  301,  127,  122 ‬
 -- 614803
select  distinct partition_key
FROM  dm_predict.bfms_fc_predict_real_flow_snapshot2022
where  snapshot_time = '20230701'  and cast(predict_datetime as varchar) = '2023-07-01'  and partition_key = '20230701' 
-- 704483
select  count(1)
from  dm_predict.dws_fc_flow_predict_collecting_di
where  partition_key = '20230701' -- 301126680

select  count(1)
from  dm_predict.dws_fc_flow_predict_collecting_di

select  count(1)
from  dm_predict.dws_fc_flow_predict_collecting_di_backup20230706
where  partition_key = '20230701' 
--
  
   -------------------------------- oms 刷历史  20230610 数据 ----------------------------------------------------
select routeproducttypecode from dm_ordi_predict.dwd_pis_route_info_v2_df
where inc_day = '20230712'

select  inc_day,inc_dayhour,sum(air_order_num) as air_order_num, sum(all_order_num) as all_order_num 
 from dm_ordi_predict.dws_cityflow_dynamic_order_hi_backup20230620
where  inc_day = '20230713'
and inc_dayhour = '2023071311'
group by inc_day,inc_dayhour


select  inc_day,inc_dayhour,sum(air_order_num) as air_order_num, sum(all_order_num) as all_order_num 
 from dm_ordi_predict.dws_cityflow_dynamic_order_hi
where  inc_day = '20230713'
and inc_dayhour = '2023071311'
group by inc_day,inc_dayhour




select
  *
from
  dwd.dwd_waybill_info_dtl_di
where
  -- 2226
select
  count(1)
from
  dm_kafka_rdmp.dm_full_order_dtl_df
where
  inc_day = '20230610'
  and (
    waybillno is null
    or waybillno = ''
  ) --245022269
select
  count(1)
from
  dm_kafka_rdmp.dm_full_order_dtl_df
where
  inc_day = '20230610' 
  
  -- 31198636
  -- 31188651
  -- 31802490
  -- 去最近45天的数据， 开窗函数
  245022269
select
  count(1)
from
  (
      select a.waybillno,b.route_code from
      (select * from dm_kafka_rdmp.dm_full_order_dtl_df where inc_day = '20230610') a
      left join 
      (select * from
          (select waybill_no,route_code,row_number() over(partition by waybill_no) as rn
            from dwd.dwd_waybill_info_dtl_di
            where inc_day between '$[time(yyyyMMdd,-30d)]' and '$[time(yyyyMMdd,-0d)]' and route_code != '' and route_code is not null
          ) a1
        where a1.rn = 1
      ) b on a.waybillno = b.waybill_no

  ) al
where
  al.route_code is null 


select a.*,if(a.waybill_no is null or a.waybill_no = '' , b.waybill_no,a.waybill_no) as waybill_no_new from 
(select * from dm_kafka_rdmp.dm_full_order_dtl_df where inc_day = '20230610') a 
 left join 
(select al.waybill_no,al.order_no from 
   (SELECT waybill_no,order_no ,row_number() over(partition by order_no) as rn  FROM ods_shiva_oms.tt_waybill_no_to_order_no
    where inc_day between '$[time(yyyyMMdd,-7d)]' and '$[time(yyyyMMdd,-0d)]' and waybill_no != '' and waybill_no is not null
    and order_no != '' and order_no is not null 
   ) al where al.rn = 1 
)  b 
on a.order_no = b.order_no
  

-- 最新获取路由代码  OMS
select
  count(1)
from
  (
      select a.waybillno,b.route_code from
      (
        select a.*,if(a.waybillno is null or a.waybillno = '' , b.waybill_no,a.waybillno) as waybillno_new from 
         (select * from dm_kafka_rdmp.dm_full_order_dtl_df where inc_day = '20230610') a 
          left join 
         (select al.waybill_no,al.order_no from 
           (SELECT waybill_no,order_no ,row_number() over(partition by order_no) as rn  FROM ods_shiva_oms.tt_waybill_no_to_order_no
            where inc_day between '$[time(yyyyMMdd,-7d)]' and '$[time(yyyyMMdd,-0d)]'
             and waybill_no != '' and waybill_no is not null and order_no != '' and order_no is not null 
           ) al where al.rn = 1 
         )  b 
          on a.orderno = b.order_no
      ) a
       left join 
      (select a1.waybill_no,a1.route_code from
          (select waybill_no,route_code,row_number() over(partition by waybill_no) as rn
            from dwd.dwd_waybill_info_dtl_di
            where inc_day between '$[time(yyyyMMdd,-30d)]' and '$[time(yyyyMMdd,-0d)]' and route_code != '' and route_code is not null
          ) a1
        where a1.rn = 1
      ) b on a.waybillno_new = b.waybill_no
  ) al
where
  al.route_code is null 


  SELECT waybill_no,order_no FROM ods_shiva_oms.tt_waybill_no_to_order_no
  where inc_day = '20230709'
  --27148830 22955
select
  count(1)
from
  dwd.dwd_waybill_info_dtl_di
where
  inc_day = '20230610'
  and (
    route_code = ''
    or route_code is null
  )

------------------------------- 航空件调整 -- 营运维度验证 ----------------------------------
-- 原表验证
select inc_day, sum(quantity) as quantity  from dm_ordi_predict.dws_air_flow_six_dims_waybill_dtl 
where inc_day between '20230601' and   '20230630'
group by inc_day

select inc_day, sum(quantity) as quantity  from dm_ordi_predict.dws_air_flow_six_dims_waybill_dtl_backup20230620 
where inc_day between '20230601' and   '20230630'
group by inc_day 

-- 特色经济验证
-- select inc_day, sum(quantity) as quantity  from dm_ordi_predict.dws_air_flow_six_dims_waybill_dtl 
-- where inc_day between '20230601' and   '20230630'
-- and if_econ_fresh = '1'
-- group by inc_day

-- select inc_day, sum(quantity) as quantity  from dm_ordi_predict.dws_air_flow_six_dims_waybill_dtl_backup20230620 
-- where inc_day between '20230601' and   '20230630'
-- and if_econ_fresh = '1'
-- group by inc_day

-- 航空5维度真实数据
select inc_day,sum(air_quentity_num) as air_quentity_num, sum(air_quentity_num) as air_quentity_num
 from dm_ordi_predict.dws_air_flow_six_dims_day
where inc_day between '20230601' and   '20230630'
group by inc_day

select inc_day,sum(air_quentity_num) as air_quentity_num, sum(air_quentity_num) as air_quentity_num
 from dm_ordi_predict.dws_air_flow_six_dims_day_backup20230620
where inc_day between '20230601' and   '20230630'
group by inc_day

--  营运维度表
select * from 
(
select inc_day,'dws_air_flow_six_dims_day_newhb' as table_name ,sum(air_quentity_num) as air_quentity_num
 from dm_ordi_predict.dws_air_flow_six_dims_day_newhb
where inc_day between '20230701' and   '20230727'
group by inc_day
) t1 
inner join 
(
    select inc_day,'dws_static_cityflow_base' as table_name2,sum(all_quantity_air) as all_quantity_air_base
    from dm_ordi_predict.dws_static_cityflow_base
    where inc_day between '20230701' and   '20230727'
    group by inc_day
) t2 
on t1.inc_day = t2.inc_day



 from dm_ordi_predict.dws_air_flow_six_dims_day_newhb_backup20230620
where inc_day between '20230601' and   '20230630'
group by inc_day

---- 

select inc_day,sum(air_quantity_num) as air_quentity_num ,sum(all_quantity_num) as all_quantity_num
 from dm_ordi_predict.dws_static_cityflow_special_econ_day
where inc_day between '20230601' and   '20230630'
-- and is_air = '1'
group by inc_day


 from dm_ordi_predict.dws_static_cityflow_special_econ_day_backup20230620
where inc_day between '20230601' and   '20230630'
-- and is_air = '1'
group by inc_day

----- 
select inc_day,sum(air_quentity_num) as air_quentity_num 
from dm_ordi_predict.dws_air_flow_five_dims_day_sub
where inc_day between '20230601' and   '20230630'
group by inc_day

select inc_day,sum(air_quentity_num) as air_quentity_num 
from dm_ordi_predict.dws_air_flow_five_dims_day_sub_backup20230620
where inc_day between '20230601' and   '20230630'
group by inc_day



dm_ordi_predict.dws_air_flow_six_dims_day
dm_ordi_predict.dws_air_flow_six_dims_day_newhb 
dm_ordi_predict.dws_static_cityflow_special_econ_day
dm_ordi_predict.dws_air_flow_five_dims_day_sub




  ----------------------------------------- 大客户 ---------------------

  select * from dm_ordi_predict.dim_ka_company_warehouse_df
where  
-- inc_day = '20220402' and
 companyabbr='唯品会'



select 
companycustno
,max(custtype) as custtype
,max(companyname) as companyname
,max(companyabbr) as companyabbr
,max(custtypename) as custtypename
,max(createdate) as createdate
,max(invaliddate) as invaliddate
from dm_kadm.kadm_dim_cust_mapping
where  companyabbr = '唯品会'	
group by companycustno
 
 -- 历史无
 select monthly_acct_code,*  
from dm_ordi_predict.dim_all_company_warehouse_df 
where
--  inc_day = '$[time(yyyyMMdd,-1d)]'and
 monthly_acct_code in ('7580536719','7574177793','7550167472','7550273418','7550174169','200309690','7550171474','201062318','5720745526','7550176207','5125885826','7580536959','7550169681','294888758','225978034','7110218742','294885832','5325050569','200116002','7110218787','7550404559','200171454','201879375','235393412','7110219884','5125891282','7550273414','225981140','7550404560','5720744384','7550179671','7110218867','7550419651','244478361','201879381','7550173850','7580536918','225978033','7110218847','244482993','7110218875','7550178832','7580537145','7110219805','7550209934','7550224819','201879351','7110219806','7550169677','7110219807','286798133','7550336016','244480251','225975839','7550247656','3712671661','7550203012','7550167651','286794523','7550167649','7550224772','7550233448','7550176719','7110218834','5125882630','7550177433','286791771','3712677048','7580536941','7110219808','7550167650','7580536917','294883812','7550187315','7550310941','7110218892','7580536697','7550169676')

-- oms 
    -- 过滤  --历史无
    -- 全表  --历史无
select account from ods_oms.wom_tb_company_account   -- 734619/蒋雨青
where    
--     is_default_account = 1  and
--    customer_status = 1  and
   account in  ('7580536719','7574177793','7550167472','7550273418','7550174169','200309690','7550171474','201062318','5720745526','7550176207','5125885826','7580536959','7550169681','294888758','225978034','7110218742','294885832','5325050569','200116002','7110218787','7550404559','200171454','201879375','235393412','7110219884','5125891282','7550273414','225981140','7550404560','5720744384','7550179671','7110218867','7550419651','244478361','201879381','7550173850','7580536918','225978033','7110218847','244482993','7110218875','7550178832','7580537145','7110219805','7550209934','7550224819','201879351','7110219806','7550169677','7110219807','286798133','7550336016','244480251','225975839','7550247656','3712671661','7550203012','7550167651','286794523','7550167649','7550224772','7550233448','7550176719','7110218834','5125882630','7550177433','286791771','3712677048','7580536941','7110219808','7550167650','7580536917','294883812','7550187315','7550310941','7110218892','7580536697','7550169676')
;
-- 7550273414  只有一条
select monthly_account from dm_elog_dwh.dim_company_warehouse_account   -- 01406122
where monthly_account in  ('7580536719','7574177793','7550167472','7550273418','7550174169','200309690','7550171474','201062318','5720745526','7550176207','5125885826','7580536959','7550169681','294888758','225978034','7110218742','294885832','5325050569','200116002','7110218787','7550404559','200171454','201879375','235393412','7110219884','5125891282','7550273414','225981140','7550404560','5720744384','7550179671','7110218867','7550419651','244478361','201879381','7550173850','7580536918','225978033','7110218847','244482993','7110218875','7550178832','7580537145','7110219805','7550209934','7550224819','201879351','7110219806','7550169677','7110219807','286798133','7550336016','244480251','225975839','7550247656','3712671661','7550203012','7550167651','286794523','7550167649','7550224772','7550233448','7550176719','7110218834','5125882630','7550177433','286791771','3712677048','7580536941','7110219808','7550167650','7580536917','294883812','7550187315','7550310941','7110218892','7580536697','7550169676')
-- noms 
    -- 7550273414  历史全表只有一条
    --  历史 加过滤条件无
select distinct(monthly_account) from ods_noms.basic_company_monthly_account  ca     -- 734619/蒋雨青
where 
ca.is_default_monthly_account = 1 and
   ca.monthly_account_type = 1 and
   ca.account_status = 1 and
--    ca.inc_day= '$[time(yyyyMMdd,-1d)]'   and
   monthly_account in  ('7580536719','7574177793','7550167472','7550273418','7550174169','200309690','7550171474','201062318','5720745526','7550176207','5125885826','7580536959','7550169681','294888758','225978034','7110218742','294885832','5325050569','200116002','7110218787','7550404559','200171454','201879375','235393412','7110219884','5125891282','7550273414','225981140','7550404560','5720744384','7550179671','7110218867','7550419651','244478361','201879381','7550173850','7580536918','225978033','7110218847','244482993','7110218875','7550178832','7580537145','7110219805','7550209934','7550224819','201879351','7110219806','7550169677','7110219807','286798133','7550336016','244480251','225975839','7550247656','3712671661','7550203012','7550167651','286794523','7550167649','7550224772','7550233448','7550176719','7110218834','5125882630','7550177433','286791771','3712677048','7580536941','7110219808','7550167650','7580536917','294883812','7550187315','7550310941','7110218892','7580536697','7550169676')

select monthly_account from dm_elog_dwh.dim_company_warehouse_account

select companyabbr,count(1) from dm_ordi_predict.dim_ka_company_warehouse_df
where  inc_day = '20230710'
group by companyabbr
-- show partitions dm_ordi_predict.dim_ka_company_warehouse_df


select * from ods_oms.wom_tb_warehouse


------------------------------------------------- 营运维度 特色经济查验 --------------------------------------
select sum(all_waybill_num) as all_waybill_num ,
sum(all_quantity_num) as all_quantity_num,
sum(air_waybill_num) as air_waybill_num,
sum(air_quantity_num) as air_quantity_num 
 from dm_ordi_predict.dws_static_cityflow_special_econ_day
 where inc_day between '20210301' and '20210601'
 group by inc_day 


select 
inc_day,
sum(air_waybill_num) as air_waybill_num,
sum(air_quentity_num) as air_quentity_num
 from dm_ordi_predict.dws_air_flow_six_dims_day_newhb 
where  inc_day between '20210301' and '20210601'
and type = '特色经济'
group by inc_day 

select 
inc_day,
sum(air_waybill_num) as air_waybill_num,
sum(air_quentity_num) as air_quentity_num
 from dm_ordi_predict.dws_air_flow_six_dims_day_newhb_backup20230620
where  inc_day between '20210301' and '20210601'
and type = '特色经济'
group by inc_day 



---------------------------------------- 营运维度 - 特色经济 替换表 ------------------------------------

select
secondPlanArriveTm,destPlanArriveTm,thirdPlanArriveTm,requireId
from dm_heavy_cargo.rt_vehicle_task_monitor_for_not_send_detail4
where inc_day = '20230710' and requireid =  '230710015857638' limit 10

-- 2022  2789
select pro_year,count(1) from dm_bie.special_eco_pro group by pro_year 
-- 2022  2710
select count(1) from (
select 
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
            ,LEVEL_3_TYPE
) al 
 -- 分组 2710
select count(1) from (
select area_code,LEVEL_2_TYPE,count(1)
 from dm_bie.special_eco_pro
 where pro_year='2022'
group by area_code,LEVEL_2_TYPE
) al 
-- null 
select area_code,LEVEL_3_TYPE
 from dm_bie.special_eco_pro
 where pro_year='2023'
and LEVEL_3_TYPE is not null and LEVEL_3_TYPE != '' 

-- 2022 9046
-- 2023 9001 
select pro_year,count(1) from dm_bie.special_eco_pro_2022new group by pro_year 
-- 2022 9046
select count(1) from (
select 
            area_code
            ,PRO_NAME
            ,LEVEL_2_TYPE
            --,priority_no
            ,inc_unit
            ,LEVEL_1_TYPE
            ,LEVEL_3_TYPE
 from dm_bie.special_eco_pro_2022new 
 where pro_year='2022'
 group by area_code
            ,PRO_NAME
           -- ,priority_no
            ,inc_unit
            ,LEVEL_1_TYPE
            ,LEVEL_2_TYPE
            ,LEVEL_3_TYPE
) al 
-- 881  area_code ,LEVEL_2_TYPE  2022年
-- 9046  area_code ,LEVEL_3_TYPE  2022年
-- 9001  area_code ,LEVEL_3_TYPE  2023年 （全表共9001条）
select count(1) from ( 
select area_code,LEVEL_3_TYPE,count(1)
 from dm_bie.special_eco_pro_2022new 
 where pro_year='2023'
group by area_code,LEVEL_3_TYPE
) al 
-- 9001 2023年
select count(1) from 
(select 
    area_code
    ,PRO_NAME
    ,LEVEL_2_TYPE
    -- ,priority_no
    ,inc_unit
    ,LEVEL_1_TYPE
    ,LEVEL_3_TYPE
from dm_bie.special_eco_pro_2022new 
 where pro_year='2023'
 group by 
    area_code
    ,PRO_NAME
    ,LEVEL_2_TYPE
    -- ,priority_no
    ,inc_unit
    ,LEVEL_1_TYPE
    ,LEVEL_3_TYPE
  ) al   

/**
    1、关联字段 从 LEVEL_2_TYPE -> LEVEL_3_TYPE
    2、没有优先级字段
    3、前后数据有差异
**/
-- 旧数据 和新数据对比
select 
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
        ,LEVEL_3_TYPE

-- 测试上游  用一个运单 是否存在多个特色经济 
/*
    select * from (
    select a.area_code ,
            a.subdivision,
            count(distinct waybill_no) as num , 
            collect_set(waybill_no) as waybill_no_set 
    from dm_ordi_predict.dws_spec_consign_waybill_day a 
    where  a.inc_day between '$[time(yyyyMMdd,-10d)]' and  '$[time(yyyyMMdd,-1d)]'
    and  a.subdivision is not null 
    group by  a.area_code ,
            a.subdivision
    ) al where  size(al.waybill_no_set) > 1 
    limit 1000
*/
-- 62432782
select count(1) from dm_ordi_predict.dws_spec_consign_waybill_day a
where  a.inc_day between '$[time(yyyyMMdd,-10d)]' and  '$[time(yyyyMMdd,-1d)]'
and  a.subdivision is not null
-- 52706807 -- inner join 后数据条数
-- old 存在一个运单，对应多个优先级，
select * from (
select waybill_no,collect_set(pro_prio_no) as pro_prio_no_set
 from tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp6
group by waybill_no
) al where  size(al.pro_prio_no_set) > 2 
limit 100 
-- 20105070 运单去重后 (join 完 ，运单开窗取1条)
select count(1) from tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp7 

-- 原始表，运单是否有重复，存在
-- 62432782 --> 24868274 
select count(distinct waybill_no ) from dm_ordi_predict.dws_spec_consign_waybill_day a
where  a.inc_day between '$[time(yyyyMMdd,-10d)]' and  '$[time(yyyyMMdd,-1d)]'
and  a.subdivision is not null

-- 查看同一个运单，存在不同 area_code,subdivision
-- 512Y , 小龙虾/牛羊肉  ，
--  问题：
    -- 1、当前表的小类和新表的差异太大
            -- dm_bie.tm_consign_info 是否需要替换  （dm_bdp_datamining.spe_waybill_category）
select waybill_no,area_code,subdivision from dm_ordi_predict.dws_spec_consign_waybill_day a
where  a.inc_day between '$[time(yyyyMMdd,-10d)]' and  '$[time(yyyyMMdd,-1d)]'
--  and waybill_no = 'SF1622636142513'
   and subdivision = '花菜'
 and a.subdivision is not null

select * from 
 (select 
    area_code
    ,PRO_NAME
    ,LEVEL_2_TYPE
    -- ,priority_no
    ,inc_unit
    ,LEVEL_1_TYPE
    ,LEVEL_3_TYPE
from dm_bie.special_eco_pro_2022new 
 where pro_year='2023'
 group by 
    area_code
    ,PRO_NAME
    ,LEVEL_2_TYPE
    -- ,priority_no
    ,inc_unit
    ,LEVEL_1_TYPE
    ,LEVEL_3_TYPE
  ) al  
where al.area_code = '512Y'
and al.LEVEL_3_TYPE in ('小龙虾','牛羊肉') 
;

---------------------------------------- 航空动态 income -----------------------------------

select inc_day,count(1) from dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi 
group by inc_day 

select length(inc_dayhour) ,inc_dayhour from dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi 
where inc_day >= '20230101'
and  length(inc_dayhour) != 10

select inc_dayhour,sum(air_quantity_num) as air_quantity_num from dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi
where inc_dayhour 
in ('2022101724','2022101824','2022101924','2022102024','2022102124','2022102224')
group by inc_dayhour

select inc_day,inc_dayhour,count(1),sum(air_quantity_num) as air_quantity_num from  dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi 
where 
-- inc_day = '20220720'
-- and inc_dayhour = '2022072002'
-- substr(inc_day,1,4) = '2022'
inc_day between '20221001'  and  '20221231' 
-- and inc_dayhour not in ('20221018 24','20221019 24','2022082410.0')
group by inc_day,inc_dayhour

select * from dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi 
where
--  inc_day = '20220901' and
 inc_dayhour = '2022082403'

select * from dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi_backup20230714
where inc_day = '20230715'
and inc_dayhour='2023071523'

-- alter table dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi_backup20230714 drop partition(inc_day='20230715');


-- alter table dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi drop partition(inc_dayhour='20221018 24');
-- alter table dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi drop partition(inc_dayhour='20221019 24');
-- alter table dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi drop partition(inc_dayhour='2022082410.0');


show partitions dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi 


----------------------------------航空件调整 和OE对比 ---------------------------------

select inc_day,sum(quantity) from dwd_o.dwd_tp_air_waybillno_info_di
where inc_day between '20230713' and '20230713' 
-- and is_valid_on_air = '是'
-- and is_on_air = '是'
group by inc_day 

select city_flow,consigned_tm,consigned_time,product_code,quantity,is_on_air,is_valid_on_air
 from  dwd_o.dwd_tp_air_waybillno_info_di
where inc_day = '20230713'
and city_flow = '631-412'

select
      inc_day,
    --   count(1),
    --   sum(all_waybill_num) as new_all_waybill_num,
    --   sum(all_quantity) as new_all_quantity,
    --   sum(all_waybill_num_air) as new_all_waybill_num_air,
      sum(all_quantity_air) as new_all_quantity_air 
    from
      dm_ordi_predict.dws_static_cityflow_base_backup20230620
    where
      inc_day between '20230701' and '20230726' 
      and is_air = '1'
    group by
      inc_day


select inc_day,sum(quantity) from dwd_o.dwd_tp_air_waybillno_info_di
where inc_day between '20230701' and '20230726' 
group by inc_day 

select
    inc_day,
    sum(all_quantity_air) as new_all_quantity_air 
from
    dm_ordi_predict.dws_static_cityflow_base
where
    inc_day between '20230501' and '20230530'
group by
    inc_day


-- citylfow 对比
select city_flow,consigned_tm,consigned_time,product_code,quantity,is_on_air,is_valid_on_air
 from  dwd_o.dwd_tp_air_waybillno_info_di
where inc_day = '20230713'

-- 2249776.0
select * from (
select city_flow,sum(quantity) as oe_quantity
 from  dwd_o.dwd_tp_air_waybillno_info_di
where inc_day = '20230713'
group by city_flow
) a 
full outer join  
(select cityflow ,sum(all_quantity_air) as new_all_quantity_air  
from dm_ordi_predict.dws_static_cityflow_base_backup20230620
where  inc_day = '20230713'
-- and is_air = '1'
group by cityflow 
) b 
on a.city_flow = b.cityflow 
where  abs(a.oe_quantity- b.new_all_quantity_air) > 0 
-- or (a.city_flow is null )
-- or (b.cityflow  is null )

select city_flow as cityflow,expiry_dt from dm_pass_atp.tm_air_flow_config_wide
where inc_day =  '$[time(yyyyMMdd)]'
and is_air_flow = 1
and expiry_dt >= '$[time(yyyy-MM-dd)]'
;


select city_flow,consigned_tm,consigned_time,product_code,quantity,is_on_air,is_valid_on_air
 from  dwd_o.dwd_tp_air_waybillno_info_di
where inc_day = '20230717'
and city_flow = '479-471'
;
select cityflow,all_quantity_air
 from  dm_ordi_predict.dws_static_cityflow_base_backup20230620
where inc_day = '20230717'
and cityflow = '479-471'

select 
        concat(nvl(a.src_dist_code,'#'),'-',nvl(a.dest_dist_code,'#')) as cityflow,
        a.consigned_tm,
		a.product_code,
		round(case when upper(a.unit_weight)='LBS' then a.meterage_weight_qty*0.45359237
                   when upper(a.unit_weight)='G' then a.meterage_weight_qty/1000 
                   else a.meterage_weight_qty 
        end,3) as meterage_weight_qty,
		a.src_area_code,
		a.src_hq_code,
		a.limit_type_code,
		a.cargo_type_code,
        a.limit_tag,
		a.volume,
		a.waybill_no,
		a.quantity,
        a.route_code 
from
	dwd.dwd_waybill_info_dtl_di a
	where a.inc_day = '20230713'
	and substr(a.consigned_tm,0,10) = '2023-07-13'
    and (a.src_hq_code<>'CN39' or a.src_hq_code is null)  -- 去丰网 20230608修改
	and (nvl(a.source_zone_code,'') 
    in (select dept_code from dim.dim_department where hq_code<>'CN39' and dept_code is not null group by dept_code ) or a.source_zone_code is null )
    and concat(nvl(a.src_dist_code,'#'),'-',nvl(a.dest_dist_code,'#'))  = '020-510'
    and product_code in (
        'SE0001',
        'SE000201',
        'SE0109',
        'SE0121',
        'SE000206',
        'SE0103',
        'SE0008',
        'SE0137',
        'SE0107',
        'SE0152',
        'SE0146',
        'SE0089',
        'SE0004',
        'SE0051',
        'SE0153',
        'SE0005'
      )
      and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null) 

------------------------------------ 航空配置表 更换 ----------------
/*
1、流向表：dm_pass_atp.tm_air_flow_config_wide
	寄件城市-派件城市=city_flow 且 is_air_flow=是 且 expiry_dt>=T
*/
select city_flow as cityflow,expiry_dt from dm_pass_atp.tm_air_flow_config_wide
where inc_day =  '$[time(yyyyMMdd)]'
and is_air_flow = 1
and expiry_dt >= '$[time(yyyy-MM-dd)]'

select * from 
(select * from dm_ordi_predict.cf_air_list_backup20230627 ) a -- 91416
right join 
(select city_flow as cityflow  from dm_pass_atp.tm_air_flow_config_wide
where inc_day =  '$[time(yyyyMMdd)]'
and is_air_flow = 1
and expiry_dt >= '$[time(yyyy-MM-dd)]'
) b 
on a.cityflow = b.cityflow
where  a.cityflow is null 
or b.cityflow is null 
/**
产品表：dm_pass_atp.tm_air_product_config
	type=1 且 产品代码+路由代码=product_code+sop_label 且 路由代码！=exclude_route_code 且 expiry_dt>=T
**/
select  product_code, sop_label as limit_tag , exclude_route_code,expiry_dt from dm_pass_atp.tm_air_product_config
where inc_day = '$[time(yyyyMMdd)]'
and type = 1
and expiry_dt >= '$[time(yyyy-MM-dd)]'


--- 质量监控

select
  t1.num_1 - t2.num_2 as diff_num
from
  (
    SELECT
      '1' as flag,
      count(1) as num_1
    FROM
      dm_ordi_predict.cf_air_list_backup20230627
  ) t1
  inner join (
    select
      '1' as flag,
      count(1) as num_2
    from
      (
        SELECT
          cityflow
        FROM
          (
            select city_flow as cityflow from dm_pass_atp.tm_air_flow_config_wide
            where inc_day =  '$[time(yyyyMMdd)]'
            and is_air_flow = 1
            and expiry_dt >= '$[time(yyyy-MM-dd)]'
            UNION ALL
            SELECT
              cityflow
            FROM
              dm_ordi_predict.cf_air_list_backup20230627
          ) data
        GROUP BY
          cityflow
    ) al
  ) t2 on t1.flag = t2.flag;

  -- 流向 


----------------------------------------------- 特快件改造 ---------------------------------------

-- 历史数据对比

select inc_day,sum(tekuai_quantity) as tekuai_quantity ,sum(tekuai_waybill_num) as tekuai_waybill_num from 
dm_ordi_predict.dws_static_cityflow_tekuai_base
where inc_day between '20230604' and '20230630' 
and is_air_flow = '1'
group by inc_day 


select inc_day,sum(all_quantity) as all_quantity,sum(all_waybill_num) as all_waybill_num   from 
dm_ordi_predict.dws_static_his_cityflow
where inc_day between '20230604' and '20230630' 
and is_air = '1'
group by inc_day 

-- 新逻辑 剔除航空件
select inc_day,sum(tekuai_quantity) as tekuai_quantity ,sum(tekuai_waybill_num) as tekuai_waybill_num from 
dm_ordi_predict.dws_static_cityflow_tekuai_base_backup20230620
where inc_day between '20230604' and '20230630' 
and is_air_flow = '1'
group by inc_day 


select inc_day,sum(all_quantity) as all_quantity,sum(all_waybill_num) as all_waybill_num   from 
dm_ordi_predict.dws_static_his_cityflow_backup20230620
where inc_day between '20230604' and '20230630' 
and is_air = '1'
group by inc_day 


--------------------------------航空动态-数据异常bug 历史数据补全 -------------------

show partitions dm_ordi_predict.dws_air_flow_dynamic_pro_qty_hi_backup20230620
show partitions dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi_backup20230620

show partitions dm_ordi_predict.dws_air_flow_dynamic_pro_qty_hi_backup20230622
show partitions dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi_backup20230622

show partitions dm_ordi_predict.dws_air_flow_dynamic_pro_qty_hi_backup20230625
show partitions dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi_backup20230625

-- 对比 0622 和 0625  ， 2023-06-27 1:00 ~ 2023-07-19 11:00 
-- 历史数据是从20221010 开始 
select inc_day,sum(all_quantity_num) from dm_ordi_predict.dws_air_flow_dynamic_pro_qty_hi_backup20230622
where inc_day between '20230627' and '20230719' 
group by inc_day

select inc_day,inc_dayhour,sum(all_quantity_num) from dm_ordi_predict.dws_air_flow_dynamic_pro_qty_hi_backup20230622
where inc_day between '20230627' and '20230627' 
group by inc_day,inc_dayhour

select inc_day,sum(all_quantity_num) from dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi_backup20230622
where inc_day between '20230627' and '20230719' 
group by  inc_day

select  inc_day,inc_dayhour,sum(all_quantity_num) from dm_ordi_predict.dws_air_flow_dynamic_pro_qty_hi_backup20230625
where 
inc_day = '20230630'
-- inc_day between '20230627' and '20230719' 
group by  inc_day,inc_dayhour


select inc_dayhour,sum(all_quantity_num) from dm_ordi_predict.dws_air_flow_dynamic_pro_qty_hi_backup20230620
group by inc_dayhour

select inc_day,inc_dayhour,sum(all_quantity_num) from dm_ordi_predict.dws_air_flow_dynamic_pro_qty_hi_backup20230620
where  
-- inc_day = '20230712'
inc_dayhour = '2023071224'
group by  inc_day,inc_dayhour

select inc_day,inc_dayhour,sum(all_quantity_num)
from dm_ordi_predict.dws_air_flow_dynamic_pro_qty_hi_backup20230620
where  inc_day = '$[time(yyyyMMdd)]'
-- and inc_dayhour in (concat('$[time(yyyyMMdd)]','01'),concat('$[time(yyyyMMdd)]','02'))
group by inc_day,inc_dayhour

select inc_day,inc_dayhour,sum(all_quantity_num)
--  from dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi_backup20230620
from dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi
where  inc_day = '20230702'
group by  inc_day,inc_dayhour

select * from dm_kafka_rdmp.rmdp_waybill_basedata
    where inc_day ='20230626'

-- 0622
select inc_day,inc_dayhour,sum(all_quantity_num) from dm_ordi_predict.dws_air_flow_dynamic_pro_qty_hi_backup20230622
 where  inc_day = '20230719'
group by  inc_day,inc_dayhour



select inc_day,inc_dayhour,sum(all_quantity_num) from dm_ordi_predict.dws_air_flow_dynamic_pro_qty_hi_backup20230625
where  inc_day = '20230627'
group by  inc_day,inc_dayhour

----- pro 异常数据 -------

select * from dm_ordi_predict.dws_air_flow_dynamic_pro_qty_hi
where
 inc_day between '20221001' and '20221031'
-- inc_dayhour = '2022100124'


/*
    inc_day=20221004/inc_dayhour=20221004 24
    inc_day=20221005/inc_dayhour=20221005 24
    inc_day=20221007/inc_dayhour=20221007 24
    inc_day=20221008/inc_dayhour=20221008 24
    inc_day=20221009/inc_dayhour=20221009 24
    inc_day=20221010/inc_dayhour=20221010 24
    inc_day=20221011/inc_dayhour=20221011 24
    inc_day=20221012/inc_dayhour=20221012 24
    inc_day=20221013/inc_dayhour=20221013 24
    inc_day=20221014/inc_dayhour=20221014 24
    inc_day=20221015/inc_dayhour=20221015 24
    inc_day=20221016/inc_dayhour=20221016 24
    inc_day=20221017/inc_dayhour=20221017 24
    inc_day=20221018/inc_dayhour=20221018 24
inc_day=20221019/inc_dayhour=20221019 24
*/

-- alter table dm_ordi_predict.dws_air_flow_dynamic_pro_qty_hi drop partition(inc_dayhour='20221019 24');

----------------------------------- 航空件调整--复制历史数据至 新表 -------------------------------------
-- 1 、流向底表
show partitions dm_ordi_predict.dws_static_cityflow_base_backup20230620
show partitions dm_ordi_predict.dws_static_his_cityflow_backup20230620

select inc_day,count(1) from 
 dm_ordi_predict.dws_static_cityflow_base_backup20230620
where inc_day >= '20230701'
group by inc_day 

select inc_day,count(1) from 
 dm_ordi_predict.dws_static_his_cityflow_backup20230620
where inc_day >= '20230701'
group by inc_day 

-- 2、营运维度底表
show partitions dm_ordi_predict.dws_air_flow_six_dims_waybill_dtl_backup20230620   -- 20200101 ~ 20230706
show partitions dm_ordi_predict.dws_air_flow_six_dims_day_backup20230620
show partitions dm_ordi_predict.dws_air_flow_six_dims_day_newhb_backup20230620 
show partitions dm_ordi_predict.dws_static_cityflow_special_econ_day_backup20230620
show partitions dm_ordi_predict.dws_air_flow_five_dims_day_sub_backup20230620

--------------- oms 数据为null---------------------
-- 1、 20221122 之前的air_order_num 为0
  -- 20221001 ~ 20221121  air_order_num 都为0 
-- 验证备份表 -- 20221001数据
-- 1.1 2023-06-12 ~ 2023-07-12  oms 因process限制在每天，导致15:00~24:00数据重复
select* from dm_ordi_predict.dws_cityflow_dynamic_order_hi_backup20230801 where inc_day = '20221001'
-- 刷新前后对比
select * from 
(select inc_day,sum(all_order_num) as all_order_num_1, sum(air_order_num) as air_order_num_1
from dm_ordi_predict.dws_cityflow_dynamic_order_hi_backup20230801
 where inc_day  between '20221001' and '20221121'
 group by inc_day
) t1 
inner join 
(select inc_day,sum(all_order_num) as all_order_num, sum(air_order_num) as air_order_num 
from dm_ordi_predict.dws_cityflow_dynamic_order_hi
 where inc_day between '20221122' and '20221122'
 group by inc_day
) t2 
on t1.inc_day = t2.inc_day ;

select inc_day,inc_dayhour,sum(all_order_num) as all_order_num, sum(air_order_num) as air_order_num 
from dm_ordi_predict.dws_cityflow_dynamic_order_hi_backup20230724
 where inc_day between '20230702' and '20230702'
 group by inc_day ,inc_dayhour
-- 20221011   359638357
-- 20221013  3.51313845E8
select inc_day,inc_dayhour,sum(all_order_num) as all_order_num, sum(air_order_num) as air_order_num 
-- from dm_ordi_predict.dws_cityflow_dynamic_order_hi_backup20230724
from dm_ordi_predict.dws_cityflow_dynamic_order_hi
 where inc_day  between '20230731' and '20230731'
 group by inc_day,inc_dayhour
--  1.1.2  修复 oms 业务区表 


-- 查看小时打点是否积压
select inc_day ,all_order_num,count(1) from (
    select inc_day,inc_dayhour,sum(all_order_num) as all_order_num, sum(air_order_num) as air_order_num 
    from dm_ordi_predict.dws_cityflow_dynamic_order_hi
    where inc_day  between '20221001' and '20230731'
    group by inc_day,inc_dayhour
) al group by inc_day ,all_order_num
having count(1) > 1 

select is_air_order,count(1) from  tmp_ordi_predict.tmp_oms_cityflow_dynamic_order_1_byhour_tmp20230620_new1
group by is_air_order

select waybillno,productcode from tmp_ordi_predict.dm_full_order_dtl_df_by_day_tmp20230620_new1
-- 关联运单  ('SF1613888039383','SF1382662710876' ,'SF1389747216177','SF1637084467184')

-- 验证订单-产品为null，在运单中是否能查到
select al.waybillno_new,al.productcode,c.waybill_no,c.product_code,c.route_code,c.limit_tag from 
(select a.orderno,a.productcode,if(a.waybillno is null or a.waybillno = '' , b.waybill_no,a.waybillno) as waybillno_new from 
        (
            select * from dm_kafka_rdmp.dm_full_order_dtl_df where  inc_day = '20221012'
            AND from_unixtime(cast(processversiontm AS bigint) / 1000, 'yyyy-MM-dd HH:mm:ss') < '2022-10-12 08:00:00' 
        ) a 
    left join 
        (select al.waybill_no,al.order_no from 
            (SELECT waybill_no,order_no ,row_number() over(partition by order_no) as rn 
                FROM ods_shiva_oms.tt_waybill_no_to_order_no
                where inc_day between '20221002' and '20221012'
                and waybill_no != '' and waybill_no is not null
                and order_no != '' and order_no is not null 
            ) al where al.rn = 1 
        )  b 
    on a.orderno = b.order_no
)al 
 left join 
  (select a1.waybill_no,a1.product_code,a1.route_code,a1.limit_tag from
	  (select waybill_no,product_code ,route_code,limit_tag,row_number() over(partition by waybill_no) as rn
		from dwd.dwd_waybill_info_dtl_di
		where inc_day between '20221012' and '20221015'
        and product_code != '' and product_code is not null
	  ) a1
	where a1.rn = 1
  ) c on al.waybillno_new = c.waybill_no
where  c.product_code is not null and c.product_code != ''
-- 运单表
select waybill_no,product_code from dwd.dwd_waybill_info_dtl_di
where inc_day = '20221012' and 
 waybill_no in  ('SF1613888039383','SF1382662710876' ,'SF1389747216177','SF1637084467184')

-- 订单运单关系表
SELECT waybill_no,order_no 
FROM ods_shiva_oms.tt_waybill_no_to_order_no
where inc_day between '20221012' and '20221012'
and waybill_no != '' and waybill_no is not null
and order_no != '' and order_no is not null 

 select collect_set(waybill_no),product_code from dwd.dwd_waybill_info_dtl_di
where inc_day = '20221012' 
and product_code in ('SE0004','SE0008','SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
group by product_code 

-- SF1380978511272 , SF1381605518377
select waybill_no,product_code from dwd.dwd_waybill_info_dtl_di
where inc_day = '20221012' 
and product_code in ('SE0004','SE0008','SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
and product_code is not null and product_code != ''

-- 订单表
select productcode,LimitTypeCode, CargoTypeCode, ExpressTypeCode,limittag,processversiontm from dm_kafka_rdmp.dm_full_order_dtl_df 
where  inc_day = '20221012' 
and (processversiontm is null or processversiontm = '')
-- and productcode is not null and productcode != ''
-- and LimitTypeCode is not null and LimitTypeCode != ''
-- and CargoTypeCode is not null and CargoTypeCode != ''
-- and ExpressTypeCode is not null and ExpressTypeCode != ''

-- 回刷历史，添加处理时间限制
-- AND from_unixtime(cast(processversiontm AS bigint) / 1000, 'yyyy-MM-dd HH:mm:ss') < '20221012 01:00:00' 

-- 2023-06-07 才有数据
select * from dm_ordi_predict.dwd_pis_route_info_v2_df
where inc_day = '20221012'
-- and product_code is not null and product_code != ''
-- and routeproducttypecode is not null and routeproducttypecode != ''

show partitions dm_ordi_predict.dwd_pis_route_info_v2_df  -- 2023-06-07 才有数据
show partitions ods_shiva_oms.tt_waybill_no_to_order_no   -- 2017-01-01  才有数据
-- 发哥表
dm_ordi_predict.ods_pass_waybill_dtl_hi 

select * from ods_shiva_oms.tt_waybill_no_to_order_no 
where  inc_day = '20221012'

show partitions dm_ordi_predict.dws_cityflow_dynamic_order_hi

-- dm_ordi_predict.dws_cityflow_dynamic_order_hi 添加质量监控
    -- 航空订单量-大于0
    select
    inc_day,
    inc_dayhour,
    sum(air_order_num) as air_order_num
    from dm_ordi_predict.dws_cityflow_dynamic_order_hi
    where inc_day = '$[time(yyyyMMdd,-1h)]'
    and inc_dayhour = if(  '$[time(H)]' = 0, concat('$[time(yyyyMMdd,-1h)]', '24'), '$[time(yyyyMMddHH)]')
    group by inc_day,inc_dayhour

    -- 小时分区-格式校验
    select  count(distinct inc_dayhour) as partition_num from 
    dm_ordi_predict.dws_cityflow_dynamic_order_hi 
    --  dm_ordi_predict.dws_air_flow_dynamic_area_pro_qty_hi
    where  inc_dayhour= if('$[time(H)]' = 0,concat('$[time(yyyyMMdd,-1h)]','24'),'$[time(yyyyMMddHH)]')



--2 oms air_order_num 比 all_order_num 大？

dm_ordi_predict.dws_cityflow_dynamic_order_hi

-- 3、oms 分区异常，同时导致业务区表也异常  , √
/**
    -、20221012 没数据   ,oms + 业务区两张表
    -、异常数据  √
        inc_day=20230216/inc_dayhour=2023021524
        inc_day=20230217/inc_dayhour=2023021624
        inc_day=20230224/inc_dayhour=2023022324
        inc_day=20230228/inc_dayhour=2023022724
        inc_day=20230304/inc_dayhour=2023030324
        inc_day=20230316/inc_dayhour=2023031524
        inc_day=20230323/inc_dayhour=2023032224
        inc_day=20230325/inc_dayhour=2023032424
        inc_day=20230328/inc_dayhour=2023032724
        inc_day=20230329/inc_dayhour=2023032824
        inc_day=20230330/inc_dayhour=2023032924
        inc_day=20230401/inc_dayhour=2023033124
**/
select inc_day,inc_dayhour ,sum(all_order_num) as all_order_num ,sum(air_order_num) as air_order_num
from  dm_ordi_predict.dws_cityflow_dynamic_order_hi_backup20230724
where inc_day = '20221012'
group by inc_day ,inc_dayhour

show  partitions dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi_backup20230622

select inc_day,inc_dayhour ,sum(all_order_num) as all_order_num ,sum(air_order_num) as air_order_num
from  dm_ordi_predict.dws_cityflow_dynamic_order_hi
where
--  inc_dayhour in ('2023021524','2023021624','2023022324','2023022724','2023030324',
-- '2023031524','2023032224','2023032424','2023032724','2023032824','2023032924','2023033124') and
  inc_day != substr(inc_dayhour,1,8)
group by inc_day,inc_dayhour 

show partitions dm_ordi_predict.dws_cityflow_dynamic_area_order_hi
show partitions dm_ordi_predict.dws_cityflow_dynamic_order_hi_backup20230724

-- alter table dm_ordi_predict.dws_cityflow_dynamic_area_order_hi drop partition(inc_day='20230216',inc_dayhour='2023021524');
-- alter table dm_ordi_predict.dws_cityflow_dynamic_area_order_hi drop partition(inc_day='20230330',inc_dayhour='2023032924');
-- alter table dm_ordi_predict.dws_cityflow_dynamic_area_order_hi drop partition(inc_day='20230224',inc_dayhour='2023022324');
-- alter table dm_ordi_predict.dws_cityflow_dynamic_area_order_hi drop partition(inc_day='20230228',inc_dayhour='2023022724');
-- alter table dm_ordi_predict.dws_cityflow_dynamic_area_order_hi drop partition(inc_day='20230329',inc_dayhour='2023032824');
-- alter table dm_ordi_predict.dws_cityflow_dynamic_area_order_hi drop partition(inc_day='20230304',inc_dayhour='2023030324');
-- alter table dm_ordi_predict.dws_cityflow_dynamic_area_order_hi drop partition(inc_day='20230217',inc_dayhour='2023021624');
-- alter table dm_ordi_predict.dws_cityflow_dynamic_area_order_hi drop partition(inc_day='20230323',inc_dayhour='2023032224');
-- alter table dm_ordi_predict.dws_cityflow_dynamic_area_order_hi drop partition(inc_day='20230328',inc_dayhour='2023032724');
-- alter table dm_ordi_predict.dws_cityflow_dynamic_area_order_hi drop partition(inc_day='20230325',inc_dayhour='2023032424');
-- alter table dm_ordi_predict.dws_cityflow_dynamic_area_order_hi drop partition(inc_day='20230316',inc_dayhour='2023031524');
-- alter table dm_ordi_predict.dws_cityflow_dynamic_area_order_hi drop partition(inc_day='20230401',inc_dayhour='2023033124');

-- alter table dm_ordi_predict.dws_cityflow_dynamic_order_hi drop partition(inc_day='20230216',inc_dayhour='2023021524');
-- alter table dm_ordi_predict.dws_cityflow_dynamic_order_hi drop partition(inc_day='20230330',inc_dayhour='2023032924');
-- alter table dm_ordi_predict.dws_cityflow_dynamic_order_hi drop partition(inc_day='20230224',inc_dayhour='2023022324');
-- alter table dm_ordi_predict.dws_cityflow_dynamic_order_hi drop partition(inc_day='20230228',inc_dayhour='2023022724');
-- alter table dm_ordi_predict.dws_cityflow_dynamic_order_hi drop partition(inc_day='20230329',inc_dayhour='2023032824');
-- alter table dm_ordi_predict.dws_cityflow_dynamic_order_hi drop partition(inc_day='20230304',inc_dayhour='2023030324');
-- alter table dm_ordi_predict.dws_cityflow_dynamic_order_hi drop partition(inc_day='20230217',inc_dayhour='2023021624');
-- alter table dm_ordi_predict.dws_cityflow_dynamic_order_hi drop partition(inc_day='20230323',inc_dayhour='2023032224');
-- alter table dm_ordi_predict.dws_cityflow_dynamic_order_hi drop partition(inc_day='20230328',inc_dayhour='2023032724');
-- alter table dm_ordi_predict.dws_cityflow_dynamic_order_hi drop partition(inc_day='20230325',inc_dayhour='2023032424');
-- alter table dm_ordi_predict.dws_cityflow_dynamic_order_hi drop partition(inc_day='20230316',inc_dayhour='2023031524');
-- alter table dm_ordi_predict.dws_cityflow_dynamic_order_hi drop partition(inc_day='20230401',inc_dayhour='2023033124');

-- 4、oms数据异常
-- 20230701（周六）当天的oms小时打点数据，总量上看有些异常，比航空收件量低很多，而且也比20230702（周日）的航空订单量低
    -- 联系上游查看数据0701-0702 差异这么多的原因
select inc_day ,inc_dayhour,sum(all_order_num) as all_order_num ,sum(air_order_num) as air_order_num
from  dm_ordi_predict.dws_cityflow_dynamic_order_hi
where inc_day between '20230630' and '20230703'
group by inc_day,inc_dayhour


select inc_day ,sum(all_order_num) as all_order_num ,sum(air_order_num) as air_order_num
from  dm_ordi_predict.dws_cityflow_dynamic_order_hi_backup20230620
where inc_day between '20230630' and '20230703'
and substr(inc_dayhour,9,2) = '24'
group by inc_day

select inc_day, count(distinct orderid) as all_order_num from
dm_kafka_rdmp.dm_full_order_dtl_df
where inc_day between '20230630' and '20230703'
and iscancel= 0
group by inc_day   


-- 添加数据质量

select inc_day ,inc_dayhour ,sum(all_order_num) as all_order_num,
sum(air_order_num) as air_order_num
 from dm_ordi_predict.dws_cityflow_dynamic_area_order_hi
where  inc_day between '20221012' and '20221012'
group by inc_day ,inc_dayhour 

-- 11556164

 dm_ordi_predict.dws_air_flow_dynamic_area_pro_qty_hi
   
  sum(all_quantity_num) as all_quantity_num ,
  sum(air_mainwaybill_num) as air_mainwaybill_num ,
  sum(air_quantity_num) as air_quantity_num,


----------------------------- 航空动态 & oms 新增 业务区 ------------------

select  inc_day ,inc_dayhour,
  sum(all_mainwaybill_num) as all_mainwaybill_num,
  sum(all_quantity_num) as all_quantity_num ,
  sum(air_mainwaybill_num) as air_mainwaybill_num ,
  sum(air_quantity_num) as air_quantity_num,
  sum(all_wgt_num) as all_wgt_num,
  sum(air_wgt_num) as air_wgt_num
 from dm_ordi_predict.dws_air_flow_dynamic_pro_qty_hi_backup20230713
 where inc_day = '20221001'
group by inc_day ,inc_dayhour

select 
 inc_day ,inc_dayhour,
  sum(all_mainwaybill_num) as all_mainwaybill_num,
  sum(all_quantity_num) as all_quantity_num ,
  sum(air_mainwaybill_num) as air_mainwaybill_num ,
  sum(air_quantity_num) as air_quantity_num,    
  sum(all_wgt_num) as all_wgt_num,
  sum(air_wgt_num) as air_wgt_num
 from dm_ordi_predict.dws_air_flow_dynamic_pro_qty_hi
 where inc_day = '20221001'
group by inc_day ,inc_dayhour
;
select count(1) 
 from dm_ordi_predict.dws_air_flow_dynamic_pro_qty_hi_backup20230713
 where inc_day = '20230720' and 
 inc_dayhour = '2023072014'
 and  (area_code is null or area_code = '')

 select *
  from dm_ordi_predict.dws_air_flow_dynamic_pro_qty_hi_backup20230713
 where inc_day = '20221001'
 group by inc_dayhour 


 -- 收入板块
select  inc_day ,inc_dayhour,
  sum(all_mainwaybill_num) as all_mainwaybill_num,
  sum(all_quantity_num) as all_quantity_num ,
  sum(air_mainwaybill_num) as air_mainwaybill_num ,
  sum(air_quantity_num) as air_quantity_num,
  sum(all_wgt_num) as all_wgt_num,
  sum(air_wgt_num) as air_wgt_num
 from dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi_backup20230713
 where inc_day = '20221001'
group by inc_day ,inc_dayhour

select 
 inc_day ,inc_dayhour,
  sum(all_mainwaybill_num) as all_mainwaybill_num,
  sum(all_quantity_num) as all_quantity_num ,
  sum(air_mainwaybill_num) as air_mainwaybill_num ,
  sum(air_quantity_num) as air_quantity_num,    
  sum(all_wgt_num) as all_wgt_num,
  sum(air_wgt_num) as air_wgt_num
 from dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi
 where inc_day = '20221001'
 and inc_dayhour in ('2022100121','2022100122','2022100123','2022100124')
group by inc_day ,inc_dayhour
;

-- oms 验证
SELECT inc_dayhour,sum(all_order_num) as all_order_num, sum(air_order_num) as air_order_num
FROM dm_ordi_predict.dws_cityflow_dynamic_order_hi_backup20230713
    where inc_day = '20230601'
  and inc_dayhour in ('2023060121','2023060122','2023060123','2023060124')
 group by inc_dayhour 

 SELECT inc_dayhour,sum(all_order_num) as all_order_num, sum(air_order_num) as air_order_num
FROM dm_ordi_predict.dws_cityflow_dynamic_order_hi
  where inc_day = '20230601'
  and inc_dayhour in ('2023060121','2023060122','2023060123','2023060124')
 group by inc_dayhour 


--  alter table dm_ordi_predict.dws_cityflow_dynamic_order_hi_backup20230713 rename to dm_ordi_predict.dws_cityflow_dynamic_area_order_hi;
--  alter table dm_ordi_predict.dws_air_flow_dynamic_pro_qty_hi_backup20230713 rename to dm_ordi_predict.dws_air_flow_dynamic_area_pro_qty_hi;
--  alter table dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi_backup20230713 rename to dm_ordi_predict.dws_air_flow_dynamic_area_income_qty_hi;

-- 航空动态底表 -- 新增业务区字段，
-- 测试 字段指定位置添加
alter table dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi_backup20230620 add columns (src_area_code string comment '起始业务区编码') cascade;
alter table dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi_backup20230620 change src_area_code src_area_code string after weight_type_code;
-- select * from  dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi_backup20230620
alter table dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi_backup20230620 add columns (src_area_name string comment '起始业务区名称') cascade;
alter table dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi_backup20230620 change src_area_name src_area_name string after src_area_code;

-- 航空动态 -  area & 原始 分区校验 

show partitions dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi;
show partitions dm_ordi_predict.dws_air_flow_dynamic_area_income_qty_hi;

show partitions dm_ordi_predict.dws_air_flow_dynamic_pro_qty_hi;
show partitions dm_ordi_predict.dws_air_flow_dynamic_area_pro_qty_hi;


---------------------------------------------- 营运维度重量 ----------------------------------------------

 -- 0725 old 
select 
al.src_province,
al.dest_province,
split(al.city_flow,'-')[0],
al.type,
sum(avg_weight) as avg_weight,
sum(sum_avg_weight) as sum_avg_weight
from (
  select 
    days,
    city_flow,
    src_city_name,
    dest_city_name,
    type,
    air_waybill_num,
    air_quentity_num,
    air_sum_weight,
    air_avg_weight,
    air_mid_weight,
    src_province,
    dest_province,
    t2.avg_weight as avg_weight ,
    t1.air_quentity_num*t2.avg_weight as sum_avg_weight,
    inc_day
from  
(select * from dm_ordi_predict.dws_air_flow_six_dims_day_newhb 
    where inc_day = '20230725'
)t1 
left join 
(select 
  send_province,
  arrive_province,
  operation_type,
  dist_code,
  avg(avg_weight) as avg_weight
 from tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp9
group by 
  send_province,
  arrive_province,
  operation_type,
  dist_code
) t2 
on t1.src_province=t2.send_province
and t1.dest_province=t2.arrive_province
and split(t1.city_flow,'-')[0]=t2.dist_code
and t1.type=t2.operation_type
) al  group by 
al.src_province,
al.dest_province,
split(al.city_flow,'-')[0],
al.type ;

-- 0725  new 

select 
    al.src_province,
    al.dest_province,
    split(al.city_flow,'-')[0],
    al.type,
    sum(avg_weight) as avg_weight,
    sum(sum_avg_weight) as sum_avg_weight
 from dm_ordi_predict.dws_air_flow_six_dims_day_newhb al 
    where inc_day = '20230725'
 group by 
al.src_province,
al.dest_province,
split(al.city_flow,'-')[0],
al.type ;

--  '20230726' is not null 27226 
select count(1) from dm_ordi_predict.dws_air_flow_six_dims_day_newhb 
    where inc_day = '20230726'
and avg_weight is not null 

select *  from dm_ordi_predict.dws_air_flow_six_dims_day_newhb 
    where inc_day = '20230727'
and avg_weight is not null 


-------------->>>>>>>>>
-- 切换航空件步骤：
    -- 1、营运维度 -- 重量 07-26 重新刷
    -- 2、特快件 & base表，mapping 
    /**
        2 验证base 的省份 是否存在
            select * from dm_ordi_predict.dws_static_cityflow_base
            where inc_day between '20230727' and  '20230727'
            and (
                (src_province is null or src_province = '')
            or  (dest_province is null or dest_province = '')
        3 航空省道省验证
        select * from 
        ( select inc_day,count(1) as count_new, sum(air_quantity_num) as air_num_new 
          from dm_predict.dws_airflow_province_qty_d 
         where inc_day between '20230701' and '20230726'
         group by inc_day ) t1 
         inner join 
          ( select inc_day,count(1) as count_old, sum(air_quantity_num) as air_num_old
          from dm_predict.dws_airflow_province_qty_d_backup20230724
         where inc_day between '20230701' and '20230726'
         group by inc_day ) t2
         on t1.inc_day = t2.inc_day 
        4、营运维度验证
            --1 、del对比
                select * from 
                (select inc_day,count(1),sum(quantity) as quantity_new from dm_ordi_predict.dws_air_flow_six_dims_waybill_dtl
                where inc_day between '20230701' and '20230726'
                group by inc_day ) t1 
                inner join 
                (select  inc_day,count(1),sum(quantity) as quantity_old from dm_ordi_predict.dws_air_flow_six_dims_waybill_dtl_backup20230724
                where inc_day between '20230701' and '20230726'
                group by inc_day  ) t2 
                on t1.inc_day = t2.inc_day

            -- 2、six_dims
                    -- type对比
                select type,
                    sum(air_waybill_num) as air_waybill_num_new,
                    sum(air_quentity_num) as air_quentity_num_new,
                    sum(air_sum_weight) as air_sum_weight_new,
                    sum(air_avg_weight) as air_avg_weight_new
                from dm_ordi_predict.dws_air_flow_six_dims_day
                where inc_day = '20230726'
                group by  type 
                    -- 前后对比
                    select * from 
                    (select inc_day,count(1), 
                        sum(air_waybill_num) as air_waybill_num_new,
                        sum(air_quentity_num) as air_quentity_num_new,
                        sum(air_sum_weight) as air_sum_weight_new,
                        sum(air_avg_weight) as air_avg_weight_new
                    from dm_ordi_predict.dws_air_flow_six_dims_day
                    where inc_day between '20230701' and '20230726'
                    group by inc_day ) t1 
                    inner join 
                    (select inc_day,count(1), 
                        sum(air_waybill_num) as air_waybill_num_old,
                        sum(air_quentity_num) as air_quentity_num_old,
                        sum(air_sum_weight) as air_sum_weight_old,
                        sum(air_avg_weight) as air_avg_weight_old
                    from dm_ordi_predict.dws_air_flow_six_dims_daybackup20230724
                    where inc_day between '20230701' and '20230726'
                    group by inc_day  ) t2 
                    on t1.inc_day = t2.inc_day
                3、 newhb  type = 特色经济 ,特快，新空配 ，鞋服大客户，陆升航
                    select type, 
                    sum(air_waybill_num) as air_waybill_num_new,
                            sum(air_quentity_num) as air_quentity_num_new,
                            sum(air_sum_weight) as air_sum_weight_new,
                            sum(air_avg_weight) as air_avg_weight_new,
                            sum(avg_weight) as avg_weight_new,
                            sum(sum_avg_weight) as sum_avg_weight_new
                    from dm_ordi_predict.dws_air_flow_six_dims_day_newhb 
                        where inc_day = '20230727'
                        group by type 
                    -- 数量对比
                     select * from 
                        (select inc_day,count(1), 
                            sum(air_waybill_num) as air_waybill_num_new,
                            sum(air_quentity_num) as air_quentity_num_new,
                            sum(air_sum_weight) as air_sum_weight_new,
                            sum(air_avg_weight) as air_avg_weight_new,
                            sum(avg_weight) as avg_weight_new,
                            sum(sum_avg_weight) as sum_avg_weight_new
                        from dm_ordi_predict.dws_air_flow_six_dims_day_newhb
                        where inc_day between '20230701' and '20230726'
                        group by inc_day ) t1 
                        inner join 
                        (select inc_day,count(1), 
                            sum(air_waybill_num) as air_waybill_num_old,
                            sum(air_quentity_num) as air_quentity_num_old,
                            sum(air_sum_weight) as air_sum_weight_old,
                            sum(air_avg_weight) as air_avg_weight_old,
                            sum(avg_weight) as avg_weight_old,
                            sum(sum_avg_weight) as sum_avg_weight_old
                        from dm_ordi_predict.dws_air_flow_six_dims_day_newhb_backup20230724
                        where inc_day between '20230701' and '20230726'
                        group by inc_day  ) t2 
                        on t1.inc_day = t2.inc_day

                    4、spec_econ
                         select * from 
                            (select inc_day,count(1), 
                                sum(all_waybill_num) as all_waybill_num_new,
                                sum(all_quantity_num) as all_quantity_num_new,
                                sum(air_waybill_num) as air_waybill_num_new,
                                sum(air_quantity_num) as air_quentity_num_new,
                                sum(air_sum_weight) as air_sum_weight_new,
                                sum(air_avg_weight) as air_avg_weight_new,
                                sum(avg_weight) as avg_weight_new,
                                sum(all_weight) as all_weight_new
                            from dm_ordi_predict.dws_static_cityflow_special_econ_day
                            where inc_day between '20230701' and '20230726'
                            group by inc_day ) t1 
                            inner join 
                            (select inc_day,count(1), 
                                sum(all_waybill_num) as all_waybill_num_old,
                                sum(all_quantity_num) as all_quantity_num_old,
                                sum(air_waybill_num) as air_waybill_num_old,
                                sum(air_quantity_num) as air_quentity_num_old,
                                sum(air_sum_weight) as air_sum_weight_old,
                                sum(air_avg_weight) as air_avg_weight_old,
                                sum(avg_weight) as avg_weight_old,
                                sum(all_weight) as all_weight_old
                            from dm_ordi_predict.dws_static_cityflow_special_econ_day_backup20230724
                            where inc_day between '20230701' and '20230726'
                            group by inc_day  ) t2 
                            on t1.inc_day = t2.inc_day
                    5、day_sub  ,type= 特色经济 ,特快，新空配 ，鞋服大客户，陆升航
                        select type,
                            sum(air_waybill_num) as air_waybill_num_old,
                            sum(air_quentity_num) as air_quentity_num_old,
                            sum(air_sum_weight) as air_sum_weight_old,
                            sum(air_avg_weight) as air_avg_weight_old,
                            sum(avg_weight) as avg_weight_old,
                            sum(sum_avg_weight) as sum_avg_weight_old
                        from  dm_ordi_predict.dws_air_flow_five_dims_day_sub
                        where inc_day = '20230726'
                        group by type  ; 
                        -- 数据对比
                         select * from 
                            (select inc_day,count(1), 
                                sum(air_waybill_num) as air_waybill_num_new,
                                sum(air_quentity_num) as air_quentity_num_new,
                                sum(air_sum_weight) as air_sum_weight_new,
                                sum(air_avg_weight) as air_avg_weight_new,
                                sum(avg_weight) as avg_weight_new,
                                sum(sum_avg_weight) as sum_avg_weight_new
                            from dm_ordi_predict.dws_air_flow_five_dims_day_sub
                            where inc_day between '20230701' and '20230726'
                            group by inc_day ) t1 
                            inner join 
                            (select inc_day,count(1), 
                                sum(air_waybill_num) as air_waybill_num_old,
                                sum(air_quentity_num) as air_quentity_num_old,
                                sum(air_sum_weight) as air_sum_weight_old,
                                sum(air_avg_weight) as air_avg_weight_old,
                                sum(avg_weight) as avg_weight_old,
                                sum(sum_avg_weight) as sum_avg_weight_old
                            from dm_ordi_predict.dws_air_flow_five_dims_day_sub_backup20230724
                            where inc_day between '20230701' and '20230726'
                            group by inc_day  ) t2 
                            on t1.inc_day = t2.inc_day



    **/

alter table dm_ordi_predict.cf_air_list rename to dm_ordi_predict.cf_air_list_backup20230724;
alter table dm_ordi_predict.cf_air_list_backup20230627 rename to dm_ordi_predict.cf_air_list;

-- 航空动态
alter table dm_ordi_predict.dws_air_flow_dynamic_pro_qty_hi rename to dm_ordi_predict.dws_air_flow_dynamic_pro_qty_hi_backup20230724;
alter table dm_ordi_predict.dws_air_flow_dynamic_pro_qty_hi_backup20230622 rename to dm_ordi_predict.dws_air_flow_dynamic_pro_qty_hi;

alter table dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi rename to dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi_backup20230724;
alter table dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi_backup20230622 rename to dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi;

-- oms 
alter table dm_ordi_predict.dws_cityflow_dynamic_order_hi rename to dm_ordi_predict.dws_cityflow_dynamic_order_hi_backup20230724;
alter table dm_ordi_predict.dws_cityflow_dynamic_order_hi_backup20230620 rename to dm_ordi_predict.dws_cityflow_dynamic_order_hi;


-- 特快件
alter table dm_ordi_predict.dws_static_cityflow_tekuai_base rename to dm_ordi_predict.dws_static_cityflow_tekuai_base_backup20230724;
alter table dm_ordi_predict.dws_static_cityflow_tekuai_base_backup20230620 rename to dm_ordi_predict.dws_static_cityflow_tekuai_base;

-- 静态底表
alter table dm_ordi_predict.dws_static_his_cityflow rename to dm_ordi_predict.dws_static_his_cityflow_backup20230724;
alter table dm_ordi_predict.dws_static_his_cityflow_backup20230620 rename to dm_ordi_predict.dws_static_his_cityflow;

alter table dm_ordi_predict.dws_static_cityflow_base rename to dm_ordi_predict.dws_static_cityflow_base_backup20230724;
alter table dm_ordi_predict.dws_static_cityflow_base_backup20230620 rename to dm_ordi_predict.dws_static_cityflow_base;

-- 营运维度

alter table dm_ordi_predict.dws_air_flow_six_dims_waybill_dtl rename to dm_ordi_predict.dws_air_flow_six_dims_waybill_dtl_backup20230724;
alter table dm_ordi_predict.dws_air_flow_six_dims_waybill_dtl_backup20230620 rename to dm_ordi_predict.dws_air_flow_six_dims_waybill_dtl;

alter table dm_ordi_predict.dws_air_flow_six_dims_day rename to dm_ordi_predict.dws_air_flow_six_dims_daybackup20230724; -- 
alter table dm_ordi_predict.dws_air_flow_six_dims_day_backup20230620 rename to dm_ordi_predict.dws_air_flow_six_dims_day;-- 

alter table dm_ordi_predict.dws_air_flow_six_dims_day_newhb rename to dm_ordi_predict.dws_air_flow_six_dims_day_newhb_backup20230724; -- 
alter table dm_ordi_predict.dws_air_flow_six_dims_day_newhb_backup20230620 rename to dm_ordi_predict.dws_air_flow_six_dims_day_newhb; -- 

alter table dm_ordi_predict.dws_static_cityflow_special_econ_day rename to dm_ordi_predict.dws_static_cityflow_special_econ_day_backup20230724;
alter table dm_ordi_predict.dws_static_cityflow_special_econ_day_backup20230620 rename to dm_ordi_predict.dws_static_cityflow_special_econ_day;

alter table dm_ordi_predict.dws_air_flow_five_dims_day_sub rename to dm_ordi_predict.dws_air_flow_five_dims_day_sub_backup20230724;
alter table dm_ordi_predict.dws_air_flow_five_dims_day_sub_backup20230620 rename to dm_ordi_predict.dws_air_flow_five_dims_day_sub;

-- 航空省到省 

alter table dm_predict.dws_airflow_province_qty_d rename to dm_predict.dws_airflow_province_qty_d_backup20230724;
alter table dm_predict.dws_airflow_province_qty_d_backup20230620 rename to dm_predict.dws_airflow_province_qty_d;