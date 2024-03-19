-- 240951  src_province=北京
--  Origin_code dest_province = 广东  operation_type
-- 57511383  历史全部  -- 54949487
-- 2条  带如下条件   1
select
  count(1)
from
  dm_predict.dws_fc_hk_six_dims_predict_collecting_di
where
  predict_datetime = '2023-06-01'
  and origin_code = '010'
  and dest_code = '020'
  and operation_type = '鞋服大客户'
select
  count(1)
from
  dm_predict.dws_fc_hk_six_dims_predict_collecting_di
select
  avg_qty_weight,
  src_city_code,
  arrive_province,
  operation_type
from
  tmp_ordi_predict.tmp_shenhy_air_five_weidu_actualW_1
where
  src_city_code = '010'
  and arrive_province = '广东'
  and operation_type = '鞋服大客户' -- origin _code = 010 dest_code = 020  北京市 广州市  object_type = 4  航空流向营运维度   operation_type = 鞋服大客户/新空配
  -- 预测重量predict_wright 不一致(24,27)
  -- 计算预测重量 前   11811807
select
  count(1)
from
  tmp_dm_predict.tmp_shenhy_hk_dims_fixed_concat_tmp6 -- 计算预测重量 后   14373703
select
  count(1)
from
  tmp_dm_predict.tmp_shenhy_hk_dims_fixed_concat_tmp7
from
  tmp_dm_predict.tmp_shenhy_hk_dims_fixed_concat_tmp6 t1
  left join tmp_ordi_predict.tmp_shenhy_air_five_weidu_actualW_1 t2 ---来自规调的件均重 近30天件均重
  on t1.Origin_code = t2.src_city_code -- and t1.src_province=t2.send_province
  and t1.dest_province = t2.arrive_province
  and t1.operation_type = t2.operation_type
  left join (
    select
      operation_type,
      avg(avg_qty_weight) as avg_qty_weight
    from
      tmp_ordi_predict.tmp_shenhy_air_five_weidu_actualW_1
    where
      avg_qty_weight > 0
    group by
      operation_type
  ) t3 on t1.operation_type = t3.operation_type;
-- 14373703
select
  count(1)
from
  tmp_dm_predict.tmp_shenhy_hk_dims_fixed_concat_tmp8 -- 11811807
select
  count(1)
from
  tmp_dm_predict.tmp_shenhy_hk_dims_fixed_concat_tmp5 -------------------- 营运维度，predict_weight 为null的数据----------------------------------------------
  -- object_type_code  4,7
  -- object_type   航空流向营运维度,航空流向营运维度省份汇总
select
  object_type_code,
  object_type,
  count(1)
from
  dm_predict.dws_fc_hk_six_dims_predict_collecting_di
group by
  object_type_code,
  object_type
select
  count(1)
from
  dm_predict.dws_fc_hk_six_dims_predict_collecting_di
where
  predict_weight is null
select
  partition_key,
  operation_type,
  count(1)
from
  dm_predict.dws_fc_hk_six_dims_predict_collecting_di
where
  predict_weight is null -- and object_type_code != '7'
group by
  partition_key,
  operation_type
select
  operation_type,
  count(1)
from
  dm_predict.dws_fc_hk_six_dims_predict_collecting_di
where
  partition_key >= '20230610'
  and predict_weight is null -- and object_type_code != '7'
group by
  operation_type
select
  *
from
  dm_predict.dws_fc_hk_six_dims_predict_collecting_di
where
  partition_key >= '20230610'
  and object_type_code = '4' -- and  predict_weight is null
  and origin_code = '010'
  and dest_code = '023'
  and partition_key = '20230825'
select
  *
from
  tmp_dm_predict.tmp_shenhy_hk_dims_fixed_concat_tmp7_20230614
where
  object_type_code = '4' -- and  predict_weight is null
  and origin_code = '010'
  and dest_code = '023'
  and partition_key = '20230825' -- 1、验证数据条数
  -- 13859790
select
  count(1)
from
  tmp_dm_predict.tmp_shenhy_hk_dims_fixed_concat_tmp7_20230614 -- 13859790
select
  count(1)
from
  dm_predict.dws_fc_hk_six_dims_predict_collecting_di
where
  partition_key >= '20230610'
  and object_type_code = '4' -- 总计 14156400
  -- 4  13859790
  -- 7    296610
select
  object_type_code,
  count(1)
from
  dm_predict.dws_fc_hk_six_dims_predict_collecting_di
where
  partition_key >= '20230610'
group by
  object_type_code -- 14156400
  -- 4  13859790
  -- 7    296610
select
  object_type_code,
  count(1)
from
  dm_predict.dws_fc_hk_six_dims_predict_collecting_di_backup20230614
group by
  object_type_code -- 表全量 60970710
select
  count(1)
from
  dm_predict.dws_fc_hk_six_dims_predict_collecting_di -- 备份历史数据表    60970710
select
  count(1)
from
  dm_predict.dws_fc_hk_six_dims_predict_collecting_di_backup20230614 -- 2 、验证 predict_weight是否还有null
  -- 山东，浙江
select
  *
from
  tmp_dm_predict.tmp_shenhy_hk_dims_fixed_concat_tmp7_20230614
where
  predict_weight is null
select
  count(1)
from
  dm_predict.dws_fc_hk_six_dims_predict_collecting_di_backup20230614
where
  predict_weight is null -- 3 验证重量是否相同
  --  3.57895596E8     5.74974242E8
select
  sum(predict_quantity) as predict_quantity,
  sum(predict_weight) as predict_weight
from
  dm_predict.dws_fc_hk_six_dims_predict_collecting_di
where
  partition_key >= '20230610' -- 3.57895596E8  7.99820434E8
select
  sum(predict_quantity) as predict_quantity,
  sum(predict_weight) as predict_weight
from
  dm_predict.dws_fc_hk_six_dims_predict_collecting_di_backup20230614
where
  partition_key >= '20230610'
select
  count(1)
from
  dm_predict.dws_fc_hk_six_dims_predict_collecting_di
where
  partition_key between '$[time(yyyyMMdd,-45d)]'
  and '$[time(yyyyMMdd,+0d)]'
  and predict_weight is null
select
  count(1)
from
  dm_predict.dws_fc_hk_six_dims_predict_collecting_di
where
  partition_key >= '$[time(yyyyMMdd,+0d)]'
select
  operation_type,
  avg(avg_qty_weight) as avg_qty_weight
from
  tmp_ordi_predict.tmp_shenhy_air_five_weidu_actualW_1
where
  avg_qty_weight > 0
group by
  operation_type
select
  *
from
  tmp_ordi_predict.tmp_shenhy_air_five_weidu_actualW_1 --
select
  operation_type,
  avg(avg_weight) as avg_weight
from
  tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp9
where
  plan_send_dt between '$[time(yyyy-MM-dd,-30d)]'
  and '$[time(yyyy-MM-dd,-1d)]'
group by
  operation_type
select
  operation_type,
  count(1)
from
  dm_ops.dm_zz_air_waybill_avg_weight
where
  inc_day between '$[time(yyyyMMdd,-45d)]'
  and '$[time(yyyyMMdd,-1d)]'
  and plan_send_dt between '$[time(yyyy-MM-dd,-30d)]'
  and '$[time(yyyy-MM-dd,-1d)]'
group by
  operation_type
select
  operation_type,
  count(1)
from
  tmp_dm_ops.dm_zz_air_waybill_avg_weight_temp3
where
  plan_send_dt between '$[time(yyyy-MM-dd,-30d)]'
  and '$[time(yyyy-MM-dd,-1d)]'
group by
  operation_type -------------------------------------- OMS 去丰网 -----------------------------------------
  -- 收入版块代码(1 时效,2 电商,3 国际,4 快运,5 医药,6 其他,7 同城, 8 冷运, 9 丰网
select
  incomecode,
  count(1)
from
  dm_kafka_rdmp.dm_full_order_dtl_df
where
  inc_day = '$[time(yyyyMMdd)]'
group by
  incomecode -- 上游表 最早分区 inc_day=20210519
  show partitions dm_kafka_rdmp.dm_full_order_dtl_df -- 	inc_day=20230615/inc_dayhour=2023061513  1288672599
select
  count(1)
from
  dm_ordi_predict.dws_cityflow_dynamic_order_hi --inc_day=20210520/inc_dayhour=2021052001  -- 最早分区
  show partitions dm_ordi_predict.dws_cityflow_dynamic_order_hi
  /**
    -- 总计  968
    -- null  254
    -- hq_code in ('CN03','CN06')   431
    -- hq_code not in ('CN03','CN06')   283 
    
    -- nvl(hq_code,'') in ('CN03','CN06')  431
    -- nvl(hq_code,'') not in ('CN03','CN06')  537
    
    -- hq_code in ('CN03','CN06',null)  431
    -- hq_code not in ('CN03','CN06',null)  0 
    
    --  nvl(hq_code,'') in ('CN03','CN06',null)   431
    --  nvl(hq_code,'') not in ('CN03','CN06',null)  0 
    结论：
      1、B不存在null，A存在null
        -、A not in B  如果A存在null，则null值被过滤
        -、nvl(A,'') not in  B 如果A存在null，null值不会被过滤
       
      2、A,B都存在null
        -、A不管是否做nvl， in B 的时候， null都不会被查出来
        -、A不管是否做nvl，not in B 的时候,  什么都查不出来。
        
    **/
select
  count(1)
from
  dm_ordi_predict.dim_city_level_mapping_df
where
  inc_day = '$[time(yyyyMMdd,-1d)]'
  and (
    nvl(hq_code, '') in ('CN03', 'CN06')
    or hq_code is null
  )
  /**
        验证   !(nvl(a.income_code,'')='丰网' or nvl(a.hq_code,'')='CN39')  -- 去丰网  incomecode  手动回刷历史数据 去丰网
    结论，加了nvl,可以查出来is null的数据
    **/
  -- 总数  968
  -- nvl(hq_code,'') = 'CN03'  89
  -- hq_code is null  254
select
  count(1)
from
  dm_ordi_predict.dim_city_level_mapping_df
where
  inc_day = '$[time(yyyyMMdd,-1d)]' -- and nvl(hq_code,'') = 'CN03'
  -- and (hq_code != 'CN03')
  -- and (hq_code != 'CN03' or hq_code is null  )
  -- and nvl(hq_code,'') != 'CN03'
  -- and !(nvl(hq_code,'') = 'CN03')
  -- and !(hq_code = 'CN03')
  -- 总数  968
  -- nvl(hq_code,'') = 'CN03'  88
  -- hq_code is null  254
select
  count(1)
from
  (
    select
      if(
        hq_code = 'CN03'
        and city_code = '010',
        '',
        hq_code
      ) as hq_code
    from
      dm_ordi_predict.dim_city_level_mapping_df
    where
      inc_day = '$[time(yyyyMMdd,-1d)]'
  ) al
where
  --   !(nvl(hq_code,'') = 'CN03')
  -- hq_code = 'CN03'
  !(hq_code = 'CN03')
  /**
        验证 select  if(hq_code = 'CN03',1,0) as flag 
            select 中的不影响条数
    **/
select
  count(1)
from
  (
    select
      hq_code,
      if(hq_code = 'CN03', 1, 0) as flag
    from
      (
        select
          if(
            hq_code = 'CN03'
            and city_code = '010',
            '',
            hq_code
          ) as hq_code
        from
          dm_ordi_predict.dim_city_level_mapping_df
        where
          inc_day = '$[time(yyyyMMdd,-1d)]'
      ) al
  ) al2 -- 验证数据
  -- 订单表 昨天数据   309611518
select
  count(1)
from
  tmp_ordi_predict.dm_full_order_dtl_df_by_day -- 280460098
select
  inc_day,
  inc_dayhour,
  count(1),
  sum(all_order_num) as tmp_all_order_num,
  sum(air_order_num) as tmp_air_order_num
from
  dm_ordi_predict.dws_cityflow_dynamic_order_hi_backup20230615
where
  inc_day = '20230614'
group by
  inc_day,
  inc_dayhour
select
  inc_day,
  inc_dayhour,
  count(1),
  sum(all_order_num) as all_order_num,
  sum(air_order_num) as air_order_num
from
  dm_ordi_predict.dws_cityflow_dynamic_order_hi
where
  inc_day = '20230614'
group by
  inc_day,
  inc_dayhour -- 20230614 1788922  -- 去丰网后
  -- 20230614 1770628  -- 去丰网前
select
  count(1)
from
  dm_ordi_predict.dws_cityflow_dynamic_order_hi
where
  inc_day = '20230614'
alter table
  dm_predict.predict_vip_customer_data drop if exists partition(inc_day = '__HIVE_DEFAULT_PARTITION__') ------------------- income 验证 ---------------------------------------------
select
  concat(
    concat(
      substr(inc_day, 1, 4),
      '-',
      substr(inc_day, 5, 2)
    ),
    '-',
    substr(inc_day, 7, 2)
  ) as inc_day,
  sum(all_quantity)
from
  -- dm_ordi_predict.dws_static_cityflow_base
  dm_ordi_predict.dws_static_cityflow_base_backup20230608
where
  inc_day >= '20230301'
  and income_code = '其他'
group by
  concat(
    concat(
      substr(inc_day, 1, 4),
      '-',
      substr(inc_day, 5, 2)
    ),
    '-',
    substr(inc_day, 7, 2)
  )
select
  income_code
from
  dm_ordi_predict.dws_static_cityflow_base
where
  inc_day = '20230101' --  晓雯  静态45天件量，票量波动  every_day run
select
  concat(
    concat(
      substr(inc_day, 1, 4),
      '-',
      substr(inc_day, 5, 2)
    ),
    '-',
    substr(inc_day, 7, 2)
  ) as inc_day,
  sum(all_quantity) as all_quantity,
  sum(all_waybill_num) as all_waybill_num
from
  dm_ordi_predict.dws_static_cityflow_base
where
  inc_day >= '$[time(yyyyMMdd,-45d)]'
  and inc_day <= '$[time(yyyyMMdd,-1d)]'
group by
  concat(
    concat(
      substr(inc_day, 1, 4),
      '-',
      substr(inc_day, 5, 2)
    ),
    '-',
    substr(inc_day, 7, 2)
  )
  
  
   ------------------------------------------------ 航空件调整 ------------------------------------------------------------------
  /**
    weight_type_code	_col1	
    <3KG	        388153220	
    [3KG	10KG)	252730526
    [10KG	15KG)	96840654
    [15KG	20KG)	59925157
    >=20KG	        118462306	
    
    **/
select
  weight_type_code,
  count(1)
from
  dm_ordi_predict.dws_static_cityflow_base
group by
  weight_type_code
select
  -- 上游运单宽表
select
  a.src_dist_code,
  -- 原寄地区域代码
  a.dest_dist_code,
  -- 目的地区域代码
  a.consigned_tm,
  -- 寄件时间
  a.product_code,
  -- 产品代码(维表：dim.dim_prod_info_df)
  round(
    case
      when upper(a.unit_weight) = 'LBS' then a.meterage_weight_qty * 0.45359237
      when upper(a.unit_weight) = 'G' then a.meterage_weight_qty / 1000
      else a.meterage_weight_qty
    end,
    3
  ) as meterage_weight_qty,
  -- 计费总重量
  a.src_area_code,
  -- 源寄地区部
  a.src_hq_code,
  -- 	源寄地经营本部
  a.limit_type_code,
  -- 时效类型(维表：dim.dim_tml_type_info_df)
  a.cargo_type_code,
  -- 快件内容(维表：dim.dim_pub_cargo_type_info_df)
  a.limit_tag,
  -- 	时效标签(T1:即日;T4:特快;T6:标快;SP6:标快+;T801:
  a.volume,
  -- 总体积
  a.waybill_no,
  -- 	运单号
  a.quantity -- 	包裹总件数
,
  a.route_code -- 路由代码
from
  dwd.dwd_waybill_info_dtl_di a
where
  inc_day = '20230616'
  and route_code in ('T6', 'ZT6')
select
  *
from
  dm_op.HK_LCB_peizhibiao_006 -- 10171415037   路由代码为null/''
  -- 33770301440  20200101之后数据
  --  5492054797  指定产品，路由代码为空的条数
  -- 15802180785  指定产品，20200101之后数据
select
  count(1)
from
  dwd.dwd_waybill_info_dtl_di
where
  product_code in (
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
  and(
    route_code is null
    or route_code = ''
  )
select
  product_code,
  count(1) as count_num,
  sum(quantity) as quantity_sum
from
  dwd.dwd_waybill_info_dtl_di
where
  inc_day = '20230624'
group by
  product_code -- SE0136  	2023-06-24 00:00:03
select
  product_code,
  consigned_tm
from
  dwd.dwd_waybill_info_dtl_di
where
  inc_day = '20230624'
order by
  consigned_tm --
select
  *
from
  (
    select
      product_code,
      consigned_tm,
      row_number() over(
        partition by product_code
        order by
          consigned_tm
      ) as rank_1
    from
      dwd.dwd_waybill_info_dtl_di
    where
      product_code in (
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
      and consigned_tm is not null
      and consigned_tm != ''
  ) al
where
  al.rank_1 = 1
select
  *
from
  (
    select
      product_code,
      consigned_tm,
      row_number() over(
        partition by product_code
        order by
          substr(consigned_tm, 1, 10)
      ) as rank_1
    from
      dwd.dwd_waybill_info_dtl_di
    where
      product_code in (
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
      and consigned_tm is not null
      and consigned_tm != ''
  ) al
where
  al.rank_1 <= 10
select
  a.prod_code,
  a.limit_type,
  if(a.limit_type is null, 1, 0) as flag1,
  if(a.limit_type = '', 1, 0) as flag2,
  split(a.del_routing_codes, ','),
  -- if(limit_type  in (split(a.del_routing_codes,',')[0],',',split(a.del_routing_codes,',')[1]),'A','B'),
  concat(split(a.del_routing_codes, ',') [0], 'A'),
  concat('("', split(a.del_routing_codes, ',') [0], 'A")'),
  -- concat("('",split(a.del_routing_codes,',')[0],"A')"),
  if(
    a.limit_type != split(a.del_routing_codes, ',') [0],
    'A',
    'B'
  ),
  a.weight_segment
from
  (
    select
      *
    from
      dm_op.HK_LCB_peizhibiao_006
    where
      if_valid = '是'
  ) a
  left join dm_op.HK_LCB_peizhibiao_006 b on a.prod_code = b.prod_code -- 110224
  -- 91416  if_hangkong = '是'
select
  count(1)
from
  dm_op.HK_LCB_peizhibiao_001
where
  if_hangkong = '是'
select
  *
from
  dm_ordi_predict.cf_air_list -- 91451
select
  a.city_flow,
  b.cityflow,
  if(a.city_flow != b.cityflow, 0, 1) as flag
from
  (
    select
      *
    from
      dm_op.HK_LCB_peizhibiao_001
    where
      if_hangkong = '是'
  ) a
  right join dm_ordi_predict.cf_air_list b on a.city_flow = b.cityflow 
  
  -- 重刷历史使用表 dm_ordi_predict.cf_air_list_backup20230627  , 后续切换为正式
select
  count(1)
from
  dm_ordi_predict.cf_air_list_backup20230627 -- 91416
  -- 8983-8983 ,897-897
select
  *
from
  dm_ordi_predict.cf_air_list_backup20230627
where
  split(cityflow, '-') [0] = split(cityflow, '-') [1] --897-897
select
  *
from
  dm_ordi_predict.cf_air_list
where
  split(cityflow, '-') [0] = split(cityflow, '-') [1] -- 8983-8983 ,897-897
select
  *
from
  dm_op.HK_LCB_peizhibiao_001
where
  if_hangkong = '是'
  and split(city_flow, '-') [0] = split(city_flow, '-') [1] -- 验证 国际件是否不存在航空流向
  -- 存在国内的，但是起始目的city一样
select
  a.cityflow
from
  (
    select
      cityflow
    from
      dm_ordi_predict.dws_static_his_cityflow
    where
      product_code in ('SE0153', 'SE0005')
      and cityflow regexp '^(\\d{3,4})-(\\d{3,4})'
    group by
      cityflow
  ) a
  inner join dm_ordi_predict.cf_air_list_backup20230627 b on a.cityflow = b.cityflow -- 查询下游任务
select
  table_name,
  next_task_id,
  next_flow_id,
  next_table_name,
  next_task_name,
  next_owner_code,
  next_owner_name,
  next_app_name,
  next_is_freeze,
  cancel_flag,
  next_cancel_flag
from
  adl.adl_rel_next_depend_info_df
where
  inc_day = '20230625'
  and "table_name" = 'dm_ordi_predict.cf_air_list' 
  
  -- 测试剔除路由代码
  -- 20230625 测试  --  28465773
  -- route_code is null 20884
  -- route_code =''  1
  -- product_code  is null  396793
  -- product_code =''  1
  -- product_code  in   					 13740376
  -- product_code  not in 		   		 14328604 （少了is null的数据）
  -- nvl(a.product_code,'') not in		 14725397
  -- 指定产品剔除路由代码'T6','ZT6'条数				23445423
  -- 指定产品带'T6','ZT6'条数					        5020350
  -- 直接剔除路由代码'T6','ZT6'条数					22777045
  -- 带'T6','ZT6'条数	                               5688728
select
  count(1) -- product_code,
  -- route_code
from
  (
    select
      if(waybill_no = 'SF1510981908543', '', route_code) as route_code,
      if(
        waybill_no = 'SF1510981908543',
        '',
        product_code
      ) as product_code
    from
      dwd.dwd_waybill_info_dtl_di
    where
      inc_day = '20230625'
  ) a
where
  -- (
  --     (
  --         nvl(a.product_code,'') in
  --         ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
  --         and (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)
  --     )
  -- or
  --     (
  --         nvl(a.product_code,'') not in
  --         ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
  --         or product_code is null
  --     )
  -- )
  -- nvl(a.product_code,'') in
  --         ('SE0001','SE000201','SE000206','SE0004','SE0008','SE0051','SE0089','SE0103','SE0107','SE0109','SE0121','SE0137','SE0146','SE0152','SE0153','SE0005')
  -- (nvl(a.route_code,'') not in ('T6','ZT6') or a.route_code is null)
  nvl(a.route_code, '') in ('T6', 'ZT6')
select
  count(1)
from
  dwd.dwd_waybill_info_dtl_di
where
  inc_day = '20230625' --  28465773
  and product_code = ''
select
  src_province,
  dest_province
from
  dm_ordi_predict.dws_static_cityflow_base_backup20230620
where
  inc_day between '20230513'
  and '20230626'
  and (
    src_province is null
    or src_province = ''
    or dest_province is null
    or dest_province = ''
  ) 
  
  -- 流向底表数据验证  1个月
  -- '20230513' and '20230626'
  -- '20230520' and '20230703'
select
  b.inc_day,
  a.new_all_waybill_num,
  a.new_all_quantity,
  a.new_all_waybill_num_air,
  a.new_all_quantity_air,
  b.all_waybill_num,
  b.all_quantity,
  b.all_waybill_num_air,
  b.all_quantity_air,
  b.all_waybill_num - a.new_all_waybill_num as diff_all_waybill_num,
  b.all_quantity - a.new_all_quantity as diff_all_quantity,
  b.all_waybill_num_air - a.new_all_waybill_num_air as diff_all_waybill_num_air,
  b.all_quantity_air - a.new_all_quantity_air as diff_all_quantity_air
from
  (
    select
      inc_day,
      count(1),
      sum(all_waybill_num) as new_all_waybill_num,
      sum(all_quantity) as new_all_quantity,
      sum(all_waybill_num_air) as new_all_waybill_num_air,
      sum(all_quantity_air) as new_all_quantity_air --  ,sum(fact_air_waybill_num) as new_fact_air_waybill_num
      -- ,sum(fact_air_quantity) as new_fact_air_quantity
    from
      dm_ordi_predict.dws_static_cityflow_base_backup20230620
    where
      inc_day between '20230520'
      and '20230703'
    group by
      inc_day
  ) a
  right join (
    select
      inc_day,
      count(1),
      sum(all_waybill_num) as all_waybill_num,
      sum(all_quantity) as all_quantity,
      sum(all_waybill_num_air) as all_waybill_num_air,
      sum(all_quantity_air) as all_quantity_air --  ,sum(fact_air_waybill_num) as fact_air_waybill_num
      -- ,sum(fact_air_quantity) as fact_air_quantity
      --   from  dm_ordi_predict.dws_static_cityflow_base
    from
      tmp_ordi_predict.dws_static_cityflow_base_tmp20230704 -- 切换为同一份数据对比
    where
      inc_day between '20230520'
      and '20230703'
    group by
      inc_day
  ) b on a.inc_day = b.inc_day 
  
  -- 前后流向数差异
select
  inc_day,
  count(distinct cityflow) as cityflow_count
from
  dm_ordi_predict.dws_static_cityflow_base_backup20230620
where
  inc_day between '20230520'
  and '20230703'
  and is_air = '1'
  -- add  13 
  and cityflow in  ('021-711','351-335','417-459','513-027','539-574','591-751','714-398','724-357','738-566','776-855','798-736','826-917','832-745','833-871','856-773','8981-8983','8983-898','8983-8981','8983-8983','898-8983','912-316','953-970')
group by
  inc_day

select
  inc_day,
  count(distinct cityflow) as cityflow_count_old
from
  tmp_ordi_predict.dws_static_cityflow_base_tmp20230704 -- 切换为同一份数据对比
where
  inc_day between '20230520'
  and '20230703'
  and is_air = '1'
  -- delete 34
  and cityflow in  ('578-557','578-558','552-577','577-552','558-574','578-561','558-577','570-516','577-554','576-557','557-580','564-580','516-576','580-561','561-576','578-516','578-552','577-516','516-580','558-578','557-578','570-518','577-561','527-578','516-578','516-570','577-518','561-578','577-557','554-577','576-561','574-558','577-558','579-516','576-516','576-558','557-576','527-577','564-577','516-579','577-527','580-516','558-580','580-558','578-527','561-580','552-578','580-564','580-557','561-577','518-578','558-576','518-577','516-577','557-577','578-518','577-564')
group by
  inc_day
  


select
  inc_day,
  cityflow as cityflow_count_new,
  sum(all_quantity_air) as all_quantity_air_new
from
  dm_ordi_predict.dws_static_cityflow_base_backup20230620
where
  inc_day
  = '20230604'
--    between '20230604'and '20230703'
  and is_air = '1'
group by inc_day,
cityflow


select
  inc_day,
  cityflow as cityflow_count_old,
  sum(all_quantity_air) as all_quantity_air_old
from
  tmp_ordi_predict.dws_static_cityflow_base_tmp20230704 -- 切换为同一份数据对比
where
  inc_day between '20230604'
  and '20230703'
  and is_air = '1'
group by inc_day
    cityflow
   -- select  cityflow,count(1)  from dm_ordi_predict.dws_static_cityflow_base_backup20230620
  -- where inc_day = '20230520'
  -- and is_air = '1'
  -- group by cityflow
  -- SE0134   丰网


----  测试剔除的路由数据，cityflow是不是扎堆
    -- 029-971	119	122	-3   最新查询 20230604又没有？  -- 因为后续20230604 backup表刷新了
select al.inc_day, cityflow ,all_quantity_air_old,all_quantity_air_new,al.diff_air_num
from (
select if(a.inc_day is null ,b.inc_day,a.inc_day) as inc_day,
if(a.cityflow_count_old is null ,b.cityflow_count_new,a.cityflow_count_old ) as cityflow,
if(a.all_quantity_air_old is null ,0,a.all_quantity_air_old ) as all_quantity_air_old
, if(b.inc_day is null ,a.inc_day,b.inc_day) as inc_day_1,b.cityflow_count_new,
if(b.all_quantity_air_new is null ,0,b.all_quantity_air_new )  as all_quantity_air_new 
,if(a.all_quantity_air_old is null ,0,a.all_quantity_air_old ) - if(b.all_quantity_air_new is null ,0,b.all_quantity_air_new )  as diff_air_num
from 
(select
  inc_day,
  cityflow as cityflow_count_old,
  sum(all_quantity_air) as all_quantity_air_old
from
  tmp_ordi_predict.dws_static_cityflow_base_tmp20230704 -- 切换为同一份数据对比
where
  inc_day = '20230604'
  and is_air = '1'
group by inc_day
        ,cityflow) a 
    full outer join 
(select
  inc_day,
  cityflow as cityflow_count_new,
  sum(all_quantity_air) as all_quantity_air_new
from
  dm_ordi_predict.dws_static_cityflow_base_backup20230620
where
  inc_day = '20230604'
  and is_air = '1'
group by inc_day,
cityflow ) b
on a.inc_day = b.inc_day 
and a.cityflow_count_old = b.cityflow_count_new
) al where al.diff_air_num != 0


 -- 一个月数据，cityflow差异
select 
-- al.inc_day,
 cityflow ,all_quantity_air_old,all_quantity_air_new,al.diff_air_num
from (select
-- select if(a.inc_day is null ,b.inc_day,a.inc_day) as inc_day,
if(a.cityflow_count_old is null ,b.cityflow_count_new,a.cityflow_count_old ) as cityflow,
if(a.all_quantity_air_old is null ,0,a.all_quantity_air_old ) as all_quantity_air_old ,
-- , if(b.inc_day is null ,a.inc_day,b.inc_day) as inc_day_1,b.cityflow_count_new,
if(b.all_quantity_air_new is null ,0,b.all_quantity_air_new )  as all_quantity_air_new 
,if(a.all_quantity_air_old is null ,0,a.all_quantity_air_old ) - if(b.all_quantity_air_new is null ,0,b.all_quantity_air_new )  as diff_air_num
from 
(select
--   inc_day,
  cityflow as cityflow_count_old,
  sum(all_quantity_air) as all_quantity_air_old
from
  tmp_ordi_predict.dws_static_cityflow_base_tmp20230704 -- 切换为同一份数据对比
where
  inc_day between '20230604' and '20230703'
  and is_air = '1'
group by 
-- inc_day ,
cityflow) a 
    full outer join 
(select
--   inc_day,
  cityflow as cityflow_count_new,
  sum(all_quantity_air) as all_quantity_air_new
from
  dm_ordi_predict.dws_static_cityflow_base_backup20230620
where
  inc_day between '20230604' and '20230703'
  and is_air = '1'
group by 
-- inc_day,
cityflow ) b
on 
-- a.inc_day = b.inc_day and
 a.cityflow_count_old = b.cityflow_count_new
) al where al.diff_air_num != 0





select
  inc_day,
  sum(quantity) as all_quantity
from
  dwd.dwd_waybill_info_dtl_di
where
  --  inc_day = '20230615'
  inc_day between '20230513'
  and '20230703' -- T-55D T-1D  -- 手动回刷历史数据
  and substr(consigned_tm, 0, 10) between '2023-05-13'
  and '2023-07-03'
  and (
    src_hq_code <> 'CN39'
    or src_hq_code is null
  )
  and (
    nvl(source_zone_code, '') in (
      select
        dept_code
      from
        dim.dim_department
      where
        hq_code <> 'CN39'
        and dept_code is not null
      group by
        dept_code
    )
    or source_zone_code is null
  )
  and (
    nvl(product_code, '') <> 'SE0134'
    or product_code is null
  )
group by
  inc_day


select
  product_code
from
  dm_ordi_predict.dws_static_his_cityflow
where
  inc_day between '20230513'
  and '20230626'
group by
  product_code
  
  
   -- 查询运单  剔除路由的数据，原来有多少
select
  inc_day,
  count(1),
  sum(all_quantity) as all_quantity,
  sum(all_quantity_air) as all_quantity_air,
  sum(all_quantity_air_1) as all_quantity_air_1,
  sum(all_quantity_air_2) as all_quantity_air_2
from
  (
    select
      inc_day,
      is_air,
      sum(quantity) as all_quantity,
      sum(if(is_air = '1', quantity, 0)) as all_quantity_air,
      sum(if(is_air_1 = '1', quantity, 0)) as all_quantity_air_1,
      sum(if(is_air_2 = '1', quantity, 0)) as all_quantity_air_2
    from
      (
        select
          a.inc_day,
          a.cityflow,
          a.product_code,
          a.quantity,
     
     -- 新增路由
          case
            when a.product_code = 'SE000201'
            and b.cityflow is not null
            and (nvl(a.route_code, '') in ('T6', 'ZT6')) then '1'
            when a.product_code = 'SE0001'
            and b.cityflow is not null
            and (nvl(a.route_code, '') in ('T6', 'ZT6')) then '1'
            when a.product_code = 'SE0107'
            and b.cityflow is not null
            and (nvl(a.route_code, '') in ('T6', 'ZT6')) then '1'
            when a.product_code = 'SE0103'
            and b.cityflow is not null
            and (nvl(a.route_code, '') in ('T6', 'ZT6')) then '1'
            when a.product_code = 'SE0051'
            and b.cityflow is not null
            and (nvl(a.route_code, '') in ('T6', 'ZT6')) then '1'
            when a.product_code = 'SE0089'
            and b.cityflow is not null
            and (nvl(a.route_code, '') in ('T6', 'ZT6')) then '1'
            when a.product_code = 'SE0109'
            and b.cityflow is not null
            and (nvl(a.route_code, '') in ('T6', 'ZT6')) then '1'
            when a.product_code = 'SE0008'
            and b.cityflow is not null
            and (
              a.limit_tag = 'T4'
              or a.limit_tag = 'T801'
            )
            and (nvl(a.route_code, '') in ('T6', 'ZT6')) then '1'
            when a.product_code = 'SE0137'
            and b.cityflow is not null
            and (nvl(a.route_code, '') in ('T6', 'ZT6')) then '1'
            when a.product_code = 'SE0121'
            and b.cityflow is not null
            and (nvl(a.route_code, '') in ('T6', 'ZT6')) then '1'
            when a.product_code = 'SE0004'
            and a.limit_tag = 'SP6'
            and b.cityflow is not null
            and (nvl(a.route_code, '') in ('T6', 'ZT6')) then '1'
            when a.product_code = 'SE0146'
            and b.cityflow is not null
            and (nvl(a.route_code, '') in ('T6', 'ZT6')) then '1'
            when a.product_code = 'SE0152'
            and b.cityflow is not null
            and (nvl(a.route_code, '') in ('T6', 'ZT6')) then '1' --20230403 新增两个航空产品
            when a.product_code = 'SE000206'
            and b.cityflow is not null
            and (nvl(a.route_code, '') in ('T6', 'ZT6')) then '1'
            when a.product_code = 'SE0153'
            and b.cityflow is not null
            and (nvl(a.route_code, '') in ('T6', 'ZT6')) then '1' -- 20230627 新增航空产品
            when a.product_code = 'SE0005'
            and b.cityflow is not null
            and (nvl(a.route_code, '') in ('T6', 'ZT6')) then '1' -- 20230627 新增航空产品
            else '0'
          end as is_air_1 
    -- 新增产品
,case
            when a.product_code = 'SE0153'
            and b.cityflow is not null
            and (
              nvl(a.route_code, '') not in ('T6', 'ZT6')
              or a.route_code is null
            ) then '1' -- 20230627 新增航空产品
            when a.product_code = 'SE0005'
            and b.cityflow is not null
            and (
              nvl(a.route_code, '') not in ('T6', 'ZT6')
              or a.route_code is null
            ) then '1' -- 20230627 新增航空产品
            else '0'
          end as is_air_2,

     case
            when a.product_code = 'SE000201'
            and b.cityflow is not null
            and (
              nvl(a.route_code, '') not in ('T6', 'ZT6')
              or a.route_code is null
            ) then '1'
            when a.product_code = 'SE0001'
            and b.cityflow is not null
            and (
              nvl(a.route_code, '') not in ('T6', 'ZT6')
              or a.route_code is null
            ) then '1'
            when a.product_code = 'SE0107'
            and b.cityflow is not null
            and (
              nvl(a.route_code, '') not in ('T6', 'ZT6')
              or a.route_code is null
            ) then '1'
            when a.product_code = 'SE0103'
            and b.cityflow is not null
            and (
              nvl(a.route_code, '') not in ('T6', 'ZT6')
              or a.route_code is null
            ) then '1'
            when a.product_code = 'SE0051'
            and b.cityflow is not null
            and (
              nvl(a.route_code, '') not in ('T6', 'ZT6')
              or a.route_code is null
            ) then '1'
            when a.product_code = 'SE0089'
            and b.cityflow is not null
            and (
              nvl(a.route_code, '') not in ('T6', 'ZT6')
              or a.route_code is null
            ) then '1'
            when a.product_code = 'SE0109'
            and b.cityflow is not null
            and (
              nvl(a.route_code, '') not in ('T6', 'ZT6')
              or a.route_code is null
            ) then '1'
            when a.product_code = 'SE0008'
            and b.cityflow is not null
            and (
              a.limit_tag = 'T4'
              or a.limit_tag = 'T801'
            )
            and (
              nvl(a.route_code, '') not in ('T6', 'ZT6')
              or a.route_code is null
            ) then '1'
            when a.product_code = 'SE0137'
            and b.cityflow is not null
            and (
              nvl(a.route_code, '') not in ('T6', 'ZT6')
              or a.route_code is null
            ) then '1'
            when a.product_code = 'SE0121'
            and b.cityflow is not null
            and (
              nvl(a.route_code, '') not in ('T6', 'ZT6')
              or a.route_code is null
            ) then '1'
            when a.product_code = 'SE0004'
            and a.limit_tag = 'SP6'
            and b.cityflow is not null
            and (
              nvl(a.route_code, '') not in ('T6', 'ZT6')
              or a.route_code is null
            ) then '1'
            when a.product_code = 'SE0146'
            and b.cityflow is not null
            and (
              nvl(a.route_code, '') not in ('T6', 'ZT6')
              or a.route_code is null
            ) then '1'
            when a.product_code = 'SE0152'
            and b.cityflow is not null
            and (
              nvl(a.route_code, '') not in ('T6', 'ZT6')
              or a.route_code is null
            ) then '1' --20230403 新增两个航空产品
            when a.product_code = 'SE000206'
            and b.cityflow is not null
            and (
              nvl(a.route_code, '') not in ('T6', 'ZT6')
              or a.route_code is null
            ) then '1'
            when a.product_code = 'SE0153'
            and b.cityflow is not null
            and (
              nvl(a.route_code, '') not in ('T6', 'ZT6')
              or a.route_code is null
            ) then '1' -- 20230627 新增航空产品
            when a.product_code = 'SE0005'
            and b.cityflow is not null
            and (
              nvl(a.route_code, '') not in ('T6', 'ZT6')
              or a.route_code is null
            ) then '1' -- 20230627 新增航空产品
            else '0'
          end as is_air
          /*
                        case   
                            -- when a.product_code = 'SE000201' and b.cityflow is not null  then '1'
                    		-- when a.product_code = 'SE0001' and b.cityflow is not null  then '1'
                    		-- when a.product_code = 'SE0107' and b.cityflow is not null  then '1'
                            -- when a.product_code = 'SE0103' and b.cityflow is not null  then '1'
                            -- when a.product_code = 'SE0051' and b.cityflow is not null  then '1'
                    		-- when a.product_code = 'SE0089' and b.cityflow is not null  then '1'
                    		-- when a.product_code = 'SE0109' and b.cityflow is not null  then '1'
                    		-- when a.product_code = 'SE0008' and b.cityflow is not null and (a.limit_tag ='T4' or a.limit_tag ='T801')  then '1'
                    		-- when a.product_code = 'SE0137' and b.cityflow is not null  then '1'
                    		-- when a.product_code = 'SE0121' and b.cityflow is not null  then '1'
                    		-- when a.product_code = 'SE0004' and a.limit_tag = 'SP6' and b.cityflow is not null  then '1'
                            -- when a.product_code = 'SE0146' and b.cityflow is not null  then '1'
                            -- when a.product_code = 'SE0152' and b.cityflow is not null  then '1'  --20230403 新增两个航空产品
                            -- when a.product_code = 'SE000206' and b.cityflow is not null  then '1'
                            when a.product_code = 'SE0153' and b.cityflow is not null  then '1'  -- 20230627 新增航空产品
                            when a.product_code = 'SE0005' and b.cityflow is not null  then '1'  -- 20230627 新增航空产品
                    		else '0' 
                        end as is_air
                    */
        from
          (
            select
              concat(
                nvl(
                  case
                    when src_dist_code in ('410', '413') then '024'
                    when src_dist_code = '910' then '029'
                    when src_dist_code = '731' then '7311'
                    when src_dist_code = '733' then '7313'
                    when src_dist_code = '899' then '8981'
                    when src_dist_code = '732' then '7312'
                    when src_dist_code = '888' then '088'
                    else src_dist_code
                  end,
                  '#'
                ),
                '-',
                nvl(
                  case
                    when dest_dist_code in ('410', '413') then '024'
                    when dest_dist_code = '910' then '029'
                    when dest_dist_code = '731' then '7311'
                    when dest_dist_code = '733' then '7313'
                    when dest_dist_code = '899' then '8981'
                    when dest_dist_code = '732' then '7312'
                    when dest_dist_code = '888' then '088'
                    else dest_dist_code
                  end,
                  '#'
                )
              ) as cityflow,
              *
            from
              dwd.dwd_waybill_info_dtl_di
            where
              --  inc_day = '20230615'
              inc_day between '20230604'
              and '20230703' -- T-55D T-1D  -- 手动回刷历史数据
              and substr(consigned_tm, 0, 10) between '2023-06-04'
              and '2023-07-03'
              and (
                src_hq_code <> 'CN39'
                or src_hq_code is null
              )
              and (
                nvl(source_zone_code, '') in (
                  select
                    dept_code
                  from
                    dim.dim_department
                  where
                    hq_code <> 'CN39'
                    and dept_code is not null
                  group by
                    dept_code
                )
                or source_zone_code is null
              )
              and (
                nvl(product_code, '') <> 'SE0134'
                or product_code is null
              ) --  and product_code in ('SE0004','SE0008','SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206','SE0153','SE0005')
          ) a
          left join (
            select
              *
            from
              dm_ordi_predict.cf_air_list -- 20230627 切换使用业务提供航空流向表数据
              -- select * from dm_ordi_predict.cf_air_list_backup20230627
          ) b on a.cityflow = b.cityflow
      ) al
    group by
      al.inc_day,
      al.is_air
  ) all2
group by
  all2.inc_day;
-- 因为刷线最近45天数据， 故查询2023-01月数据对比
  -- 运单表  890142122
  -- base    891216369
  -- 差异原因：  运单剔除 cn39 ， base加了剔除 income_code = '丰网'
select
  sum(all_quantity) as new_all_quantity
from
  dm_ordi_predict.dws_static_cityflow_base
where
  inc_day between '20230101'
  and '20230131' 
  
  --- fact关联表
  -- inc_day=20210401
  show partitions dm_ordi_predict.dm_fact_air_waybill_no -- 上游表
  table dm_ordi_predict.dm_fact_air_waybill_no partition(inc_day)
select
  mainwaybillno as waybill_no,
  inc_day
from
  ods_kafka_fvp.fvp_core_fact_route_op
where
  inc_day >= '${yyyyMMdd2}'
  and inc_day <= '${yyyyMMdd1}'
  and datastatus = '1'
  and (
    opcode = '105'
    or opcode = '106'
  )
group by
  mainwaybillno,
  inc_day
select
  *
from
  (
    select
      inc_day,
      sum(all_waybill_num) as all_waybill_num,
      sum(all_quantity) as all_quantity,
      sum(all_waybill_num_air) as all_waybill_num_air,
      sum(all_quantity_air) as all_quantity_air,
      sum(fact_air_waybill_num) as fact_air_waybill_num,
      sum(fact_air_quantity) as fact_air_quantity,
      sum(fact_air_weight) as fact_air_weight,
      sum(fact_air_volume) as fact_air_volume
    from
      dm_ordi_predict.dws_static_cityflow_base
    group by
      inc_day
  ) al
where
  al.fact_air_waybill_num = 0
  
   ------- 航空动态打点，剔除
  -- 20230609  最近数据   tmp_dm_predict.rmdp_waybill_basedata_tmp20230629
select
  productcode,
  routecode
from
  dm_kafka_rdmp.rmdp_waybill_basedata
where
  inc_day = '20230615' 
  -- 20230612  才有数据  2023070查询
  -- inc_day=20230628/inc_dayhour=2023062824  【1~24】
select
  inc_day,
  inc_dayhour,
  count(1)
from
  dm_ordi_predict.dws_air_flow_dynamic_pro_qty_hi
where
  inc_day = '20230628'
group by
  inc_day,
  inc_dayhour
select
  inc_day,
  inc_dayhour,
  count(1)
from
  dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi
where
  inc_day = '20230628'
group by
  inc_day,
  inc_dayhour -- 新旧差异对比
select
  a.inc_day,
  a.inc_dayhour,
  a.count_old,
  a.all_mainwaybill_num,
  a.all_quantity_num,
  a.air_mainwaybill_num,
  a.air_quantity_num,
  b.count_new,
  b.all_mainwaybill_num as new_all_mainwaybill_num,
  b.all_quantity_num as new_all_quantity_num,
  b.air_mainwaybill_num as new_air_mainwaybill_num,
  b.air_quantity_num as new_air_quantity_num,
  b.count_new - a.count_old,
  b.all_mainwaybill_num - a.all_mainwaybill_num,
  b.all_quantity_num - a.all_quantity_num,
  b.air_mainwaybill_num - a.air_mainwaybill_num,
  b.air_quantity_num - a.air_quantity_num
from
  (
    select
      inc_day,
      inc_dayhour,
      count(1) as count_old,
      sum(all_mainwaybill_num) as all_mainwaybill_num,
      sum(all_quantity_num) as all_quantity_num,
      sum(air_mainwaybill_num) as air_mainwaybill_num,
      sum(air_quantity_num) as air_quantity_num
    from
      dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi
    where
      inc_day = '20230618'
    group by
      inc_day,
      inc_dayhour
  ) a
  inner join (
    select
      inc_day,
      inc_dayhour,
      count(1) as count_new,
      sum(all_mainwaybill_num) as all_mainwaybill_num,
      sum(all_quantity_num) as all_quantity_num,
      sum(air_mainwaybill_num) as air_mainwaybill_num,
      sum(air_quantity_num) as air_quantity_num
    from
      dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi_backup20230620
    where
      inc_day = '20230618'
    group by
      inc_day,
      inc_dayhour
  ) b on a.inc_day = b.inc_day
  and a.inc_dayhour = b.inc_dayhour -- 20230620 24点数据对比
  -- 新增航空件 件量统计
select
  a.air_quantity_num_3_new - b.air_quantity_num_3_old as diff,
  *
from
  (
    select
      'A' as flag,
      sum(all_mainwaybill_num) as will_new,
      sum(all_quantity_num) as quantity_new,
      -- sum(air_mainwaybill_num) as air_mainwaybill_num,
      sum(air_mainwaybill_num_1) as air_mainwaybill_num_1_new,
      sum(air_mainwaybill_num_2) as air_mainwaybill_num_2_new,
      sum(air_mainwaybill_num_3) as air_mainwaybill_num_3_new,
      -- sum(air_quantity_num) as air_quantity_num ,
      sum(air_quantity_num_1) as air_quantity_num_1_new,
      sum(air_quantity_num_2) as air_quantity_num_2_new,
      sum(air_quantity_num_3) as air_quantity_num_3_new
    from
      -- tmp_dm_predict.tmp_dws_hk_dynamic_waybill_2_tmp2023062024
      -- tmp_dm_predict.tmp_dws_hk_dynamic_waybill_2_tmp2023061324  -- 20230618
      tmp_dm_predict.tmp_dws_hk_dynamic_waybill_2_tmp2023062020
  ) a
  inner join (
    select
      'A' as flag,
      sum(all_mainwaybill_num) as will_old,
      sum(all_quantity_num) as quantity_old,
      sum(air_mainwaybill_num) as air_mainwaybill_num_old,
      sum(air_mainwaybill_num_1) as air_mainwaybill_num_1_old,
      sum(air_mainwaybill_num_2) as air_mainwaybill_num_2_old,
      -- sum(air_mainwaybill_num_3) as air_mainwaybill_num_3,
      sum(air_quantity_num) as air_quantity_num_old,
      sum(air_quantity_num_1) as air_quantity_num_1_old,
      sum(air_quantity_num_2) as air_quantity_num_2_old,
      sum(air_quantity_num_3) as air_quantity_num_3_old
    from
      tmp_dm_predict.tmp_dws_hk_dynamic_waybill_2_tmp2023062020_old -- tmp_dm_predict.tmp_dws_hk_dynamic_waybill_2_tmp2023061324_old
  ) b on a.flag = b.flag

--底表  add 17  16
select
  count(
    distinct concat(pickupcitycode, '-', delivercitycode)
  )
from
  tmp_dm_predict.tmp_dws_hk_dynamic_waybill_2_tmp2023062024
where 
 productcode in ('SE0153','SE0005')

concat(pickupcitycode, '-', delivercitycode) in 
         ('021-711','351-335','417-459','513-027','539-574','591-751','714-398','724-357','738-566','776-855','798-736','826-917','832-745','833-871','856-773','8981-8983','8983-898','8983-8981','8983-8983','898-8983','912-316','953-970')
         
-- 底表  delete  57
select
  count(
    distinct concat(pickupcitycode, '-', delivercitycode)
  )
from
  tmp_dm_predict.tmp_dws_hk_dynamic_waybill_2_tmp2023062024_old
where concat(pickupcitycode, '-', delivercitycode) in 
         ('578-557','578-558','552-577','577-552','558-574','578-561','558-577','570-516','577-554','576-557','557-580','564-580','516-576','580-561','561-576','578-516','578-552','577-516','516-580','558-578','557-578','570-518','577-561','527-578','516-578','516-570','577-518','561-578','577-557','554-577','576-561','574-558','577-558','579-516','576-516','576-558','557-576','527-577','564-577','516-579','577-527','580-516','558-580','580-558','578-527','561-580','552-578','580-564','580-557','561-577','518-578','558-576','518-577','516-577','557-577','578-518','577-564')


select
  count(
    distinct concat(pickupcitycode, '-', delivercitycode)
  )
from
  tmp_dm_predict.tmp_dws_hk_dynamic_waybill_2_tmp2023062019
  where productcode in ('SE0153','SE0005')

  -- 航空 - 总件量差异对比
select
  sum(if(delete_flag = '1', quantity, 0)) as delete_quantity,
  sum(if(add_flag = '1', quantity, 0)) as add_quantity,
  sum(quantity) as quantity
from
  (
    select
      if(
        concat(pickupcitycode, '-', delivercitycode) in (
         '578-557','578-558','552-577','577-552','558-574','578-561','558-577','570-516','577-554','576-557','557-580','564-580','516-576','580-561','561-576','578-516','578-552','577-516','516-580','558-578','557-578','570-518','577-561','527-578','516-578','516-570','577-518','561-578','577-557','554-577','576-561','574-558','577-558','579-516','576-516','576-558','557-576','527-577','564-577','516-579','577-527','580-516','558-580','580-558','578-527','561-580','552-578','580-564','580-557','561-577','518-578','558-576','518-577','516-577','557-577','578-518','577-564'
        ),
        '1',
        '0'
      ) as delete_flag,
      if(
        concat(pickupcitycode, '-', delivercitycode) in (
         '021-711','351-335','417-459','513-027','539-574','591-751','714-398','724-357','738-566','776-855','798-736','826-917','832-745','833-871','856-773','8981-8983','8983-898','8983-8981','8983-8983','898-8983','912-316','953-970'
        ),
        '1',
        '0'
      ) as add_flag,
      cast(quantity as bigint) as quantity
    from
      tmp_dm_predict.tmp_dws_hk_dynamic_waybill_1_tmp2023062018
  ) al --- 航空特快件 验证
  -- 1.49046764E8  	1.51593786E8
select
  count(1),
  sum(tekuai_waybill_num) as tekuai_waybill_num,
  sum(tekuai_quantity) as tekuai_quantity
from
  dm_ordi_predict.dws_static_cityflow_tekuai_base
where
  inc_day between '20230513'
  and '20230626'
select
  count(1),
  sum(tekuai_waybill_num) as tekuai_waybill_num,
  sum(tekuai_quantity) as tekuai_quantity
from
  dm_ordi_predict.dws_static_cityflow_tekuai_base_backup20230620
where
  inc_day between '20230513'
  and '20230626'
select
  count(1)
from
  dm_ordi_predict.dws_static_his_cityflow_backup20230620 -- 34835265
where
  inc_day between '20230513'
  and '20230626'
select
  count(1)
from
  tmp_ordi_predict.tmp_allflow_shenhy_air_kuaiman_tmp1_tmp20230620 -- 6645464
select
  count(1)
from
  tmp_ordi_predict.tmp_allflow_shenhy_air_kuaiman_tmp1 -- 6619040
select
  count(1)
from
  dm_ordi_predict.dws_static_his_cityflow -- 44925033
where
  inc_day between '20230513'
  and '20230626' ---------------------------------------  484086  20230617 11-13点数据暴增---------------------
select
  inc_day,
  inc_dayhour,
  count(1),
  sum(air_mainwaybill_num) as pro_air_mainwaybill_num,
  sum(air_quantity_num) as pro_air_quantity_num
from
  dm_ordi_predict.dws_air_flow_dynamic_pro_qty_hi
where
  inc_day >= '20230610'
  and substr(inc_dayhour, 9, 2) = '13'
group by
  inc_day,
  inc_dayhour
select
  inc_day,
  inc_dayhour,
  count(1),
  sum(air_mainwaybill_num) as income_air_mainwaybill_num,
  sum(air_quantity_num) as income_air_quantity_num
from
  dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi
where
  inc_day >= '20230610'
  and substr(inc_dayhour, 9, 2) = '13'
group by
  inc_day,
  inc_dayhour
   ----------------------------------- 验证城市动态打点  --------------------------------------------------------------------------------
select
  inc_day,
  count(1),
  sum(pickup_piece) as pickup_piece
from
  dm_ordi_predict.dws_dynamic_cityflow_base_hi
where
  inc_day between '20230705'
  and '20230719'
  and hour = '13'
group by
  inc_day

select
  hour,
  count(1),
  sum(pickup_piece) as pickup_piece
from
  dm_ordi_predict.dws_dynamic_cityflow_base_hi
where
  inc_day = '20230718'
group by hour;

select
  hour,
  count(1) as cnt,
  sum(pickup_piece) as pickup_piece_cnt
from
  dm_ordi_predict.dws_dynamic_cityflow_base_hi
where
  inc_day = '20230628'
group by
  hour

select
  hour,
  count(1),
  sum(pickup_piece) as pickup_piece_cnt
from
  dm_oewm.oewm_receive_flow_ez
where
  (deliver_hq_code <> 'CN39'or deliver_hq_code is null )
  and (pickup_hq_code <> 'CN39'or pickup_hq_code is null)
  and (nvl(deliver_dept_code, '') in (
      select  dept_codefrom  dim.dim_department
      where hq_code <> 'CN39' and dept_code is not null
        group by dept_code)
    or deliver_dept_code is null)
  and (
    nvl(pickup_dept_code, '') in (
      select dept_code
      from dim.dim_department
      where hq_code <> 'CN39'and dept_code is not null
      group by  dept_code
    )
    or pickup_dept_code is null
  )
  and inc_day = '20230719'
group by
  hour 
  -- 数据补录 20230628  10~16点
  -- 备份20230628 当天数据 dm_ordi_predict.dws_dynamic_cityflow_base_hi_backup20230628
  -- 验证条数   截止 20230628 17点   117183338281
  -- 117223756080
select
  count(1)
from
  dm_ordi_predict.dws_dynamic_cityflow_base_hi
where
  inc_day = '20230628'
select
  inc_day,
  hour,
  count(1) as cnt,
  sum(pickup_piece) as pickup_piece_cnt
from
  dm_ordi_predict.dws_dynamic_cityflow_base_hi
where
  inc_day = '20230628'
group by
  inc_day,
  hour