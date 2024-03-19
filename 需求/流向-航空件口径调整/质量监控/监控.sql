-- old   dm_ordi_predict.cf_air_list_backup20230627
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
          cityflow,
          name
        FROM
          (
            SELECT
              city_flow as cityflow,
              if_hangkong as name
            FROM
              dm_op.hk_lcb_peizhibiao_001
            where
              if_hangkong = '是'
            UNION ALL
            SELECT
              cityflow,
              name
            FROM
              dm_ordi_predict.cf_air_list_backup20230627
          ) data
        GROUP BY
          cityflow,
          name
      ) al
  ) t2 on t1.flag = t2.flag;
  
  
  
  -- new    dm_ordi_predict.cf_air_list_backup20230627
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
  
  
  
  --- 流向全表检测
  
  select
  t1.num_1 - t2.num_2 as diff_num
from
  (
    SELECT
      '1' as flag,
      count(1) as num_1
    from dm_ordi_predict.tm_air_flow_config_wide
where inc_day =  '20230718'
and is_air_flow = 1
and expiry_dt >= '$[time(yyyy-MM-dd)]'
  ) t1
  inner join (
    select
      '1' as flag,
      count(1) as num_2
    from
      (
        SELECT
          *
        FROM
          (
            select city_flow as cityflow ,is_air_flow,expiry_dt from dm_pass_atp.tm_air_flow_config_wide
where inc_day =  '$[time(yyyyMMdd)]'
and is_air_flow = 1
and expiry_dt >= '$[time(yyyy-MM-dd)]'
            UNION ALL
          select city_flow as cityflow,is_air_flow,expiry_dt from dm_ordi_predict.tm_air_flow_config_wide
where inc_day =  '20230718'
and is_air_flow = 1
and expiry_dt >= '$[time(yyyy-MM-dd)]'
          ) data
       group by cityflow,is_air_flow,expiry_dt
      ) al
  ) t2 on t1.flag = t2.flag
  
  
    --- 产品 字段检测
  
   select
  t1.num_1 - t2.num_2 as diff_num
from
  (
    SELECT
      '1' as flag,
      count(1) as num_1
     from dm_ordi_predict.tm_air_product_config
where inc_day = '20230718'
and type = 1
and expiry_dt >= '$[time(yyyy-MM-dd)]'
  ) t1
  inner join (
    select
      '1' as flag,
      count(1) as num_2
    from
      (
        SELECT
          *
        FROM
          (
           select  product_code, sop_label as limit_tag , exclude_route_code,expiry_dt from dm_pass_atp.tm_air_product_config
where inc_day = '$[time(yyyyMMdd)]'
and type = 1
and expiry_dt >= '$[time(yyyy-MM-dd)]'
            UNION ALL
          select  product_code, sop_label as limit_tag , exclude_route_code,expiry_dt from dm_ordi_predict.tm_air_product_config
where inc_day = '20230718'
and type = 1
and expiry_dt >= '$[time(yyyy-MM-dd)]'
          ) data
       group by product_code,limit_tag,exclude_route_code,expiry_dt
      ) al
  ) t2 on t1.flag = t2.flag
