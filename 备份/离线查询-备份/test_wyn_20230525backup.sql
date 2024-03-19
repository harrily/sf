/**
 0  76626       3%          76626            76626
 1  427437      17%         427437           427437
 2  1913220     80%         1833192         1792692
 3                          80028  -- 多出来


 
    -、是否是因为 origin_Code 存在中文？
**/
-- select  all2.flag ,all2.origin_Code,all2.src_province,all2.dest_province ,sum(num_1) as num_1 from (

-- select origin_Code,src_province,dest_province,flag,count(1) as num_1 from (
select flag,count(1) from (
    select
    t1.*,
    case
        when t2.avg_weight is not NULL then 0
        when t3.avg_weight is not NULL then 1
        -- when t4.avg_weight is not NULL then 3
        else 2 end as flag
    from
    -- tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp2 t1
  tmp_dm_predict.tmp_shenhy_noairflow_collect_hk_tmp2 t1
    left join tmp_dm_predict.tmp_dm_cityflow_month_wgt_1 t2 
        on nvl(t1.origin_Code,'null') = t2.dist_code
        and nvl(t1.dest_province,'null')= t2.arrive_province
    left join (
        select
        send_province,
        arrive_province,
        avg(avg_weight) as avg_weight
        from
        tmp_dm_predict.tmp_dm_cityflow_month_wgt_1
        group by
        send_province,
        arrive_province
    ) t3 
        on nvl(t1.src_province,'null') = t3.send_province
        and nvl(t1.dest_province,'null') = t3.arrive_province
-- 新增 origin_code 根据座位省份匹配  -- 何龙，反馈这个省份的没用了
    -- 之前的根据origin_Code 匹配省份，那也就匹配不上
    -- left join (
    --     select
    --     send_province,
    --     arrive_province,
    --     avg(avg_weight) as avg_weight
    --     from
    --     tmp_dm_predict.tmp_dm_cityflow_month_wgt_1
    --     group by
    --     send_province,
    --     arrive_province
    -- ) t4
    --     on t1.origin_Name = t4.send_province
    --     and t1.dest_Name = t4.arrive_province

    left join (
        select
        avg(avg_weight) as avg_weight
        from
        tmp_dm_predict.tmp_dm_cityflow_month_wgt_1 
    ) t5 on 1=1 
 ) al 
    group by flag
--  where flag = 2
--  and  al.origin_Code = '021'
-- group by al.flag ,al.origin_Code,al.src_province,al.dest_province

-- ) all2 where all2.src_province != '' and all2.src_province is not null 
-- and all2.dest_province != '' and all2.dest_province is not null 
-- group by all2.flag ,all2.origin_Code,all2.src_province,all2.dest_province
-- presto 可以读出来 spark读不出来  -- 预览的问题
    -- 021 上海 安徽
select origin_Code,src_province,dest_province from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp2 
where origin_Code = '023'
order by dest_province  desc

select *  from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp2 
where origin_Code = '021'

-- 519 常州市 
select 
 dist_code,city_name 
from dim.dim_department where city_name = '上海市'

-- SFO 常州市  code错误 ，应为 519  (均重表有)
    -- SGN 益阳市   code错误 ，应为 737 (均重表无)
-- 458  伊春市  (均重表无)
-- 853 潮州市   code错误，应为 768（均重表无768/853）
-- 551 合肥市   dest_province='' 未满足(因为原表dest_code = 'BKK',导致dest_province='')
select * from  tmp_dm_predict.tmp_dm_cityflow_month_wgt_1 where dist_code = '023' and arrive_province = '四川'
-- 663516任务 
select * from  tmp_dm_predict.tmp_dm_cityflow_tekuai_month_wgt_1 where dist_code = '737'


 -- 617182 上游表
select * from  dm_ordi_predict.cityflows_fast_resultz_online
where predicted_datetime = '2023-07-12'
and split(object_code,'-')[0] = '重庆'


-- 1425
select  send_province ,arrive_province,count(1)  from tmp_dm_predict.tmp_dm_cityflow_month_wgt_1
group by send_province ,arrive_province 

select
          plan_send_dt,
          arrive_province,
          real_weight,
          waybill_cnt,
          avg_weight,
          zone_code,
          inc_day
        from
          dm_ops.dm_zz_air_waybill_avg_weight 


    select
      plan_send_dt,
    --   t3.province as send_province,
      arrive_province,
      CASE WHEN operation_type IN ('经济快件') THEN '慢产品'
           WHEN operation_type IN ('特快系列','陆升航','特色经济','鞋服大客户') THEN '快产品'
           ELSE null 
       END AS operation_type,
      real_weight,
      waybill_cnt,
      avg_weight,
      zone_code,
      inc_day
    --   ,t2.dist_code
    from
      dm_ops.dm_zz_air_waybill_avg_weight 
-- 2888
select count(1) from tmp_dm_predict.tmp_dm_cityflow_tekuai_month_wgt_1

-- 2296755 test  选择航空城市流向
select count(1) from tmp_dm_predict.tmp_shenhy_noairflow_collect_hk_tmp2
-- 2417283
select count(1) from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp2 
-- 2417283
select count(1) from
(select 
t1.* from dm_ordi_predict.cityflows_fast_resultz_online t1
left join 
dm_ordi_predict.cf_air_list t2
on t1.object_code=t2.cityflow
where t2.cityflow is null
and t1.inc_day='$[time(yyyyMMdd,-1d)]') al

-- select origin_Code,src_province,dest_province 
select count(1) from  tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp2  
where (origin_Code is null or origin_Code = '') or 
(src_province is null or src_province = '') or 
(dest_province is null or dest_province = '') 
-- 532  山东， 上海 ， 1.8847905412323
select dist_code,send_province,arrive_province,avg_weight from tmp_dm_predict.tmp_dm_cityflow_month_wgt_1 limit 1000

-- 38W 
select count(1) from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp2 
where origin_code in ('ACC','ADD','AKL','BKK','BKO','BNE','CAI','CJJ','DAR','DFW','DLA','FIH','HAN','HRE','ICN','JFK','JHB','JNB','KGL','KTM','KUA','KUL','LAD','LAX','LOS','MEL','NBO','null','ORD','OSA','PEN','PER','PUS','RYG','SAB','SFO','SGN','SIN','SWK','SYD','TAE','TYO','ULN','YVR','安徽','澳门','北京','福建','甘肃','广东','广西','贵州','海南','航空总量','河北','河南','黑龙江','湖北','湖南','吉林','江苏','江西','辽宁','内蒙古','宁夏','青海','山东','山西','陕西','上海','四川','台湾','天津','西藏','香港','新疆','云南','浙江','重庆','')
-- and src_province is not null and src_province != ''



-- dest 包含 '#'
select count(1) from tmp_dm_predict.tmp_shenhy_noairflow_collect_hk_tmp2
where origin_Code like '%#%' 
or  dest_Code like '%#%'
-- !(origin_Code rlike '([a-z]|[A-Z])+' 
-- or dest_Code rlike '([a-z]|[A-Z])+' )

-- 原始数据 (2296755)
-- 匹配上的  50w+ 22% 
-- 全未匹配上的 180w-  78%
    -- 952074   53%     dist_code,src_province,dest_province正常，但未匹配上
    -- 826038   46%     海外

-- 原始数据 (2296755)
-- 826038    海外                 36%
-- 14580     dest_code包含 '#'    0.6%
-- 1456137  正常数据              63.4%
    -- 952074   65%   -- 正常数据且未匹配上的占比
    

    select origin_Code,dest_Code , count(1)  from tmp_dm_predict.tmp_shenhy_noairflow_collect_hk_tmp2
where (src_province is null or src_province = '') or dest_province is null or dest_province = ''
group by origin_Code,dest_Code


-- 663516任务
    -- 3200800
select count(1) from tmp_dm_predict.tmp_dm_cityflow_tekuai_month_wgt_2 
/**
    0 566025
    1 2108275
    2 526500
**/
select flag,count(1) from (
    select 
       case
            when t2.avg_weight is not NULL then 0
            when t3.avg_weight is not NULL then 1
        else 2 end as flag
    from tmp_dm_predict.tmp_dm_cityflow_tekuai_month_wgt_2 t1
    left join 
      tmp_dm_predict.tmp_dm_cityflow_tekuai_month_wgt_1  t2
    on split(t1.object_code,'-')[0]=t2.dist_code
    and t1.dest_province=t2.arrive_province
    and t1.operation_type=t2.operation_type
    left join 
    (select 
    send_province,
    arrive_province,
    operation_type,
    avg(avg_weight) as avg_weight
    from tmp_dm_predict.tmp_dm_cityflow_tekuai_month_wgt_1
    group by send_province,
    arrive_province,
    operation_type) t3
    on t1.src_province=t3.send_province
    and t1.dest_province=t3.arrive_province
    and t1.operation_type=t3.operation_type 
    left join 
    (select 
    operation_type,
    avg(avg_weight) as avg_weight
    from tmp_dm_predict.tmp_dm_cityflow_tekuai_month_wgt_1
    group by operation_type) t4
    on t1.operation_type=t4.operation_type 
) al group by al.flag ;

-- 897 在distribution没有
select dept_code,dist_code,provinct_name from dim.dim_department 
where  dept_code ='897'

select * from dm_ops.dim_city_area_distribution
where  province = '西藏自治区'
-- city_code = '911'
-- and

select object_code from dm_ordi_predict.cityflows_fast_resultz_online
where inc_day='$[time(yyyyMMdd,-1d)]'
and object_type = '航空城市流向'


/***
    2023-05-15  非空网络代码验证
**/
select count(1) from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp1  -- 241W

-- 81 
select * from (
select predicted_datetime,count(1) from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp1
group by predicted_datetime
) al order by predicted_datetime desc

-- 29840    全部 2417040
select count(1) from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp1 where predicted_datetime = '2023-08-02'
-- 29840
select count(1) from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp2 where predict_datetime = '2023-08-02'
-- 567 全部 45927
select count(1) from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp3 where predict_datetime = '2023-08-02'
-- 30407 合计  全部 2462967
select count(1) from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp4  where predict_datetime = '2023-08-02'

--  新逻辑 
-- 28351   全部 2296431
select count(1) from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp1_test where predicted_datetime = '2023-08-02'
-- 28351
select count(1) from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp2_test where predict_datetime = '2023-08-02'
-- 28351  全部 2296431
select count(1) from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp2_1_test where predict_datetime = '2023-08-02'
--  3485  全部 282285    更改dim_dep -- 567    全部 45927
select count(1) from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp3_test where predict_datetime = '2023-08-02'
-- 31836 合计  全部 2578716   更改dim_dep-- 28918  全部 2342358
select count(1) from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp4_test where predict_datetime = '2023-08-02'

select src_province,dest_province,count(1) from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp4_test
group by src_province,dest_province 

-- 2578716
select count(1) from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp4_test 
    -- 验证weight  -- 因为quantity= 0 = =
select * from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp4_test  
where predict_datetime = '2023-07-18' 
-- and predict_quantity != 0
and origin_code = '519'
and dest_code = '514'
-- and predict_Weight is not null 
-- and predict_Weight != ''

-- select * from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp2_1_test  
-- where predict_datetime = '2023-08-02' 
-- and origin_code = '519'
-- and dest_code = '514'


    --  底表
select predict_datetime,object_type,sum(predict_quantity) as quantity,sum(predict_weight) as pre_weight from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp4_test  
where predict_datetime = '2023-07-18' 
and object_type = '非空网流向-城市'
group by predict_datetime,object_type

select count(1) from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp4_test  
where  predict_datetime = '2023-07-18' 
and object_type = '非空网流向-城市'

-- 上游表检验

select * from dm_ordi_predict.cityflows_fast_resultz_online 
where inc_day='$[time(yyyyMMdd,-1d)]'
and object_type = '航空城市流向'
and split(object_code,'-')[0] = '519'
and split(object_code,'-')[1] = '514'
and predicted_datetime = '2023-07-18'



select count(1) from 
    (
    select 
    t1.*
    from dm_ordi_predict.cityflows_fast_resultz_online t1
    left join 
    dm_ordi_predict.cf_air_list t2
    on t1.object_code=t2.cityflow
    where t2.cityflow is null
    and t1.inc_day='$[time(yyyyMMdd,-1d)]'  --20230417修改取数分区为昨日的分区
    and t1.object_type = '航空城市流向' 
) al where predicted_datetime = '2023-07-18'

-- 重量校验
select dist_code,arrive_province,send_province,avg_weight from 
tmp_dm_predict.tmp_dm_cityflow_month_wgt_1
where  dist_code = '519'
-- and arrive_province = '江苏'
and send_province = '江苏'

 select
send_province,
avg(avg_weight) as avg_weight
		from
			tmp_dm_predict.tmp_dm_cityflow_month_wgt_1
            where  send_province = '江苏'
        group by send_province

select * from tmp_dm_predict.tmp_dm_cityflow_month_wgt_1 
where 
-- dist_code = '519'
-- and 
send_province = '江苏' 
and arrive_province = '江苏'
-- 2.07 
        select
			send_province,
			avg(avg_weight) as avg_weight
		from
			tmp_dm_predict.tmp_dm_cityflow_month_wgt_1
            where send_province = '江苏' 
            group by send_province

-- 2462967
select count(1) from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp4
    -- 原表
select * from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp4 
where predict_datetime = '2023-08-02' 
and origin_code = '913'
and dest_code = '371'

-- 5626732
select partition_key,count(1) from dm_predict.dws_fc_nohk_tekuai_predict_collecting_di
group by partition_key 



-- 省份对 未null查询
-- 加dep&过滤航空城市 之前匹配 96w   是因为 origin，dest 存在外国，dest存在###
--之后 14580  全是因为 dest_code=###
select dest_code,count(1) from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp2_test
where src_province is null or src_province = '' 
or dest_province is null or dest_province = ''
group by  dest_code

select origin_code,dest_code,count(1) from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp2_test
where src_province is null or src_province = '' 
or dest_province is null or dest_province = ''
group by origin_code ,dest_code 
-- 验证是否匹配上 dep的省份
select * from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp2
where src_province like '%省'
or dest_province like '%省'

select * from (
select partition_key ,count(1) from dm_predict.dws_fc_nohk_tekuai_predict_collecting_di 
 group by partition_key
 ) al order by partition_key desc

 -- 验证上游数据 和下游数据量

-- 08-02 :28351   全部 2296431
select count(1) from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp1_test
-- 2296431
select count(1) from  dm_ordi_predict.cityflows_fast_resultz_online t1
left join 
dm_ordi_predict.cf_air_list t2
on t1.object_code=t2.cityflow
 where t2.cityflow is null
and t1.inc_day='$[time(yyyyMMdd,-1d)]' 
and t1.object_type = '航空城市流向'

---------------- 日常验证----------------------------
-- 非空网归集-上游表验证
select '$[time(yyyyMMdd,79d)]' from dm_ordi_predict.cityflows_fast_resultz_online 
where inc_day='$[time(yyyyMMdd,-1d)]'
-- 非空网归集 验证
-- partition_key 20230802 20230801
select * from 
(select partition_key,count(1) from 
dm_predict.dws_fc_nohk_tekuai_predict_collecting_di 
group by partition_key 
) al order by al.partition_key desc 

select * from dm_predict.dws_fc_nohk_tekuai_predict_collecting_di
where partition_key = '$[time(yyyyMMdd,+79d)]'
and object_Type_Code = '8' 

-- 城市流向-省份对

select *  from dm_predict.dws_city_province_qty_di 
where inc_day= '20230515'


---------------------------------- 流线数据重刷验证-------------------

-- 74393934  202206   74978495
-- 64701514   202207  64866697
-- 流向底表  历史
select substr(inc_day,1,6) as month_1, sum(all_waybill_num_air) as all_waybill_num_air ,
sum(all_quantity_air) as all_quantity_air
from dm_ordi_predict.dws_static_cityflow_base
where is_air = '1'
and inc_day between '20220501' and '20230531'
group by substr(inc_day,1,6)

-- 流向底表 重刷1 
select substr(inc_day,1,6) as month_1, sum(all_waybill_num_air) as all_waybill_num_air 
,sum(all_quantity_air) as all_quantity_air
 from tmp_ordi_predict.dws_static_cityflow_base_202206tmp
where is_air = '1'
and inc_day between '20220601' and '20220731'
group by substr(inc_day,1,6)

-- 重刷2 
select substr(inc_day,1,6) as month_1, sum(all_waybill_num_air) as all_waybill_num_air 
,sum(all_quantity_air) as all_quantity_air
 from tmp_ordi_predict.dws_static_cityflow_base_202206tmp_1
where is_air = '1'
and inc_day between '20220601' and '20220731'
group by substr(inc_day,1,6)

-- select  all_waybill_num_air , all_quantity_air  from tmp_ordi_predict.dws_static_cityflow_base_202206tmp
-- where is_air = '1'
-- and inc_day between '20220601' and '20220731'
-- -- limit 100 
-- group by substr(inc_day,1,6)

-- 维度流向
select substr(inc_day,1,6) as month_1 ,sum(air_waybill_num) as  air_waybill_num,
sum(air_quentity_num) as  air_quentity_num from 
dm_ordi_predict.dws_air_flow_six_dims_day_newhb
where  inc_day between '20220601' and '20220731'
group by substr(inc_day,1,6)


------------------------------- 448434 --------
-- old版本
-- 2021 2586
-- 2022 2789
-- 2023 2789
select pro_year,count(1)
  from dm_bie.special_eco_pro 
-- where  pro_year='2022'
 group by pro_year

-- new 
-- 2020 9046
-- 2021 9046
-- 2022 9046
-- 2023 9001
select pro_year,count(1)
  from  dm_bie.special_eco_pro_2022new 
-- where  pro_year='2022'
 group by pro_year



--  替换表 操作
select 
	waybill_no,pro_name,level_1_type,level_2_type 
from 
	dm_bie.bie_fact_special_econ_dely_dtl  -- 驳回
where inc_day between  '$[time(yyyyMMdd,-45d)]' and '$[time(yyyyMMdd,-11d)]'


select 
	waybill_no,pro_name,level_1_type,level_2_type 
from 
	dm_bie.bie_fact_special_econ_dely_dtl_2022new
where inc_day between  '$[time(yyyyMMdd,-45d)]' and '$[time(yyyyMMdd,-11d)]'

-- 87016147  计数
-- 87016086  waybill分组后计数
select count(1) from 
(select waybill_no,count(1)
	-- waybill_no,pro_name,level_1_type,level_2_type 
from 
	dm_bie.bie_fact_special_econ_dely_dtl_2022new
where inc_day between  '$[time(yyyyMMdd,-45d)]' and '$[time(yyyyMMdd,-11d)]'
group by waybill_no
) al 
select 
    waybill_no,count(1)
from (
select waybill_no,pro_name,level_1_type,level_2_type 
from  dm_bie.bie_fact_special_econ_dely_dtl_2022new
where inc_day between  '$[time(yyyyMMdd,-45d)]' and '$[time(yyyyMMdd,-11d)]'
group by  inc_daywaybill_no,pro_name,level_1_type,level_2_type) t 
group by waybill_no 
having count(1) > 1 
--  and waybill_no in ('SF7000314637355','SF7000314611496')


-------------------------528211 去掉城市流向 第二天验证 -----------------
-- 448419任务验证
-- 2023-05-19 去掉后 5859953
-- 2023-05-18 去掉前 32811058
select count(1) from tmp_dm_predict.tmp_dm_fc_flow_predict_collecting_di_1
-- 2023-05-19 去掉后 5859953
select count(1) from tmp_dm_predict.tmp_dm_fc_flow_predict_collecting_di_2  -- +fbq/hq/area
-- 223681538
select count(1) from dm_predict.dws_fc_flow_predict_collecting_di  -- predict_datetime写入partition_key  
    -- 2023-05-18 26971953
    select count(1) from tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_2 -- 原城市流向tmp表

-- 452259任务验证
---  -- 1386877452
select count(1) from  tmp_ordi_predict.tmp_shenhy_air_special_econ_tmp3   --结果中间表 452259任务
-- 1386877447
select count(1) from tmp_ordi_predict.tmp_shenhy_air_special_econ_tmp2   -- 主表



--------------------------- 非空网 过滤cityflow，以及城市名称错位 -------------------
select  * from dm_predict.dws_fc_nohk_tekuai_predict_collecting_di 
where partition_key = '20230525'
 and origin_code = 'RYG'
 and dest_code = '577'
-- and origin_code = '859'
-- and dest_code = '886'


select * from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp2
where origin_Code = 'RYG'

select * from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp1  
where object_code = 'RYG-574'


select object_code ,object_name ,count(1) from (
select 
    t1.object_code,t1.object_name,split(t1.object_name,'-')[0] as origin_Name
,split(t1.object_name,'-')[1] as dest_Name
from dm_ordi_predict.cityflows_fast_resultz_online t1
left join 
dm_ordi_predict.cf_air_list t2
on t1.object_code=t2.cityflow
where t2.cityflow is null
and t1.inc_day='$[time(yyyyMMdd,-1d)]'  --20230417修改取数分区为昨日的分区
and t1.object_type = '航空城市流向' 
 and object_code = 'RYG-577'
) al
where (split(al.object_name,'-')[1] is null or split(al.object_name,'-')[1] = '')
or(split(al.object_name,'-')[0] is null or split(al.object_name,'-')[0] = '')
group by object_code ,object_name

-- city,province 
select * from dm_ops.dim_city_area_distribution 
where city_code in ('577','859','886','RYG')
-- city_name,replace(replace(t6.provinct_name,'省',''),'自治区','')
select * from dim.dim_department
where dept_code = '577'
and delete_flg='0'AND country_code ='CN'

-- 非空网城市问题
-- 历史数据回刷  -- 5622608 
select  count(1) from dm_predict.dws_fc_nohk_tekuai_predict_collecting_di 

show partitions  dm_predict.dws_fc_nohk_tekuai_predict_collecting_di 


select * from dm_ordi_predict.cityflows_fast_resultz_online

-- 1456299
select count(1) from (
select 
t1.*
from dm_ordi_predict.cityflows_fast_resultz_online t1
left join 
dm_ordi_predict.cf_air_list t2
on t1.object_code=t2.cityflow
where t2.cityflow is null
and t1.inc_day='$[time(yyyyMMdd,-1d)]'  --20230417修改取数分区为昨日的分区
and t1.object_type = '航空城市流向' 
and t1.object_code regexp '(\\\d{3})-(\\\d{3})'  
and t1.object_code = '859-886'
) al 
-- 2295783
select  count(1) from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp1 



--------------去丰网-------------------------

select * From dm_ordi_predict.dws_dynamic_cityflow_base_hi
where inc_day='20230522'
and pickup_hq_code = 'CN39'


show partitions dm_ordi_predict.dws_dynamic_cityflow_base_hi

-- 1446722301 -- 总计
-- 1065050  inc_day = '20230512'
-- 1064594 inc_day = '20230512'
  select count(1) from dm_ordi_predict.dws_static_his_cityflow
  where inc_day = '20230512'

-- 28277028
--  select * from (
-- 58063  inc_day = '20230512'
-- 58020 inc_day = '20230512'  -- 不用nvl  58020
 select count(1) from dm_ordi_predict.dws_static_his_cityflow
 where
--   (nvl(income_code,'')='丰网' or nvl(hq_code,'')='CN39') 
(income_code='丰网' or hq_code = 'CN39') 
 and inc_day = '20230512'
--  )al where al.income_code = '' and al.hq_code = ''

--1418418110  --二者合计 1446695138
-- 1006953 inc_day = '20230512'
-- 1006574 inc_day = '20230512'  -- 不用nvl 1006540
select count(1) FROM dm_ordi_predict.dws_static_his_cityflow
where  !(income_code='丰网' or hq_code = 'CN39')
-- !(nvl(income_code,'')='丰网' or nvl(hq_code,'')='CN39') 
and inc_day = '20230512'
--    income_code <>'丰网' and  hq_code<>'CN39'

-- 验证非是为什么数据相加不一致  --> hq_code 或者income_code 存在 '' 
select cityflow,sum(num) as num from (
    select * from 
 (
 select cityflow,count(1) as num from dm_ordi_predict.dws_static_his_cityflow
 where (income_code='丰网' or hq_code='CN39') 
 and inc_day = '20230512'
 group by cityflow
) union all 
 (
 select cityflow,count(1) as num FROM dm_ordi_predict.dws_static_his_cityflow
where  !(income_code='丰网' or hq_code='CN39') 
and inc_day = '20230512'
 group by cityflow
)  
) al group By cityflow

-- 
select * from dm_ordi_predict.dws_static_his_cityflow
where inc_day = '20230512'
and cityflow in ('#-#','#-020','#-024','#-431','#-535','#-663','#-769','010-#')

select *
FROM
  dm_ordi_predict.dws_static_his_cityflow
  where cityflow = '574-574' and 
 inc_day = '20230512'

 and hq_code='CN39'
 and income_code !='丰网'

   (income_code='丰网'
  and hq_code='CN39')

---  insertover write 写法
  -- 916626186
-- 909396647 --  去掉2023
-- 894196620 --  去掉2022 
-- 889072594 --  去掉2021  
select count(1) from dm_ordi_predict.dws_static_cityflow_base
where inc_day >= '20210101' 
and (nvl(income_code,'')='丰网' or nvl(hq_code,'')='CN39')

-- 1447757648 05-22 被更新， 大概数值
-- 1419824883 求导 2021，2022，2023
select count(1) from dm_ordi_predict.dws_static_his_cityflow
where inc_day >= '20210101' 
and (nvl(income_code,'')='丰网' or nvl(hq_code,'')='CN39')

--------- 回收站恢复 --- 对比

SELECT sum(all_quantity) as all_quantity_001 FROM dm_ordi_predict.dws_static_001_base 
where inc_day = '20230502';

select 
 sum(all_quantity) as all_quantity ,
 sum(all_quantity_air) as all_quantity_air,
 sum(fact_air_quantity) as fact_air_quantity	
from dm_ordi_predict.dws_static_cityflow_base 
where inc_day = '20230502';



-- 20210101 开始
show partitions dm_ordi_predict.dws_static_cityflow_base_old0523
show partitions dm_ordi_predict.dws_static_his_cityflow_old0523


-- 核查数据内容
--  38619227   inc_day=20210202
--  6867500    inc_day=20220202
--  34467877   inc_day=20230522
 -- 10656062159   inc_day='20210101' and '20211231'
select sum(all_quantity) as all_quantity  from dm_ordi_predict.dws_static_001_base 
where type = 'pickup'
and inc_day between  '20210101' and '20211231'
-- 38619227    inc_day=20210202
--  6867500    inc_day=20220202
--  34467877   inc_day=20230522
-- 10656062158    inc_day='20210101' and '20211231'
select sum(all_quantity) as all_quantity 
from dm_ordi_predict.dws_static_cityflow_base_old0523
where inc_day between '20210101' and '20211231'

select sum(all_quantity) as all_quantity
from dm_ordi_predict.dws_static_cityflow_base
where inc_day = '20230522'

-- 200111273
select * from dm_ordi_predict.dws_static_cityflow_base
where inc_day between '20230522' and '20230522'
and income_code  = '丰网'

-- 200111273
select count(1) from dm_ordi_predict.dws_static_cityflow_base_old0523
where inc_day between '20210101' and '20211231'


select * from dm_ordi_predict.dws_static_his_cityflow
where inc_day between '20230522' and '20230522'
and hq_code ='CN39'

----------------------------------  城市维度表开发--------------------
-- 验证流向底表是否存在  city_code ,不在辅助表



-- 358 
-- 0, 980 
- !0  999
select count(1) from dim.dim_department
 where
  delete_flg='0' AND
--  country_code ='CN'and
  dept_code = dist_code 
 and dist_code in ('886','853','852')

 select 
    city_code,
    area_code,
	distribution_code,
	hq_code,
	city_name,
	city,
	area_name,
	distribution_name,
	hq_name,
	province_name,
	province
from dm_ops.dim_city_area_distribution
where city_code  in ('886','853','852')

-- 886 853 852  港澳台 -- distri有，depart-country_code不是cn，是code。rib
-- dep , distribute 没有数据
-- dep, 存在dist_code ,dept_code为拼音，  比如 Gansusheng

select t1.dist_code,t2.city_code  from (
select * from dim.dim_department
 where delete_flg='0'AND country_code ='CN'
 and dept_code = dist_code ) t1 
 right join dm_ops.dim_city_area_distribution t2
 on t1.dist_code = t2.city_code 
 where t1.dist_code is null 


select 
dept_code,
	dist_code as city_code,  
	area_code,
	division_code,
	hq_code,
	city_name,
	city_name as city,
	area_name,
	division_name,
	hq_name,
	provinct_name as province_name,
	replace(replace(provinct_name,'省',''),'自治区','') as province
from dim.dim_department 
 where delete_flg = '0'
 and country_code ='CN'
 and dept_code = dist_code 
--  and dist_code = 'Gansusheng'

select 
    city_code,
    area_code,
	distribution_code,
	hq_code,
	city_name,
	city,
	area_name,
	distribution_name,
	hq_name,
	province_name,
	province
from dm_ops.dim_city_area_distribution
where city_code = '024'


select * from dm_ordi_predict.dim_city_level_mapping_df 
where inc_day = '20230525'
and city_code = 'LON'


select * from dim.dim_department
where dist_code = 'LON'

'LON' and 
('565','731','733','888','910','#')
('CAN','FIN','HGH','HKG','HOU','LON','PEK','PVG','SDA','SZX','WAS','仁川','')


------------------------- 城市维度表 -----

select * from dm_ordi_predict.dim_city_level_mapping_df 
where inc_day = '$[time(yyyyMMdd,-1d)]'
and city_name like '%自治%'

select * from  dm_ops.dim_city_area_distribution 
where city_code in ('433','908','909','970','973','974','999','634','8982')

select * from dim.dim_department 
where dist_code in ('732','410','413','910','731','565','733' ,'634','8982')
and dist_code = dept_code 

-- ,'634','8982'
--  ('732','410','413','910','731','565','733'  


select * from dim.dim_department 
where dist_code in ('994')
and dist_code = dept_code 

----------------------------------- fbq,hq,area 验证
select 
t1.city_code as tmp_city_code
,t1.area_code as tmp_area_code 
,t1.fbq_code as tmp_fbq_code
,t1.hq_code as tmp_hq_code
,t2.city_code 
,t2.area_code as area_code 
,t2.distribution_code as distribution_code
,t2.hq_code as hq_code
 from tmp_dm_predict.tmp_dm_fc_city_2_hq_df_1 t1
left join 
(select * from dm_ordi_predict.dim_city_level_mapping_df
where inc_day = '20230525') t2 
on t1.city_code = t2.city_code 



---  null
select * from dm_ordi_predict.dim_city_level_mapping_df
where inc_day = '20230526'
and  area_code in ('111Y' ,'222Y','300Y' ,'333Y' ,'555Y','666Y' ,'700Y' ,'777Y' ,'888Y' ,'999Y') 

-- 没有
select * from dim.dim_department 
where area_code  in ('111Y' ,'222Y','300Y' ,'333Y' ,'555Y','666Y' ,'700Y' ,'777Y' ,'888Y' ,'999Y') 
and dist_code = dept_code 

-- 没有
select * from  dm_ops.dim_city_area_distribution 
where area_code  in ('111Y' ,'222Y','300Y' ,'333Y' ,'555Y','666Y' ,'700Y' ,'777Y' ,'888Y' ,'999Y') 


select * from dm_ops.dim_city_area_distribution 
where city_code in ('634','8982')

select * from dim.dim_department 
where dist_code in ('634','8982')
and dist_code = dept_code 


-- 对应area_code in ('898Y','531Y')
select city_code,area_code from 
dm_fin_itp.itp_vs_rel_city_bg_area_mpp a 
where BG_code='SF001' and to_tm='9999-12-31'
and hq_version='YJ'
and city_code in ('634','8982')

select * from 
tmp_dm_predict.tmp_dm_06_fc_jyfb_level_1 
where dept_code in ('634','8982')

select * from (
select area_code,fbq_code,hq_code
from tmp_dm_predict.tmp_dm_06_fc_jyfb_level_10 
group by area_code,fbq_code,hq_code
) a
where a.area_code in  ('898Y','531Y')

select * from (
select area_code,fbq_code,hq_code from 
-- tmp_dm_predict.tmp_dm_06_fc_jyfb_level_9
-- tmp_dm_predict.tmp_dm_06_fc_jyfb_level_8
group by  area_code,fbq_code,hq_code
) a where a.area_code in  ('898Y','531Y')


-- 缺失的fbq来自于这个表
select
            area_code
			,fbq_code
			,fbq_name
        from dm_oewm.dim_area_fbq
    where area_code in  ('898Y','531Y')

select * from tmp_dm_predict.tmp_dm_06_fc_jyfb_level_8
   where area_code in  ('898Y','531Y')



--------------- 验证非空网 ------------
select count(1) from  tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp20230526   -- 1456461

select count(1) from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp2  --  1456461

-- 142479
select count(1) from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp2 
where( origin_Name is  null or origin_Name = '' 
or dest_Name is  null or dest_Name = '' 
or src_province  is  null or src_province = '' 
or dest_province  is  null or dest_province = '' 
)

select *  from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp2
where
 origin_code = '852' 
and dest_code = '772'

-- 0 
select count(1) from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp20230526 
where( origin_Name is  null or origin_Name = '' 
or dest_Name is  null or dest_Name = '' 
or src_province  is  null or src_province = '' 
or dest_province  is  null or dest_province = '' 
)

--- 验证 528211   --------------- 


select count(1)  from tmp_dm_predict.tmp_dm_fc_flow_predict_collecting_di_2;  -- 5942043


-- object_type_code = 2 5 ,6 
select object_type_code ,object_type_name ,count(1)  
 from tmp_dm_predict.tmp_dm_fc_flow_predict_collecting_di_2
 group by object_type_code ,object_type_name
-- 2  有空值，其他类型无
 select * from  tmp_dm_predict.tmp_dm_fc_flow_predict_collecting_di_2
 where object_type_code = '2'
 and (origin_name is null or origin_name = '' or dest_name is null or dest_name = '' ) 

select count(1) from tmp_dm_predict.tmp_dm_fc_flow_predict_collecting_di_2;-- 5942043
select count(1) from dm_predict.dws_fc_flow_predict_collecting_di   -- 历史数据 270521965
select count(1) from tmp_dm_predict.dws_fc_flow_predict_collecting_di_tmp20230526   -- 5942043

select * from tmp_dm_predict.dws_fc_flow_predict_collecting_di_tmp20230526   
where object_type_code  = '2'
and origin_code = '029'

select count(1) from tmp_dm_predict.dws_fc_flow_predict_collecting_di_tmp20230526   
 where (origin_name is null or origin_name = '' or dest_name is null or dest_name = '' ) 

-------------------- 499690  -----------------------------------
-- 425505条数
 select  split(object_code,'-')[0],split(object_code,'-')[1],
 split(object_name,'-')[0], split(object_name,'-')[1]
   from tmp_dm_predict.tmp_shenhy_hk_dims_fixed_tmp3_1 
where  

-- nvl(split(t1.object_code,'-')[0],'a')=t2.city_code
object_name 

select * from dm_ordi_predict.dim_city_level_mapping_df
where city_code in ('876','483');

select count(1)  from dm_predict.hk_cityflow_qty_predict_day_short_period  -- 历史 150633238

 select  count(1) from tmp_dm_predict.tmp_shenhy_hk_dims_fixed_tmp3_1  -- 425505
 
select count(1) from tmp_dm_predict.hk_cityflow_qty_predict_day_short_period_tmp20230526  -- 425505

-- 64    513-897
 select  * from tmp_dm_predict.tmp_shenhy_hk_dims_fixed_tmp3_1  
 where object_name is null or object_name = ''

select * from  tmp_dm_predict.tmp_shenhy_hk_dims_fixed_tmp3_1  
 where object_code = '513-897'
select * from tmp_dm_predict.hk_cityflow_qty_predict_day_short_period_tmp20230526 
 where object_code = '513-897'


-- 第二张表  -- dm_predict.dws_fc_hk_six_dims_predict_collecting_di
 select count(1) from tmp_dm_predict.tmp_shenhy_hk_dims_fixed_concat_tmp9;  -- 11477004
select count(1) from  dm_predict.dws_fc_hk_six_dims_predict_collecting_di -- 历史数据 56066544
 select count(1) from tmp_dm_predict.dws_fc_hk_six_dims_predict_collecting_di_tmp20230526  -- 11477004

 select * from tmp_dm_predict.dws_fc_hk_six_dims_predict_collecting_di_tmp20230526
where origin_code = '591' and dest_code = '897'
-- 航空流向营运维度  ,航空流向营运维度省份汇总
-- 591 897 
 select *from tmp_dm_predict.tmp_shenhy_hk_dims_fixed_concat_tmp9   -- 68条
where origin_code = '591' and dest_code = '897'

-- object_type_name  =  '航空流向营运维度'  and
 (origin_name is null or origin_name = '' 
    or dest_name is null or dest_name = ''
    or src_province is null or src_province = ''
    or dest_province is null or dest_province = '' )

     select count(1) from tmp_dm_predict.dws_fc_hk_six_dims_predict_collecting_di_tmp20230526   -- 0
where 
-- object_type_name  =  '航空流向营运维度'  and
 (origin_name is null or origin_name = '' 
    or dest_name is null or dest_name = ''
    or src_province is null or src_province = ''
    or dest_province is null or dest_province = '' )

