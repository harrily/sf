
	-- dm_ordi_predict.dm_waybill_newyear_2024_df  -- 今收今派，今收未来派   -- 维表

tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp0 [ old版本： tmp_ordi_predict.tmp_dm_waybill_p_2_d_tmp6]   -- d_rate ,d_future_rate ,dest_area_code_1 , income_code , d_period ,consigned_date, sign_date



2024-02-09 -- 除夕 
2023-01-21 -- 除夕

1、dm_ordi_predict.dm_waybill_newyear_2024_df  表，新旧寄件日期，重新对齐
2、生成tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp0 [ old版本： tmp_ordi_predict.tmp_dm_waybill_p_2_d_tmp6]  ， 去年派件比例，未来比例
3、日期范围修改： 
	-- 651119   -- 动态  3点
	-- 650045   -- 动态  15点
	-- 1002270  -- 静态 

测试后续： 
	-、静态测试： 
		tmp_ordi_predict.dm_waybill_newyear_2024_df_v2
	    tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp0
	    date_add('2023-01-28',385) 

/**
春节 - 日期报表
    1 取历史一段时间的收件且已经签收的运单，看他们在未来日期的签收比例
    2 然后 我们有春节每天的收件量，用收件量乘以比例，来预测未来天数的派件量
    默认是在几天內派送完，比如八天，那剩下的那些比例都给到第八天
**/

-- 生成比例，
	-- 版本1 
	-- 直接拿去年结果值计算   -- 43155   + 4725

DROP TABLE tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp0;
CREATE TABLE tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp0 stored AS parquet AS
select
    date_format(date_add(consigned_date,30), 'yyyy-MM-dd') as consigned_date,   -- 和主表配平  生成23年春节
    date_format(date_add(sign_date,30), 'yyyy-MM-dd') as sign_date,			    -- 和主表配平  生成23年春节
    dest_area_code as dest_area_code_1,
    income_code,
    d_period,
    if(d_period = '其他',0,d_future_waybill / p_waybill)  as d_rate,
    0 as d_future_rate
from 
dm_ordi_predict.dws_pick_2_deliver_waybill_newyear2023
where inc_day between '20230113' and '20230127'
and income_code != '丰网' 
union all 
select
    date_add(date_add(consigned_date,1),30) as consigned_date, 																-- 和主表配平
    if(sign_date != '2099-99-99',date_format(date_add(date_add(sign_date,1),30), 'yyyy-MM-dd'),sign_date) as sign_date,  	-- 和主表配平
    dest_area_code as dest_area_code_1,
    income_code,
    'T+15' as d_period,
   if(d_period = '其他',0,d_future_waybill / p_waybill) as d_rate,
    0 as d_future_rate
from 
dm_ordi_predict.dws_pick_2_deliver_waybill_newyear2023
where inc_day = '20230127'
and income_code != '丰网'
union all 
select
  date_format(date_add(consigned_date,30), 'yyyy-MM-dd') as consigned_date,
  if(sign_date != '2099-99-99',date_format(date_add(date_add(sign_date,1),30), 'yyyy-MM-dd'),sign_date) as sign_date,
  dest_area_code as dest_area_code_1,
  income_code,
  'T+15' as d_period,
  if(d_period = '其他',0,d_future_waybill / p_waybill) as d_rate,
   0 as d_future_rate
from
  dm_ordi_predict.dws_pick_2_deliver_waybill_newyear2023
where
  d_period = 'T+14'
  and income_code != '丰网'
group by
  consigned_date,
  sign_date,
  dest_area_code,
  income_code,
  d_period,
  p_waybill,
  d_future_waybill,
  inc_day
;
-- 生成比例 
	-- 版本2 
	-- 2023的 d_rate 根据 2023运单生成 
	-- 任务ID:1011954
DROP TABLE tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp0;
CREATE TABLE tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp0 stored AS parquet AS
select
    consigned_date, 																-- 和主表配平
    sign_date,  	-- 和主表配平
    dest_area_code_1,
    income_code,
	d_period,
    d_rate,
    d_future_rate
from 
tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp0_2023_test_v1

;

/*
-- 生成 2024 区域维度表
	-- 47880 	+ 7650  + 6480 
*/
INSERT OVERWRITE TABLE dm_ordi_predict.dm_waybill_newyear_2024_df
-- CREATE TABLE tmp_ordi_predict.dm_waybill_newyear_2024_df_v2 stored AS parquet AS
SELECT  t1.*
       ,t2.income_code
       ,t3.area_code
       ,t3.area_name
FROM (
select consigned_date,old_consigned_date,sign_date,d_period from
(
--    	  SELECT  '2024-02-02' as consigned_date,'2023-01-13' as old_consigned_date union ALL
--        SELECT  '2024-02-03' as consigned_date,'2023-01-14' as old_consigned_date union ALL
--        SELECT  '2024-02-04' as consigned_date,'2023-01-15' as old_consigned_date union ALL
--        SELECT  '2024-02-05' as consigned_date,'2023-01-16' as old_consigned_date union ALL
--        SELECT  '2024-02-06' as consigned_date,'2023-01-17' as old_consigned_date union ALL
--        SELECT  '2024-02-07' as consigned_date,'2023-01-18' as old_consigned_date union ALL
--        SELECT  '2024-02-08' as consigned_date,'2023-01-19' as old_consigned_date union ALL
--        SELECT  '2024-02-09' as consigned_date,'2023-01-20' as old_consigned_date union ALL
--        SELECT  '2024-02-10' as consigned_date,'2023-01-21' as old_consigned_date union ALL
--        SELECT  '2024-02-11' as consigned_date,'2023-01-22' as old_consigned_date union ALL
--        SELECT  '2024-02-12' as consigned_date,'2023-01-23' as old_consigned_date union ALL
--        SELECT  '2024-02-13' as consigned_date,'2023-01-24' as old_consigned_date union ALL
--        SELECT  '2024-02-14' as consigned_date,'2023-01-25' as old_consigned_date union ALL
--        SELECT  '2024-02-15' as consigned_date,'2023-01-26' as old_consigned_date union ALL
--        SELECT  '2024-02-16' as consigned_date,'2023-01-27' as old_consigned_date union ALL
-- 	      SELECT  '2024-02-17' as consigned_date,'2023-01-28' as old_consigned_date
	   SELECT  '2024-02-02' as consigned_date,date_format(date_add('2024-02-02',-355), 'yyyy-MM-dd') as old_consigned_date union ALL
       SELECT  '2024-02-03' as consigned_date,date_format(date_add('2024-02-03',-355), 'yyyy-MM-dd') as old_consigned_date union ALL
       SELECT  '2024-02-04' as consigned_date,date_format(date_add('2024-02-04',-355), 'yyyy-MM-dd') as old_consigned_date union ALL
       SELECT  '2024-02-05' as consigned_date,date_format(date_add('2024-02-05',-355), 'yyyy-MM-dd') as old_consigned_date union ALL
       SELECT  '2024-02-06' as consigned_date,date_format(date_add('2024-02-06',-355), 'yyyy-MM-dd') as old_consigned_date union ALL
       SELECT  '2024-02-07' as consigned_date,date_format(date_add('2024-02-07',-355), 'yyyy-MM-dd') as old_consigned_date union ALL
       SELECT  '2024-02-08' as consigned_date,date_format(date_add('2024-02-08',-355), 'yyyy-MM-dd') as old_consigned_date union ALL
       SELECT  '2024-02-09' as consigned_date,date_format(date_add('2024-02-09',-355), 'yyyy-MM-dd') as old_consigned_date union ALL
       SELECT  '2024-02-10' as consigned_date,date_format(date_add('2024-02-10',-355), 'yyyy-MM-dd') as old_consigned_date union ALL
       SELECT  '2024-02-11' as consigned_date,date_format(date_add('2024-02-11',-355), 'yyyy-MM-dd') as old_consigned_date union ALL
       SELECT  '2024-02-12' as consigned_date,date_format(date_add('2024-02-12',-355), 'yyyy-MM-dd') as old_consigned_date union ALL
       SELECT  '2024-02-13' as consigned_date,date_format(date_add('2024-02-13',-355), 'yyyy-MM-dd') as old_consigned_date union ALL
       SELECT  '2024-02-14' as consigned_date,date_format(date_add('2024-02-14',-355), 'yyyy-MM-dd') as old_consigned_date union ALL
       SELECT  '2024-02-15' as consigned_date,date_format(date_add('2024-02-15',-355), 'yyyy-MM-dd') as old_consigned_date union ALL
       SELECT  '2024-02-16' as consigned_date,date_format(date_add('2024-02-16',-355), 'yyyy-MM-dd') as old_consigned_date union ALL
	   SELECT  '2024-02-17' as consigned_date,date_format(date_add('2024-02-17',-355), 'yyyy-MM-dd') as old_consigned_date

 ) t1
left join
(
       SELECT  '2024-02-02' as sign_date,'T+0'  as d_period union ALL
       SELECT  '2024-02-03' as sign_date,'T+1'  as d_period union ALL
       SELECT  '2024-02-04' as sign_date,'T+2'  as d_period union ALL
       SELECT  '2024-02-05' as sign_date,'T+3'  as d_period union ALL
       SELECT  '2024-02-06' as sign_date,'T+4'  as d_period union ALL
       SELECT  '2024-02-07' as sign_date,'T+5'  as d_period union ALL
       SELECT  '2024-02-08' as sign_date,'T+6'  as d_period union ALL
       SELECT  '2024-02-09' as sign_date,'T+7'  as d_period union ALL
       SELECT  '2024-02-10' as sign_date,'T+8'  as d_period union ALL
       SELECT  '2024-02-11' as sign_date,'T+9'  as d_period union ALL
       SELECT  '2024-02-12' as sign_date,'T+10' as d_period union ALL
       SELECT  '2024-02-13' as sign_date,'T+11' as d_period union ALL
       SELECT  '2024-02-14' as sign_date,'T+12' as d_period union ALL
       SELECT  '2024-02-15' as sign_date,'T+13' as d_period union ALL
       SELECT  '2024-02-16' as sign_date,'T+14' as d_period union ALL
	   SELECT  '2024-02-17' as sign_date,'T+15' as d_period union ALL
       SELECT  '2099-99-99' as sign_date,'其他' as d_period
 ) t2
where t1.consigned_date<=sign_date
) t1
LEFT JOIN
(
    SELECT  '时效' AS income_code     UNION ALL
    SELECT  '电商' AS income_code     UNION ALL
    SELECT  '国际' AS income_code     UNION ALL
    SELECT  '大件' AS income_code     UNION ALL
    SELECT  '医药' AS income_code     UNION ALL
    SELECT  '其他' AS income_code     UNION ALL
    SELECT  '冷运' AS income_code     
	-- UNION ALL
   --  SELECT  '丰网' AS income_code
) t2
LEFT JOIN
(
    SELECT  area_code
           ,area_name
    FROM dm_ordi_predict.itp_vs_rel_city_bg_area_mpp
    WHERE BG_code = 'SF001'
    AND to_tm = '9999-12-31'
    AND area_code NOT IN ('852Y', '886Y')
    AND hq_version = 'YJ'
    GROUP BY  area_code
             ,area_name
) t3;


/**


-- 收转派  春节报表
 -、根据运单生长2023年rate 
  -、2023-02-07 2023/01/27 填充 2023/1/28
  -、解决比例溢出问题。
 -、d_period 根据 维度表来， 固定T0 ~ T15

 
SELECT consigned_tm
    ,signin_tm
       ,product_code
       ,dest_dist_code    
       ,dest_area_code    -- 补充业务区代码
       ,waybill_no                                                  
       ,quantity 
    ,inc_day
FROM dwd.dwd_waybill_info_dtl_di
where inc_day BETWEEN '20230113' AND '20230127'
-- 使用20230127 填充20230128
union all 
SELECT  date_add(consigned_tm,1) as consigned_tm
    ,date_add(signin_tm,1) as signin_tm
       ,product_code
       ,dest_dist_code    
       ,dest_area_code    -- 补充业务区代码
       ,waybill_no                                                  
       ,quantity 
    ,'20230128' as inc_day
FROM dwd.dwd_waybill_info_dtl_di
where inc_day BETWEEN '20230127' AND '20230127'

对比记录： 
select count(1) from tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp0_2023_test -- 47880 

select count(1) from tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp0  -- 47880 

select * from tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp0_2023_test
where consigned_date = '2023-02-12'
and income_code = '电商'
and dest_area_code_1 =  '010Y'

select * from tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp0
where consigned_date = '2023-02-12'
and income_code = '电商'
and dest_area_code_1 =  '010Y'

select consigned_date,count(1) from tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp4_tmp2023
group by consigned_date
-- 验证 比例计算   ， 版本1 和版本2 
	-- 新旧对比
select * from (
select 
    consigned_date
    ,sign_date 
    ,dest_area_code_1 
    ,income_code
    ,d_period
    ,d_rate
    ,d_future_rate
from tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp0_2023_test 
) t1 
full outer join 
 (
select 
    consigned_date as O_consigned_date
    ,sign_date as O_sign_date 
    ,dest_area_code_1  as O_sdest_area_code_1
    ,income_code as  O_sincome_code
    ,d_period  as O_sd_period
    ,d_rate as  O_sd_rate
    ,d_future_rate as O_sd_future_rate
from tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp0 
) t2 
on t1.consigned_date = t2.O_consigned_date
and  nv(t1.sign_date,'') = nv(t2.O_sign_date,'')
and  t1.dest_area_code_1 = t2.O_sdest_area_code_1
and t1.income_code = t2.O_sincome_code
and t1.d_period = t2.O_sd_period


--  d_rate 根据2023真实值，计算，添加'其他'比例 ，最终优化结果 验证
-- 比例校验
select   consigned_date  ,dest_area_code_1,income_code,
    sum(d_rate) as sum_d_rate
from 
    -- tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp0_2023_test_v1
    tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp0
group by consigned_date,dest_area_code_1,income_code

-- 具体数据校验
select *
from 
	-- tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp0_2023_test_v1
	 tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp0
where consigned_date = '2023-02-12'
    and dest_area_code_1 = '010Y'
    and  income_code = '电商'
-- 条数校验
select count(1)-- 47880 
from tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp0_2023_test_v1
select count(1) from   -- 47880 
tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp0


**/