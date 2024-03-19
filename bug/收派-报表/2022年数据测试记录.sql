-- 生成2023年维度表
CREATE TABLE 7.dm_waybill_newyear_2023_df_v2 stored AS parquet AS
SELECT  t1.*
       ,t2.income_code
       ,t3.area_code
       ,t3.area_name
FROM (
select consigned_date,old_consigned_date,sign_date,d_period from
(
       SELECT  '2023-01-13' as consigned_date,'2022-01-23' as old_consigned_date union ALL
       SELECT  '2023-01-14' as consigned_date,'2022-01-24' as old_consigned_date union ALL
       SELECT  '2023-01-15' as consigned_date,'2022-01-25' as old_consigned_date union ALL
       SELECT  '2023-01-16' as consigned_date,'2022-01-26' as old_consigned_date union ALL
       SELECT  '2023-01-17' as consigned_date,'2022-01-27' as old_consigned_date union ALL
       SELECT  '2023-01-18' as consigned_date,'2022-01-28' as old_consigned_date union ALL
       SELECT  '2023-01-19' as consigned_date,'2022-01-29' as old_consigned_date union ALL
       SELECT  '2023-01-20' as consigned_date,'2022-01-30' as old_consigned_date union ALL
       SELECT  '2023-01-21' as consigned_date,'2022-01-31' as old_consigned_date union ALL
       SELECT  '2023-01-22' as consigned_date,'2022-02-01' as old_consigned_date union ALL
       SELECT  '2023-01-23' as consigned_date,'2022-02-02' as old_consigned_date union ALL
       SELECT  '2023-01-24' as consigned_date,'2022-02-03' as old_consigned_date union ALL
       SELECT  '2023-01-25' as consigned_date,'2022-02-04' as old_consigned_date union ALL
       SELECT  '2023-01-26' as consigned_date,'2022-02-05' as old_consigned_date union ALL
       SELECT  '2023-01-27' as consigned_date,'2022-02-06' as old_consigned_date union all 
	   SELECT  '2023-01-28' as consigned_date,'2022-02-07' as old_consigned_date		-- 添加1天
 ) t1
left join
(
       SELECT  '2023-01-13' as sign_date,'T+0'  as d_period union ALL
       SELECT  '2023-01-14' as sign_date,'T+1'  as d_period union ALL
       SELECT  '2023-01-15' as sign_date,'T+2'  as d_period union ALL
       SELECT  '2023-01-16' as sign_date,'T+3'  as d_period union ALL
       SELECT  '2023-01-17' as sign_date,'T+4'  as d_period union ALL
       SELECT  '2023-01-18' as sign_date,'T+5'  as d_period union ALL
       SELECT  '2023-01-19' as sign_date,'T+6'  as d_period union ALL
       SELECT  '2023-01-20' as sign_date,'T+7'  as d_period union ALL
       SELECT  '2023-01-21' as sign_date,'T+8'  as d_period union ALL
       SELECT  '2023-01-22' as sign_date,'T+9'  as d_period union ALL
       SELECT  '2023-01-23' as sign_date,'T+10' as d_period union ALL
       SELECT  '2023-01-24' as sign_date,'T+11' as d_period union ALL
       SELECT  '2023-01-25' as sign_date,'T+12' as d_period union ALL
       SELECT  '2023-01-26' as sign_date,'T+13' as d_period union ALL
       SELECT  '2023-01-27' as sign_date,'T+14' as d_period union ALL
	   SELECT  '2023-01-28' as sign_date,'T+15' as d_period union ALL		-- 添加1天
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
	 UNION ALL
     SELECT  '丰网' AS income_code
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

-- 生成2022年比例每天收件对应未来15天的派件量 tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp5_tmp2022
	--  上游任务 ID:1009499
CREATE TABLE tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp0_tmp2022 stored AS parquet AS
select
       date_format(consigned_date, 'yyyy-MM-dd')  as consigned_date,
    sign_date,
    dest_area_code_1,
    income_code,
    d_period,
    d_waybill / p_waybill as d_rate,
    0 as d_future_rate,
	replace(consigned_date,'-','') as inc_day
from 
tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp4_tmp2022
;


/***

select * from 
dm_ordi_predict.dws_pick_2_deliver_waybill_newyear2023
where income_code ='其他'

select * from 
dm_ordi_predict.dm_waybill_newyear_2024_df
where income_code ='其他'


 SELECT  dept_code
           ,dist_code
           ,area_code
    FROM dim.dim_dept_info_df
    where inc_day='$[time(yyyyMMdd,-1d)]'
    GROUP BY  dept_code
             ,dist_code
             ,area_code

select * from (
select 
consigned_date,sign_date,dest_area_code_1,income_code,d_period,
sum(d_waybill) as d_waybill,
sum(p_waybill) as p_waybill
from tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp4_20240201
group by consigned_date,sign_date,dest_area_code_1,income_code,d_period
)where consigned_date = '2022-01-23'
and income_code = '电商'
and dest_area_code_1 = '010Y'


-- 2022 测试 
 -- 2022 生成d_rate
select d_period,count(1) from 
tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp4_tmp2022
group by d_period

select * from tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp4_tmp2022 
where consigned_date = '2022-01-23'
and income_code = '电商'
and dest_area_code_1 = '010Y'

select * from tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp0_tmp2022
  -- 2022 维度表I
select * from tmp_ordi_predict.dm_waybill_newyear_2022_df_v2

-- 查看2023生成的预测数据  -- 截止2023-01-19 

select count(1) from tmp_ordi_predict.dws_pick_2_deliver_waybill_newyear2022_2023   -- 30241 
select count(1) from  dm_ordi_predict.dws_pick_2_deliver_waybill_newyear2023   -- 去年  48600

select 
  consigned_date,
    sign_date,
    sum(p_waybill) as p_waybill,
    sum(d_future_waybill) as d_future_waybill
 from tmp_ordi_predict.dws_pick_2_deliver_waybill_newyear2022_2023
group by consigned_date,
    sign_date

**/