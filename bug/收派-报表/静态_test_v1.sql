/**
计算2022年的比例，用于2023预测数据 -- 测试
春节 - 日期报表
    1 取历史一段时间的收件且已经签收的运单，看他们在未来日期的签收比例
任务ID : 1009499
**/

-- 春节收件量  
DROP TABLE tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp1_tmp2022;
CREATE TABLE tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp1_tmp2022 stored AS parquet AS
SELECT  to_date(t1.consigned_tm)                                                    AS consigned_date
       ,product_code
       ,CASE WHEN dest_dist_code is null THEN t2.dist_code  ELSE dest_dist_code END AS dest_dist_code    -- 补充城市代码
       ,CASE WHEN dest_area_code is null THEN t2.area_code  ELSE dest_area_code END AS dest_area_code    -- 补充业务区代码
       ,COUNT(waybill_no)                                                           AS all_waybill_num   -- 票量
       ,SUM(quantity)                                                               AS all_quantity      --件量 
FROM dwd.dwd_waybill_info_dtl_di t1
LEFT JOIN
(
    SELECT  dept_code
           ,dist_code
           ,area_code
    FROM dim.dim_dept_info_df
    where inc_day='$[time(yyyyMMdd,-1d)]'
    GROUP BY  dept_code
             ,dist_code
             ,area_code
) t2
ON nvl(t1.dest_zone_code, 'a') = t2.dept_code
WHERE inc_day BETWEEN '20220123' AND '20220206'     -- 修改日期范围-2024-01-26
GROUP BY  to_date(t1.consigned_tm)
         ,product_code
         ,CASE WHEN dest_dist_code is null THEN t2.dist_code  ELSE dest_dist_code END
         ,CASE WHEN dest_area_code is null THEN t2.area_code  ELSE dest_area_code END
;

-- 春节派件量
DROP TABLE tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp2_tmp2022;
CREATE TABLE tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp2_tmp2022 stored AS parquet AS
SELECT  t1.*
       ,t2.sign_date
       ,t2.all_waybill_num AS d_waybill   -- 派送票量
       ,t2.all_quantity    AS d_quantity  -- 派送件量 
FROM tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp1_tmp2022 t1
LEFT JOIN
(
    SELECT  to_date(t1.consigned_tm)                                                                                                 AS consigned_date
           ,product_code
           ,CASE WHEN to_date(signin_tm) > '2022-02-06' or to_date(signin_tm) is null THEN '2900-99-99'  ELSE to_date(signin_tm) END AS sign_date
           ,dest_dist_code
           ,dest_area_code
           ,COUNT(waybill_no)                                                                                                        AS all_waybill_num
           ,SUM(quantity)                                                                                                            AS all_quantity --件量 
    FROM dwd.dwd_waybill_info_dtl_di t1
    WHERE inc_day BETWEEN '20220123' AND '20220206'     -- 修改日期范围-2024-01-26
    GROUP BY  to_date(t1.consigned_tm)
             ,product_code
             ,CASE WHEN to_date(signin_tm) > '2022-02-06' or to_date(signin_tm) is null THEN '2900-99-99'  ELSE to_date(signin_tm) END
             ,dest_dist_code
             ,dest_area_code
) t2
ON t1.consigned_date = t2.consigned_date 
AND t1.product_code = t2.product_code 
AND nvl(t1.dest_dist_code, 'a') = nvl(t2.dest_dist_code, 'b') 
AND nvl(t1.dest_area_code, 'a') = nvl(t2.dest_area_code, 'b');

-- 补充产品版块 
DROP TABLE tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp3_tmp2022;
CREATE TABLE tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp3_tmp2022 stored AS parquet AS
SELECT  t1.*
       ,CASE WHEN t2.income_code is null THEN '其他'  ELSE t2.income_code END             AS income_code        -- 取产品板块
       ,t4.area_code                                                                    AS dest_area_code_1     -- 城市汇总业务区
       ,t4.area_name                                                                    AS dest_area_name       -- 业务区名称
       ,CASE WHEN DATEDIFF(sign_date,t1.consigned_date) = 0 THEN 'T+0'
             WHEN DATEDIFF(sign_date,t1.consigned_date) = 1 THEN 'T+1'
             WHEN DATEDIFF(sign_date,t1.consigned_date) = 2 THEN 'T+2'
             WHEN DATEDIFF(sign_date,t1.consigned_date) = 3 THEN 'T+3'
             WHEN DATEDIFF(sign_date,t1.consigned_date) = 4 THEN 'T+4'
             WHEN DATEDIFF(sign_date,t1.consigned_date) = 5 THEN 'T+5'
             WHEN DATEDIFF(sign_date,t1.consigned_date) = 6 THEN 'T+6'
             WHEN DATEDIFF(sign_date,t1.consigned_date) = 7 THEN 'T+7'
             WHEN DATEDIFF(sign_date,t1.consigned_date) = 8 THEN 'T+8'
             WHEN DATEDIFF(sign_date,t1.consigned_date) = 9 THEN 'T+9'
             WHEN DATEDIFF(sign_date,t1.consigned_date) = 10 THEN 'T+10'
             WHEN DATEDIFF(sign_date,t1.consigned_date) = 11 THEN 'T+11'
             WHEN DATEDIFF(sign_date,t1.consigned_date) = 12 THEN 'T+12'
             WHEN DATEDIFF(sign_date,t1.consigned_date) = 13 THEN 'T+13'
             WHEN DATEDIFF(sign_date,t1.consigned_date) = 14 THEN 'T+14'
             WHEN DATEDIFF(sign_date,t1.consigned_date) = 15 THEN 'T+15' ELSE '其他' END AS d_period    -- 派送周期
FROM tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp2_tmp2022 t1
LEFT JOIN
(
    SELECT  product_code
           ,income_code
    FROM dm_predict.dim_prod_base_config_full
    WHERE inc_day = '9999' 
) t2
ON t1.product_code = t2.product_code
LEFT JOIN
(
    SELECT  city_code
           ,area_code
           ,area_name
    FROM dm_ordi_predict.itp_vs_rel_city_bg_area_mpp
    WHERE BG_code = 'SF001'
    AND to_tm = '9999-12-31'
    AND area_code NOT IN ('852Y', '886Y')
    AND hq_version = 'YJ' 
) t4
ON nvl(t1.dest_dist_code, 'a') = t4.city_code
;

DROP TABLE tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp4_tmp2022;
CREATE TABLE tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp4_tmp2022 stored AS parquet AS
SELECT  t1.consigned_date
       ,t1.sign_date
       ,t1.dest_area_code_1
       ,t1.dest_area_name
       ,t1.income_code
       ,t1.d_period
       ,SUM(d_waybill)  AS d_waybill
       ,SUM(d_quantity) AS d_quantity --件量 
       ,t2.p_waybill
       ,t2.p_quantity
FROM
(
    SELECT  *
    FROM tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp3_tmp2022
    WHERE d_period <> '其他'
) t1
LEFT JOIN
(
    SELECT  consigned_date
           ,dest_area_code_1
           ,income_code
           ,SUM(all_waybill_num) AS p_waybill
           ,SUM(all_quantity)    AS p_quantity --件量 
    FROM
    (
        SELECT  consigned_date
               ,dest_area_code_1
               ,income_code
               ,all_waybill_num
               ,all_quantity
        FROM tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp3_tmp2022
        GROUP BY  consigned_date
                 ,dest_area_code_1
                 ,income_code
                 ,all_waybill_num
                 ,all_quantity
    ) t
    GROUP BY  consigned_date
             ,dest_area_code_1
             ,income_code
) t2
ON t1.consigned_date = t2.consigned_date AND t1.dest_area_code_1 = t2.dest_area_code_1 AND t1.income_code = t2.income_code
GROUP BY  t1.consigned_date
         ,t1.sign_date
         ,t1.dest_area_code_1
         ,t1.dest_area_name
         ,t1.income_code
         ,t1.d_period
         ,t2.p_waybill
         ,t2.p_quantity;




-- DROP TABLE  tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp5_tmp2022;
-- CREATE TABLE tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp5_tmp2022 stored AS parquet AS
-- SELECT  t1.consigned_date
--        ,t1.sign_date
--        ,t1.area_code                     AS dest_area_code_1
--        ,t1.area_name                     AS dest_area_name
--        ,t1.income_code
--        ,t1.d_period
--        ,nvl(d_waybill,0)                 AS d_waybill
--        ,nvl(d_quantity,0)                AS d_quantity --件量 
--        ,nvl(t2.p_waybill,t3.p_waybill)   AS p_waybill
--        ,nvl(t2.p_quantity,t3.p_quantity) AS p_quantity --件量 
--        ,t1.old_consigned_date
-- -- FROM dm_ordi_predict.dm_waybill_newyear_2024_df t1
-- FROM tmp_ordi_predict.dm_waybill_newyear_2022_df_v2 t1
-- LEFT JOIN tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp4_tmp2022 t2
-- ON t1.old_consigned_date = t2.consigned_date AND t1.sign_date = t2.sign_date AND t1.income_code = t2.income_code AND t1.area_code = t2.dest_area_code_1
-- LEFT JOIN
-- (
--     SELECT  consigned_date
--            ,dest_area_code_1
--            ,income_code
--            ,nvl(p_waybill,0)  AS p_waybill
--            ,nvl(p_quantity,0) AS p_quantity --件量 
--     FROM tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp4_tmp2022
--     GROUP BY  consigned_date
--              ,dest_area_code_1
--              ,income_code
--              ,nvl(p_waybill,0)
--              ,nvl(p_quantity,0)
-- ) t3
-- ON t1.old_consigned_date = t3.consigned_date AND t1.area_code = t3.dest_area_code_1 AND t1.income_code = t3.income_code;


DROP TABLE tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp0_tmp2022;
CREATE TABLE tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp0_tmp2022 stored AS parquet AS
select
       date_format(consigned_date, 'yyyy-MM-dd')  as consigned_date,
    sign_date,
    dest_area_code_1,
    income_code,
    case 
        when sign_date = '2022-01-23' then  'T+0'
        when sign_date = '2022-01-24' then  'T+1'
        when sign_date = '2022-01-25' then  'T+2'
        when sign_date = '2022-01-26' then  'T+3'
        when sign_date = '2022-01-27' then  'T+4'
        when sign_date = '2022-01-28' then  'T+6'
        when sign_date = '2022-01-29' then  'T+7'
        when sign_date = '2022-01-30' then  'T+8'
        when sign_date = '2022-01-31' then  'T+9'
        when sign_date = '2022-02-01' then  'T+10'
        when sign_date = '2022-02-02' then  'T+11'
        when sign_date = '2022-02-03' then  'T+12'
        when sign_date = '2022-02-04' then  'T+13'
        when sign_date = '2022-02-05' then  'T+14'
        when sign_date = '2022-02-06' then  'T+15'
    end as  d_period,
    d_waybill / p_waybill as d_rate,
    0 as d_future_rate,
	replace(consigned_date,'-','') as inc_day
from 
tmp_ordi_predict.lh_dm_waybill_newyear_d_tmp4_tmp2022
;
