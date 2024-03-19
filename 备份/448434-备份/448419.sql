
set hive.auto.convert.join=true;  -- 预防倾斜
set spark.sql.extensions=org.apache.kyuubi.sql.KyuubiSparkSQLExtension; -- 合并小文件

-- 只取航空流向、判断是否为航空件
drop table if exists tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp2;
create table tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp2 
stored as parquet as  
select
	t2.*
	/* ,case   --注意：2021.04.01前规则需要变化
        when t2.product_code = 'SE0146' and t2.limit_type_code ='T4'    then '1'   -- 20220905添加
        when t2.product_code = 'SE000201' and t2.limit_type_code ='T105'    then '1'
		when t2.product_code = 'SE0001'   and t2.limit_type_code ='T4'      then '1'
		when t2.product_code = 'SE0107'   and t2.limit_type_code ='T4'      then '1'
		when t2.product_code = 'SE0089'   and t2.limit_type_code ='T4'      then '1'
		when t2.product_code = 'SE0109'   and t2.limit_type_code ='T4'      then '1'
		when t2.product_code = 'SE0008'   and t2.limit_type_code ='T4'      then '1'
		when t2.product_code = 'SE0137'   and t2.limit_type_code ='T69'     then '1'
		when t2.product_code = 'SE0121'   and t2.limit_type_code ='SP619'  then '1'
		when t2.product_code = 'SE002401' and t2.limit_type_code ='T55'    then '1'
		when t2.product_code = 'SE0004'   and t2.limit_type_code ='T6'     and t2.limit_tag = 'SP6' and t2.city_flow is not null then '1'
		else '0' end as is_air
    */
       --- 20210401之前
   /* ,case when product_code in ('SE0001','SE000201','SE0003','SE0107','SE0109','SE0008','SE0089','SE0121','SE0137','SE0103','SE0051','SE002401') 
           then '1'
      else '0'
      end as is_air
      */
     ,case when product_code in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201'
                                 ,'SE0103','SE0051','SE0152','SE000206') then '1' --20230403 增加两个航空产品
            when product_code='SE0004' and limit_tag = 'SP6' then '1'
            when product_code = 'SE0008' and limit_tag in('T4','T801') then '1'
      else '0'
     end as is_air
from dm_ordi_predict.cf_air_list t1
join 
tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp1 t2
on t1.cityflow=t2.city_flow;

-- 判断是否特色经济
drop table if exists tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp3;
create table tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp3 
stored as parquet as  
select
	t1.*
	,t2.pro_name
	,t2.level_1_type
	,t2.level_2_type
	,case when t3.pro_name is not null then '1' else '0' end as if_econ_fresh
from tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp2 t1
left join 
(select 
	waybill_no,pro_name,level_1_type,level_2_type 
from 
	dm_bie.bie_fact_special_econ_dely_dtl
where inc_day between  '$[time(yyyyMMdd,-45d)]' and '$[time(yyyyMMdd,-11d)]'
union all
select 
 waybill_no,pro_name,level_1_type,level_2_type
from 
dm_ordi_predict.dws_spec_eco_pro_waybill_day
where inc_day between '$[time(yyyyMMdd,-10d)]' and  '$[time(yyyyMMdd,-1d)]')t2  -- 20220816添加
on t1.waybill_no=t2.waybill_no
left join 
dm_ordi_predict.dm_special_econ_dely_fresh_dtl t3
on t2.pro_name=t3.pro_name
where t1.is_air='1';
