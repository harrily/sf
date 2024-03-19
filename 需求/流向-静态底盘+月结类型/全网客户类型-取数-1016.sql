--汇总
drop table if exists tmp_dm_predict.ly_category_yd_cityflow_1016_tmp1;
create table if not exists tmp_dm_predict.ly_category_yd_cityflow_1016_tmp1 stored as parquet as
select 
t1.src_dist_code,
t1.dest_dist_code,
t1.cityflow,
t1.days
,count(t1.waybill_no) as waybill_no
,t1.inc_day,
case when t2.companycustno is not null then '1'
          when t2.companycustno is null and (t1.freight_monthly_acct_code is not null and t1.freight_monthly_acct_code<>'null' and t1.freight_monthly_acct_code<>'') then '2'
          when t2.companycustno is null and (t1.freight_monthly_acct_code is null or t1.freight_monthly_acct_code='null' or t1.freight_monthly_acct_code='') then '3'
        end as customer_type
from 
(select
	src_dist_code
	,dest_dist_code
	,concat(nvl(src_dist_code,'#'),'-',nvl(dest_dist_code,'#')) as cityflow
	,substr(consigned_tm,1,10) as days 
	,product_code
	,limit_type_code
	,cargo_type_code	
	,limit_tag
	,freight_monthly_acct_code
	--,quantity
	,waybill_no
	,inc_day
    ,route_code	
	from dwd.dwd_waybill_info_dtl_di             --运单宽表  【需要剔除丰网】
	where inc_day 
	between '20230101' and '20231015' 
	) t1
left join       --关联
(select  companyabbr,
         custtype,
         lpad(companycustno,10,'0') as companycustno
from dm_kadm.kadm_dim_cust_mapping
group by companyabbr,
        custtype,
        lpad(companycustno,10,'0')
) t2 
on t1.freight_monthly_acct_code=t2.companycustno
group by 
t1.src_dist_code,
t1.dest_dist_code,
t1.cityflow,
t1.days
,t1.inc_day,
case when t2.companycustno is not null then '1'
          when t2.companycustno is null and (t1.freight_monthly_acct_code is not null and t1.freight_monthly_acct_code<>'null' and t1.freight_monthly_acct_code<>'') then '2'
          when t2.companycustno is null and (t1.freight_monthly_acct_code is null or t1.freight_monthly_acct_code='null' or t1.freight_monthly_acct_code='') then '3'
        end