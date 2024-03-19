
insert overwrite table dm_ordi_predict.dim_city_level_mapping_df partition(inc_day) 
select 
	al.city_code,
	al.area_code,
	al.distribution_code,
	al.hq_code,
	al.city_name,
	al.city,
	al.area_name,
	al.distribution_name,
	al.hq_name,
	if(al.city_code is not null and al.city_code !='' and al.city_code rlike '^\\d+$',al.real_province_name,'海外') as province_name ,
	if(al.city_code is not null and al.city_code !='' and al.city_code rlike '^\\d+$',al.real_province,'海外') as province,
	al.real_province_name,  -- 真实省份名称
	al.real_province, -- 真实省份简称
	if(al.city_code is not null and al.city_code !='' and al.city_code rlike '^\\d+$','0','1') as if_foreign, -- 是否海外，0 否 1 是
    al.delete_flag, -- 是否删除，0否 1是
	'$[time(yyyy-MM-dd HH:mm:ss)]' as update_time,
	'$[time(yyyyMMdd)]' as inc_day
from (
    select al_1.*, row_number() over(partition by al_1.city_code ) as rn 
     from (
        select 
            if(t1.city_code is not null and t1.city_code != '', t1.city_code,t2.city_code) as city_code,
            if(t1.area_code is not null and t1.area_code != '', t1.area_code,t2.area_code) as area_code,
            case 
                when t2.distribution_code is not null and t2.distribution_code != '' then t2.distribution_code
                when t3.fbq_code is not null and t3.fbq_code != '' then t3.fbq_code
            else t1.distribution_code end as  distribution_code,
            if(t1.hq_code is not null and t1.hq_code != '', t1.hq_code,t2.hq_code) as hq_code,
            if(t1.city_name is not null and t1.city_name != '', t1.city_name,t2.city_name) as city_name,
            if(t1.city is not null and t1.city != '', t1.city,t2.city) as city,
            if(t1.area_name is not null and t1.area_name != '', t1.area_name,t2.area_name) as area_name,
            case 
                when t2.distribution_name is not null and t2.distribution_name != '' then t2.distribution_name
                when t3.fbq_name is not null and t3.fbq_name != '' then t3.fbq_name
            else t1.distribution_name end as  distribution_name,
            if(t1.hq_name is not null and t1.hq_name != '', t1.hq_name,t2.hq_name) as hq_name,
            if(t2.province_name is not null and t2.province_name != '', t2.province_name,t1.province_name) as real_province_name,
            if(t2.province is not null and t2.province != '', t2.province,t1.province) as real_province,
            t1.delete_flag
        from 
        (select 
            dist_code as city_code,  
            area_code,
            division_code as distribution_code,
            hq_code,
            city_name,
            replace(replace(city_name,'市',''),'自治州','') as city,
            area_name,
            division_name as distribution_name,
            hq_name,
            provinct_name as province_name,
            replace(replace(provinct_name,'省',''),'自治区','') as province,
            delete_flg as delete_flag
        from dim.dim_department 
        where 
			--  delete_flg = '0' and   
             dept_code = dist_code 
            and country_code ='CN'
            and dist_code rlike '^\\d+$'
        union all 
        select 
            dist_code as city_code,  
            area_code,
            division_code as distribution_code,
            hq_code,
            city_name,
            replace(replace(city_name,'市',''),'自治州','') as city,
            area_name,
            division_name as distribution_name,
            hq_name,
            provinct_name as province_name,
            replace(replace(provinct_name,'省',''),'自治区','') as province,
            delete_flg as delete_flag
        from dim.dim_department 
        where 
            -- delete_flg = '0' and
            dept_code = dist_code 
        and nvl(country_code,'') !='CN'
        ) t1 
        left join 
        (select 
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
        from dm_ops.dim_city_area_distribution) t2 
        on t1.city_code = t2.city_code
        left join 
        (select 
            area_code,
            area_name,
            fbq_code,
            fbq_name
        from dm_oewm.dim_area_fbq) t3 
        on t1.area_code = t3.area_code
    ) al_1 
) al where al.rn = 1 ;


/**
CREATE TABLE `dm_ordi_predict.dim_city_level_mapping_df`(
  `city_code` string COMMENT '城市编码',
  `area_code` string COMMENT '业务区编码',
  `distribution_code` string COMMENT '分拨区代码',
  `hq_code` string COMMENT '大区代码',
  `city_name` string COMMENT '城市名称',
  `city` string COMMENT '城市简称',
  `area_name` string COMMENT '业务区名称',
  `distribution_name` string COMMENT '分拨区名称',
  `hq_name` string COMMENT '大区名称',
  `province_name` string COMMENT '省份名称',
  `province` string COMMENT '省份简称',
  `real_province_name` string COMMENT '真实省份名称',
  `real_province` string COMMENT '真实省份简称',
  `if_foreign` string COMMENT '是否海外，0否 1是 ',
  `delete_flag` string COMMENT '是否删除，0否 1是 ',
  `update_time` string COMMENT '更新日期'
)partitioned by (inc_day string comment '分区日期yyyyMMdd') stored as parquet;
**/
