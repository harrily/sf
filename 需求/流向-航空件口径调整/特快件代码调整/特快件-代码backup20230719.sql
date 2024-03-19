drop table if exists tmp_ordi_predict.tmp_allflow_shenhy_air_kuaiman_tmp1;
create table tmp_ordi_predict.tmp_allflow_shenhy_air_kuaiman_tmp1 
stored as parquet as 
select 
days                    
,t1.cityflow                
,case when t2.cityflow is not null then '1' else 0 end as is_air_flow			 
,weight_type_code 
,operation_type
,sum(all_waybill_num)  as tekuai_waybill_num
,sum(all_quantity) as tekuai_quantity         
,sum(weight) as tekuai_weight           
,sum(volume) as tekuai_volume      
,case when t3.province is not null 
                and split(t1.cityflow,'-')[0] rlike '^\\d+$' 
          then t3.province
          else '海外' end as src_province
    ,t3.distribution_name as src_distribution_name
    ,case when t4.province is not null 
                and split(t1.cityflow,'-')[1] rlike '^\\d+$' 
          then t4.province
          else '海外' end as dest_province
    ,t4.distribution_name as dest_distribution_name 
from (select t.* 
    ,case when product_code in ('SE0089','SE0146','SE0107') and limit_type_code ='T4' then '慢产品'
          when product_code = 'SE0152' then '慢产品'
          else '快产品'
    end as operation_type
    -- 20210401之后判断
    -- 20230403 新增两个航空产品
    ,case when product_code in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051','SE0152','SE000206') then '1'
            when product_code='SE0004' and limit_tag = 'SP6' then '1'
            when product_code = 'SE0008' and limit_tag in('T4','T801') then '1'
      else '0'
     end as is_hangkong_waybill 
-- 快慢产品判断
-- when product_code in ('SE0089','SE0146') and limit_type_code ='T4'  then '顺丰空配'
-- 		   when product_code='SE0107' and limit_type_code ='T4'  then '特快包裹'

     from dm_ordi_predict.dws_static_his_cityflow t  -- 后续改调度后用
     ) t1
left join 
      dm_ordi_predict.cf_air_list t2
on t1.cityflow=t2.cityflow 
left join 
    (select city_code,province,distribution_name 
    from dm_ops.dim_city_area_distribution a  
    group by city_code,province,distribution_name
    ) t3
    on nvl(split(t1.cityflow,'-')[0],'a')=t3.city_code
    left join 
      (select city_code,province,distribution_name 
      from dm_ops.dim_city_area_distribution a  
      group by city_code,province,distribution_name
    ) t4
    on nvl(split(t1.cityflow,'-')[1],'a')=t4.city_code    
where t1.inc_day between '$[time(yyyyMMdd,-45d)]' and '$[time(yyyyMMdd)]' 
and is_hangkong_waybill='1'
group by days                    
,t1.cityflow 
,operation_type  
,case when t2.cityflow is not null then '1' else 0 end			 
,weight_type_code 
,case when t3.province is not null 
                and split(t1.cityflow,'-')[0] rlike '^\\d+$' 
          then t3.province
          else '海外' end 
    ,t3.distribution_name 
    ,case when t4.province is not null 
                and split(t1.cityflow,'-')[1] rlike '^\\d+$' 
          then t4.province
          else '海外' end  
    ,t4.distribution_name;


set hive.exec.dynamic.partition.mode= nostrict;
set hive.exec.max.dynamic.partitions=2000;  -- 所有执行节点上最多可以多少个动态分区
set hive.exec.max.dynamic.partitions.pernode = 500;

insert overwrite table dm_ordi_predict.dws_static_cityflow_tekuai_base partition(inc_day)  
select
days                    
,cityflow                
,is_air_flow			 
,weight_type_code 
,operation_type
,tekuai_waybill_num
,tekuai_quantity         
,tekuai_weight           
,tekuai_volume      
,src_province
,src_distribution_name
,dest_province
,dest_distribution_name 
,regexp_replace(days,'-','') as inc_day
from  tmp_ordi_predict.tmp_allflow_shenhy_air_kuaiman_tmp1 ;

/*
CREATE TABLE `dm_ordi_predict.dws_static_cityflow_tekuai_kuaiman_base`(
`days` string COMMENT '收件日期',
`cityflow` string COMMENT '流向对（所有流向）',
`is_air_flow` string COMMENT '是否航空流向',
`weight_type_code` string COMMENT '重量段',
operation_type string '维度（快产品/慢产品）'
`tekuai_waybill_num` double COMMENT '特快运单数',
`tekuai_quantity` double COMMENT '特快件量',
`tekuai_weight` double COMMENT '特快重量',
`tekuai_volume` double COMMENT '特快体积',
`src_province` string COMMENT '起始省份',
`src_distribution_name` string COMMENT '起始分拨区',
`dest_province` string COMMENT '目的省份',
`dest_distribution_name` string COMMENT '目的分拨区')
PARTITIONED BY (
`inc_day` string COMMENT '收件日期yyyymmdd')
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat';*/


-- alter table dm_ordi_predict.dws_static_cityflow_tekuai_base rename to dm_ordi_predict.dws_static_cityflow_tekuai_base_bak20221221

/*
drop table dm_ordi_predict.dws_static_cityflow_tekuai_base;
create table if not exists dm_ordi_predict.dws_static_cityflow_tekuai_base(
	 days                    string comment  '收件日期'
	,cityflow                string comment  '流向对（所有流向）'
    ,is_air_flow			 string comment  '是否航空流向'
    ,weight_type_code        string comment  '重量段'
    ,income_code             string comment '产品版块'
    ,tekuai_waybill_num      string comment  '特快运单数'
    ,tekuai_quantity         string comment  '特快件量'
    ,tekuai_weight           string comment  '特快重量'
    ,tekuai_volume           string comment  '特快体积'
    ,src_province            string comment  '起始省份'
    ,src_distribution_name   string comment  '起始分拨区'
    ,dest_province           string comment  '目的省份'
    ,dest_distribution_name  string comment  '目的分拨区'
)
partitioned by (inc_day string comment'收件日期yyyymmdd')
stored as parquet;
*/
/*
drop table if exists tmp_ordi_predict.tmp_shenhy_cityflow_tekuai_tmp1;
create table tmp_ordi_predict.tmp_shenhy_cityflow_tekuai_tmp1 
stored as parquet as  
select 
days                    
,t1.cityflow                
,case when t2.cityflow is not null then '1' else 0 end as is_air_flow			 
,weight_type_code 
-- ,income_code       
,sum(all_waybill_num)  as tekuai_waybill_num
,sum(all_quantity) as tekuai_quantity         
,sum(weight) as tekuai_weight           
,sum(volume) as tekuai_volume      
,case when t3.province is not null 
                and split(t1.cityflow,'-')[0] rlike '^\\d+$' 
          then t3.province
          else '海外' end as src_province
    ,t3.distribution_name as src_distribution_name
    ,case when t4.province is not null 
                and split(t1.cityflow,'-')[1] rlike '^\\d+$' 
          then t4.province
          else '海外' end as dest_province
    ,t4.distribution_name as dest_distribution_name 
from (select t.* 
    -- 20210401之后
      ,case when product_code in ('SE0001','SE0107','SE0109','SE0146','SE0089','SE0121','SE0137','SE000201','SE0103','SE0051') then '1'
            when product_code='SE0004' and limit_tag = 'SP6' then '1'
            when product_code = 'SE0008' and limit_tag in('T4','T801') then '1'
      else '0'
     end as is_hangkong_waybill 
    -- 20210401之前
   --  ,case when product_code in ('SE0001','SE000201','SE0003','SE0107','SE0109','SE0008','SE0089','SE0121','SE0137','SE0103','SE0051','SE002401') 
   --        then '1'
    --   else '0'
    --  end as is_hangkong_waybill
     -- from dm_ordi_predict.dws_static_his_cityflow_tmp1 t
     from dm_ordi_predict.dws_static_his_cityflow t  -- 后续改调度后用
     ) t1
left join 
      dm_ordi_predict.cf_air_list t2
on t1.cityflow=t2.cityflow 
left join 
    (select city_code,province,distribution_name 
    from dm_ops.dim_city_area_distribution a  
    group by city_code,province,distribution_name
    ) t3
    on nvl(split(t1.cityflow,'-')[0],'a')=t3.city_code
    left join 
      (select city_code,province,distribution_name 
      from dm_ops.dim_city_area_distribution a  
      group by city_code,province,distribution_name
    ) t4
    on nvl(split(t1.cityflow,'-')[1],'a')=t4.city_code
where t1.inc_day between '$[time(yyyyMMdd,-45d)]' and '$[time(yyyyMMdd)]' 
and t1.is_hangkong_waybill='1'
group by days                    
,t1.cityflow   
-- ,income_code             
,case when t2.cityflow is not null then '1' else 0 end			 
,weight_type_code 
,case when t3.province is not null 
                and split(t1.cityflow,'-')[0] rlike '^\\d+$' 
          then t3.province
          else '海外' end 
    ,t3.distribution_name 
    ,case when t4.province is not null 
                and split(t1.cityflow,'-')[1] rlike '^\\d+$' 
          then t4.province
          else '海外' end  
    ,t4.distribution_name;




SET hive.exec.max.dynamic.partitions.pernode =1000;  
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dm_ordi_predict.dws_static_cityflow_tekuai_base partition(inc_day)  
select 
       days                  
,cityflow              
,is_air_flow			
,weight_type_code      
-- ,income_code           
,tekuai_waybill_num    
,tekuai_quantity       
,tekuai_weight         
,tekuai_volume         
,src_province          
,src_distribution_name 
,dest_province         
,dest_distribution_name
,regexp_replace(days,'-','') as inc_day
from tmp_ordi_predict.tmp_shenhy_cityflow_tekuai_tmp1 ;*/