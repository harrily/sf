
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;  -- 非严格模式
set hive.exec.max.dynamic.partitions=2000;  -- 所有执行节点上最多可以多少个动态分区
set hive.exec.max.dynamic.partitions.pernode = 500;

insert overwrite table dm_predict.dws_fc_hk_six_dims_predict_collecting_di partition(partition_key)

select
	 data_version     
    ,feature_version   
    ,model_version     
    ,predict_datetime
	,Origin_code        
	,Dest_code          
	,Origin_name        
	,Dest_name          
	,object_type_code   
	,object_type_name        
	,weight_level  
    ,operation_type     
	,product_type       
	,round(predict_quantity,0) as predict_quantity   
	,predict_waybill    
	,round(predict_weight,0) as  predict_weight  
	,predict_volume     
	,record_time    
    ,src_area_code
    ,src_fbq_code
    ,src_hq_code
    ,dest_area_code 
    ,dest_fbq_code
    ,dest_hq_code
    ,src_province    -- 20221219增加
    ,dest_province    
	,replace(predict_datetime,'-','') as partition_key 
 from tmp_dm_predict.tmp_shenhy_hk_dims_fixed_concat_tmp9;

/*
cREATE TABLE dm_predict.dws_fc_hk_six_dims_predict_collecting_di_test1020(
`data_version` string COMMENT '数据版本',
`feature_version` string COMMENT '特征版本',
`model_version` string COMMENT '模型版本',
`predict_datetime` date COMMENT '日期',
`origin_code` string COMMENT '起始城市代码',
`dest_code` string COMMENT '目的城市代码',
`origin_name` string COMMENT '起始城市名称',
`dest_name` string COMMENT '目的城市名称',
`object_type_code` string COMMENT '对象类型代码（对应为4））',
`object_type` string COMMENT '对象类型（航空流向营运维度）',
`weight_level` string COMMENT '重量段',
`operation_type` string COMMENT '营运维度',
`product_type` string COMMENT '产品类型',
`predict_quantity` double COMMENT '预测件量',
`predict_waybill` double COMMENT '预测票量',
`predict_weight` double COMMENT '预测重量',
`predict_volume` double COMMENT '预测体积',
`record_time` timestamp COMMENT '预测生成时间',
`src_area_code` string COMMENT '始发业务区',
`src_fbq_code` string COMMENT '始发分拨区',
`src_hq_code` string COMMENT '始发大区',
`dest_area_code` string COMMENT '目的业务区',
`dest_fbq_code` string COMMENT '目的分拨区',
`dest_hq_code` string COMMENT '目的大区')
PARTITIONED BY (
`partition_key` string COMMENT '分区日期yyyyMMdd')
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat';*/