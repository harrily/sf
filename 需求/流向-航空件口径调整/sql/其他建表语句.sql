CREATE TABLE `dm_predict.dws_city_province_qty_di_backup20230620`( `days` string COMMENT '日期', `provinceflow` string COMMENT '流向', `quantity_num` bigint COMMENT '省维度累计件量', `update_time` string COMMENT '数据更新时间') PARTITIONED BY ( `inc_day` string COMMENT '分区日期yyyyMMdd') ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'


CREATE TABLE `dm_predict.dws_city_province_weight_qty_di_backup20230620`( `days` string COMMENT '日期', `provinceflow` string COMMENT '流向', `income_code` string COMMENT '产品板块', `weight_type_code` string COMMENT '重量段', `waybill_num` bigint COMMENT '累计票量', `update_time` string COMMENT '数据更新时间') PARTITIONED BY ( `inc_day` string COMMENT '分区日期yyyyMMdd') ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'



CREATE TABLE `dm_predict.air_flow_two_dims_month_backup20230620`( `ds` string COMMENT '收件日期', `id` string COMMENT '流向对', `is_air_flow` string COMMENT '是否空网, 1-空网, 0-非空网', `operation_type` string COMMENT '维度', `quantity` int COMMENT '航空月度件量', `weight` double COMMENT '航空月度重量') PARTITIONED BY ( `inc_month` string COMMENT '预测日期yyyymmdd') ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'



---    航空动态 加业务区  ， 去掉 城市对相关
CREATE TABLE `dm_ordi_predict.dws_air_flow_dynamic_pro_qty_hi_backup20230713`(
  `days` string COMMENT '收件日期',
  `area_code` string COMMENT '业务区编码',
  `area_name` string COMMENT '业务区名称',
  `product_code` string COMMENT '产品代码',
  `weight_type_code` string COMMENT '重量段',
  `all_mainwaybill_num` double COMMENT '总票数',
  `all_quantity_num` double COMMENT '总件量',
  `air_mainwaybill_num` double COMMENT '航空总票数',
  `air_quantity_num` double COMMENT '航空总件量',
  `all_wgt_num` double COMMENT '总重量',
  `air_wgt_num` double COMMENT '航空重量'
) PARTITIONED BY (
  `inc_day` string COMMENT '收件日期yyyymmdd',
  `inc_dayhour` string COMMENT '打点小时yyyymmddHH'
) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' ;

CREATE TABLE `dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi_backup20230713`(
  `days` string COMMENT '收件日期',
  `area_code` string COMMENT '业务区编码',
  `area_name` string COMMENT '业务区名称',
  `income_code` string COMMENT '收入版块',
  `weight_type_code` string COMMENT '重量段',
  `all_mainwaybill_num` double COMMENT '总票数',
  `all_quantity_num` double COMMENT '总件量',
  `air_mainwaybill_num` double COMMENT '航空总票数',
  `air_quantity_num` double COMMENT '航空总件量',
  `all_wgt_num` double COMMENT '总重量',
  `air_wgt_num` double COMMENT '航空重量'
) PARTITIONED BY (
  `inc_day` string COMMENT '收件日期yyyymmdd',
  `inc_dayhour` string COMMENT '打点小时yyyymmddHH'
) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' ;

-- oms 加业务区   ， 去掉 城市对相关
CREATE TABLE `dm_ordi_predict.dws_cityflow_dynamic_order_hi_backup20230713`(
  `order_dt` string COMMENT '收件日期',
  `area_code` string COMMENT '业务区编码',
  `area_name` string COMMENT '业务区名称',
  `all_order_num` double COMMENT '总订单量',
  `air_order_num` double COMMENT '航空订单量'
) PARTITIONED BY (
  `inc_day` string COMMENT '下单日期yyyymmdd',
  `inc_dayhour` string COMMENT '打点小时yyyymmddHH'
) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'




---    航空动态 加业务区  ， 
CREATE TABLE `dm_ordi_predict.dws_air_flow_dynamic_pro_qty_hi_backup20230714`(
  `days` string COMMENT '收件日期',
  `city_flow` string COMMENT '城市流向对',
  `src_city_code` string COMMENT '收件城市',
  `dest_city_code` string COMMENT '派件城市',
  `area_code` string COMMENT '业务区编码',
  `area_name` string COMMENT '业务区名称',
  `product_code` string COMMENT '产品代码',
  `weight_type_code` string COMMENT '重量段',
  `all_mainwaybill_num` double COMMENT '总票数',
  `all_quantity_num` double COMMENT '总件量',
  `air_mainwaybill_num` double COMMENT '航空总票数',
  `air_quantity_num` double COMMENT '航空总件量',
  `all_wgt_num` double COMMENT '总重量',
  `air_wgt_num` double COMMENT '航空重量'
) PARTITIONED BY (
  `inc_day` string COMMENT '收件日期yyyymmdd',
  `inc_dayhour` string COMMENT '打点小时yyyymmddHH'
) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' ;

CREATE TABLE `dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi_backup20230714`(
  `days` string COMMENT '收件日期',
  `city_flow` string COMMENT '城市流向对',
  `src_city_code` string COMMENT '收件城市',
  `dest_city_code` string COMMENT '派件城市',
  `area_code` string COMMENT '业务区编码',
  `area_name` string COMMENT '业务区名称',
  `income_code` string COMMENT '收入版块',
  `weight_type_code` string COMMENT '重量段',
  `all_mainwaybill_num` double COMMENT '总票数',
  `all_quantity_num` double COMMENT '总件量',
  `air_mainwaybill_num` double COMMENT '航空总票数',
  `air_quantity_num` double COMMENT '航空总件量',
  `all_wgt_num` double COMMENT '总重量',
  `air_wgt_num` double COMMENT '航空重量'
) PARTITIONED BY (
  `inc_day` string COMMENT '收件日期yyyymmdd',
  `inc_dayhour` string COMMENT '打点小时yyyymmddHH'
) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' ;



-- oms 加业务区   ， 去掉 城市对相关
CREATE TABLE `dm_ordi_predict.dws_cityflow_dynamic_order_hi_backup20230714`(
  `order_dt` string COMMENT '收件日期',
  `city_flow` string COMMENT '城市流向对',
  `area_code` string COMMENT '业务区编码',
  `area_name` string COMMENT '业务区名称',
  `all_order_num` double COMMENT '总订单量',
  `air_order_num` double COMMENT '航空订单量'
) PARTITIONED BY (
  `inc_day` string COMMENT '下单日期yyyymmdd',
  `inc_dayhour` string COMMENT '打点小时yyyymmddHH'
) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' ;