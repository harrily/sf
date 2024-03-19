----------------------开发说明----------------------------
--* 名称:【收派-动态收件】动态收件流向数据打点表
--* 任务ID:468144
--* 说明:动态收派底表（流向/城市）
--* 作者: 80006694
--* 时间: 2022/8/11 10:45
----------------------修改记录----------------------------
--* 修改人   修改时间      修改内容
--* 80006694    20220906    itp_vs_rel_city_bg_area_mpp切成正式表
----------------------------------------------------------
----------------------hive调优参数列表----------------------
set hive.execution.engine=mr;
-- container申请最大次数
set tez.am.max.app.attempts=5;
-- container资源管理
--set hive.tez.container.size=8192;
--set hive.tez.java.opts=-Xmx6553m;
-- set tez.queue.name=predict;   -- 平台禁止sql申请队列，在资源队列申请
-- 设置mapper输入文件合并的参数
--set mapred.max.split.size=256000000;  --每个Map最大输入大小
--set mapred.min.split.size.per.node=100000000; --一个节点上split的至少的大小(这个值决定了多个DataNode上的文件是否需要合并)
--set mapred.min.split.size.per.rack=100000000; --一个交换机下split的至少的大小(这个值决定了该机架下的文件是否需要合并)
--set hive.input.format=org.apache.Hadoop.hive.ql.io.CombineHiveInputFormat;  -- 执行Map前进行小文件合并
-- 设置 map输出和reduce输出进行合并的参数
-- set hive.merge.mapfiles= true    --设置 map输出和reduce输出进行合并的相关参数
-- set hive.merge.mapredfiles= true   --设置reduce端输出进行合并，默认为false
-- set hive.merge.size.per.task= 256 *1000 * 1000  --设置合并文件的大小
-- set hive.merge.smallfiles.avgsize=16000000  -- 输出文件的平均大小小于该值时，启动一个独立的MapReduce任务进行文件merge

-- 动态分区
-- set hive.exec.dynamic.partition.mode=nonstrict;
-- reduce的每个分区内排序 会大大降低插入速度
-- set hive.optimize.sort.dynamic.partition=true;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;
set mapred.reduce.tasks= -1;
-- mapjoin -- 慎用,tez引擎下 with as 结构 或者两个子查询join可能存在不可预知的bug
set hive.auto.convert.join = true;
set hive.groupby.skewindata=true;   -- 自动负载均衡，两个mr的job
set hive.map.aggr = true;    -- map段预聚合
-- 谓词下推
set hive.optimize.ppd = true;
-- 启用相关性优化器
set hive.optimize.correlation = true;
-- 使用向量化查询
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
-- 并行
set hive.exec.parallel=true;

----------------------脚本逻辑----------------------------
--输入表：dm_oewm.oewm_receive_flow_ez,dm_ordi_predict.itp_vs_rel_city_bg_area_mpp
--输出表：dm_ordi_predict.dws_dynamic_cityflow_base_hi
--简单描述脚本逻辑:小时获取收件流向数据
----------------------------------------------------------

/* 建表语句
create table `dm_ordi_predict.dws_dynamic_cityflow_base_hi`(
 `cityflow` string COMMENT '城市对',
 `consignor_date` string COMMENT '查询时间',
 `deliver_hq_code` string COMMENT '目的地大区名称',
 `deliver_area_code` string COMMENT '目的地地区名称',
 `deliver_fbq_code` string COMMENT '目的地分拨区名称',
 `deliver_city_code` string COMMENT '目的地城市名称',
 `pickup_hq_code` string COMMENT '寄件大区名称',
 `pickup_area_code` string COMMENT '寄件地区名称',
 `pickup_fbq_code` string COMMENT '寄件分拨区名称',
 `pickup_city_code` string COMMENT '寄件城市名称',
 `pickup_weight` double COMMENT '重量',
 `pickup_weight_20kg` double COMMENT '大于20KG重量',
 `pickup_piece` double COMMENT '票数',
 `pickup_piece_20kg` double COMMENT '大于20KG重量票数',
 `product_type_code` string COMMENT '产品代码',
 `deliver_dept_code` string COMMENT '目的地网点代码',
 `pickup_division_code` string COMMENT '寄件分部代码',
 `pickup_dept_code` string COMMENT '寄件网点代码',
 `deliver_division_code` string COMMENT '目的地分部代码')
COMMENT '动态收件流向数据打点表'
PARTITIONED BY (
 `inc_day` string,
 `hour` string)
ROW FORMAT SERDE
 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
 ;
*/

insert overwrite table dm_ordi_predict.dws_dynamic_cityflow_base_hi partition(inc_day='${YYYYMMDD}',hour='${H}')
select concat(nvl(a.pickup_city_code,'#'),'-',nvl(a.deliver_city_code,'#')) as cityflow
     , a.consignor_date
     , a.deliver_hq_code
     , nvl(a.deliver_area_code_jg,a.deliver_area_code) as deliver_area_code
     , a.deliver_fbq_code
     , a.deliver_city_code
     , a.pickup_hq_code
     , nvl(a.pickup_area_code_jg,a.pickup_area_code) as pickup_area_code
     , a.pickup_fbq_code
     , a.pickup_city_code
     , a.pickup_weight
     , a.pickup_weight_20kg
     , a.pickup_piece
     , a.pickup_piece_20kg
     , a.product_type_code
     , a.deliver_dept_code
     , a.pickup_division_code
     , a.pickup_dept_code
     , a.deliver_division_code
  from (
        select a.*
             , case when a.pickup_hq_code in ('CN06','CN07') then a.pickup_hq_code
                    when a.pickup_hq_code in ('CN39','HQOP') then a.pickup_area_code
                    when b.area_code is not null then b.area_code
                    when b.area_code is null then a.pickup_area_code
                    else 'other' end as pickup_area_code_jg
             , case when a.deliver_hq_code in ('CN06','CN07') then a.deliver_hq_code
                    when a.deliver_hq_code in ('CN39','HQOP') then a.deliver_area_code
                    when c.area_code is not null then c.area_code
                    when c.area_code is null then a.deliver_area_code
                    else 'other' end as deliver_area_code_jg
        --   from dm_oewm.oewm_receive_flow_ez a
        --  剔除丰网数据 20230612修改
        from (select * from dm_oewm.oewm_receive_flow_ez
                where (deliver_hq_code <>'CN39' or deliver_hq_code is null)
                and (pickup_hq_code <>'CN39' or pickup_hq_code is null)
                and (nvl(deliver_dept_code,'') in (select dept_code from dim.dim_department where hq_code<>'CN39' and dept_code is not null group by dept_code ) or deliver_dept_code is null )
                and (nvl(pickup_dept_code,'') in (select dept_code from dim.dim_department where hq_code<>'CN39' and dept_code is not null group by dept_code ) or pickup_dept_code is null )
                ) a 
          left join (
            select city_code,area_code
              from dm_ordi_predict.itp_vs_rel_city_bg_area_mpp
             where BG_code='SF001' and to_tm='9999-12-31' and area_code not in ('852Y','886Y') and hq_version = 'YJ'
            )b
            on a.pickup_city_code = b.city_code
          left join (
            select city_code,area_code
              from dm_ordi_predict.itp_vs_rel_city_bg_area_mpp
             where BG_code='SF001' and to_tm='9999-12-31' and area_code not in ('852Y','886Y') and hq_version = 'YJ'
            )c
            on a.deliver_city_code = c.city_code
          where inc_day='${YYYYMMDD}' and hour='${H}'
        )a
   ;