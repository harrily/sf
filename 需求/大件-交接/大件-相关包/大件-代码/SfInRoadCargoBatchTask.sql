package com.sf.realtime.spark.batch.main.forecast

import com.sf.realtime.common.utils.DateUtil
import com.sf.realtime.spark.context.Context
import com.sf.realtime.spark.sql.CarInfo
import org.apache.spark.sql.functions.{substring, udf}

import java.util.UUID

object SfInRoadCargoBatchTask {

  def main(args: Array[String]): Unit = {
    val startDay = args(0)
    val endDay = args(1)
    val currentDate = args(2) //业务时间  $[time(yyyy-MM-dd HH:mm:ss)]
    val incDay = DateUtil.df2Todf3(currentDate)
    val incHour = currentDate.substring(11, 13).concat(currentDate.substring(14, 16))
    //可回刷使用lastUpdateTm进行控制
    val last4Day = DateUtil.getLastDayStringdf3(DateUtil.df2Todf3(currentDate), 4)
    val next4Day = DateUtil.getLastDayStringdf3(DateUtil.df2Todf3(currentDate), -5)
    val spark = Context.getContext(true)

    //读取tidb数据
    val inRoadDatil = CarInfo.getVehicleInRoadBatchTaskInfo(spark.sqlContext, DateUtil.getLastTimeStamp(currentDate.substring(0, 10).concat(" 00:00:00"), 3), 8)
    inRoadDatil.createOrReplaceTempView("in_road_datil")
    spark.sqlContext.sql(" select requireId,carNo,transLevel,carStatus,srcZoneCode,from_unixtime(int(preArriveTm/1000), 'yyyy-MM-dd HH:mm:ss') preArriveTm,preArriveZoneCode,tickets,weight,status,'"+currentDate+"' as countTime,countDate,from_unixtime(int(preArriveTm/1000), 'yyyy-MM-dd HH:mm') arrTm from in_road_datil")
      .createOrReplaceTempView("in_road_datil_noBatch")
    //读取班次信息
    val batchInfo = spark.sqlContext.sql(""" select operate_zone_code,batch_code,batch_date, last_last_arrive_tm, last_arrive_tm from ky.dm_heavy_cargo.dm_arrive_batch_info_dtl_di  where inc_day between '"""+last4Day+"""' and '"""+next4Day+"""' """)
    batchInfo.createOrReplaceTempView("batch_info")


    val resultBatch = spark.sqlContext.sql(
      """ select
        |        requireId as require_id,carNo as car_no,transLevel as trans_level,carStatus as car_status,srcZoneCode as src_zone_code,preArriveTm as pre_arrive_tm,
        |        preArriveZoneCode as pre_arrive_zone_code,tickets,weight,status,countTime as count_time,countDate as count_date,
        |        batch_code,
        |        if(batch_date is not null,date_format(batch_date, 'yyyyMMdd'),batch_date)  as batch_date
        | from
        |    (select
        |            requireId,carNo,transLevel,carStatus,srcZoneCode,preArriveTm,preArriveZoneCode,tickets,weight,status,countTime,countDate,
        |            max(batch_code) over(partition by requireId,srcZoneCode,preArriveZoneCode) batch_code,
        |            max(batch_date) over(partition by requireId,srcZoneCode,preArriveZoneCode) batch_date,
        |            row_number() over(partition by requireId,srcZoneCode,preArriveZoneCode order by preArriveTm) rn
        |    from
        |        (select
        |                requireId,carNo,transLevel,carStatus,srcZoneCode,preArriveTm,preArriveZoneCode,tickets,weight,status,countTime,countDate,arrTm,
        |                if((b.arrTm>=a.last_last_arrive_tm and b.arrTm<a.last_arrive_tm),batch_code,null) batch_code,
        |                if((b.arrTm>=a.last_last_arrive_tm and b.arrTm<a.last_arrive_tm),batch_date,null) batch_date
        |                from in_road_datil_noBatch b left join batch_info a
        |                on b.preArriveZoneCode=a.operate_zone_code
        |        )t1
        |    )t2
        | where rn=1""".stripMargin)


    resultBatch.repartition(1).createOrReplaceTempView("resultAll")
    //    CacheUtil.insertTMonitorDataRows("t_monitor_in_road_cargo_batch",new Timestamp(System.currentTimeMillis()),count)
    spark.sqlContext.sql("set hive.merge.sparkFiles=true;")
    spark.sqlContext.sql("set mapred.max.split.size=268435456;")
    spark.sqlContext.sql("set mapred.min.split.size.per.node=268435456;")
    spark.sqlContext.sql("set mapred.min.split.size.per.rack=268435456;")
    spark.sqlContext.sql("set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;")
    spark.sqlContext.sql("set hive.exec.reducers.bytes.per.reducer=268435456;")
    spark.sqlContext.sql("set hive.exec.reducers.max=1099;")
    spark.sqlContext.sql("set hive.merge.size.per.task=268435456;")
    spark.sqlContext.sql("set hive.merge.smallfiles.avgsize=134217728;")
    spark.sqlContext.sql(
      """ insert overwrite table ky.dm_heavy_cargo.dm_cargo_quantity_prediction_dtl_rt
        | partition(inc_day='""".stripMargin+ incDay +"""', inc_hour='"""+ incHour +
        """')
          | select require_id,car_no,trans_level,car_status,src_zone_code,pre_arrive_tm,pre_arrive_zone_code,tickets,weight,status,count_time,count_date,batch_code,batch_date
          | from resultAll""".stripMargin)
  }

}
