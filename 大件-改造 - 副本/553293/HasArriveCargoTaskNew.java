package com.sf.realtime.spark.batch.main.forecast

import com.alibaba.fastjson.JSON

import java.util
import com.sf.realtime.common.utils.{DateUtil, MD5Util}
import com.sf.realtime.hbase.HbaseUtil
import com.sf.realtime.hbase.common.ColumnType
import com.sf.realtime.spark.context.Context
import com.sf.realtime.spark.sql.DeptInfo
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.util.UUID
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
object HasArriveCargoTaskNew {

  def main(args: Array[String]): Unit = {
    val spark = Context.getContext(true)
    val startDay = args(0)
    val endDay = args(1)
    val transitDf = DeptInfo.getKyNewDeptInfo(spark.sqlContext)
    transitDf.createOrReplaceTempView("transit_info_tmp")

    val sql =
          """select * from (select
        | case when idKey = 'null' then null else idKey end as idKey,
        | case when requireId = 'null' then null else requireId end as requireId,
        | case when translevel = 'null' then null else cast(translevel as int) end as translevel,
        | case when carNo = 'null' then null else carNo end as carNo,
        | case when lineCode = 'null' then null else lineCode end as lineCode,
        | case when carStatus = 'null' then null else cast(carStatus as int) end as carStatus,
        | case when lastUpdateTm = 'null' then null else cast(lastUpdateTm as bigint) end as lastUpdateTm,
        | case when srcZoneCode = 'null' then null else srcZoneCode end as srcZoneCode,
        | case when srcPlanReachTm = 'null' then null else cast(srcPlanReachTm as bigint) end as srcPlanReachTm,
        | case when srcPlanDepartTm = 'null' then null else cast(srcPlanDepartTm as bigint) end as srcPlanDepartTm,
        | case when srcActualDepartTm = 'null' then null else cast(srcActualDepartTm as bigint) end as srcActualDepartTm,
        | case when srcPreDepartTm = 'null' then null else cast(srcPreDepartTm as bigint) end as srcPreDepartTm,
        | case when srcPlanArriveTm = 'null' then null else cast(srcPlanArriveTm as bigint) end as srcPlanArriveTm,
        | case when srcActualArriveTm = 'null' then null else cast(srcActualArriveTm as bigint) end as srcActualArriveTm,
        | case when srcPreArriveTm = 'null' then null else cast(srcPreArriveTm as bigint) end as srcPreArriveTm,
        | case when secondZoneCode = 'null' then null else secondZoneCode end as secondZoneCode,
        | case when secondPlanReachTm = 'null' then null else cast(secondPlanReachTm as bigint) end as secondPlanReachTm,
        | case when secondPlanDepartTm = 'null' then null else cast(secondPlanDepartTm as bigint) end as secondPlanDepartTm,
        | case when secondActualDepartTm = 'null' then null else cast(secondActualDepartTm as bigint) end as secondActualDepartTm,
        | case when secondPreDepartTm = 'null' then null else cast(secondPreDepartTm as bigint) end as secondPreDepartTm,
        | case when secondPlanArriveTm = 'null' then null else cast(secondPlanArriveTm as bigint) end as secondPlanArriveTm,
        | case when secondActualArriveTm = 'null' then null else cast(secondActualArriveTm as bigint) end as secondActualArriveTm,
        | case when secondPreArriveTm = 'null' then null else cast(secondPreArriveTm as bigint) end as secondPreArriveTm,
        | case when thirdZoneCode = 'null' then null else thirdZoneCode end as thirdZoneCode,
        | case when thirdPlanReachTm = 'null' then null else cast(thirdPlanReachTm as bigint) end as thirdPlanReachTm,
        | case when thirdPlanDepartTm = 'null' then null else cast(thirdPlanDepartTm as bigint) end as thirdPlanDepartTm,
        | case when thirdActualDepartTm = 'null' then null else cast(thirdActualDepartTm as bigint) end as thirdActualDepartTm,
        | case when thirdPreDepartTm = 'null' then null else cast(thirdPreDepartTm as bigint) end as thirdPreDepartTm,
        | case when thirdPlanArriveTm = 'null' then null else cast(thirdPlanArriveTm as bigint) end as thirdPlanArriveTm,
        | case when thirdActualArriveTm = 'null' then null else cast(thirdActualArriveTm as bigint) end as thirdActualArriveTm,
        | case when thirdPreArriveTm = 'null' then null else cast(thirdPreArriveTm as bigint) end as thirdPreArriveTm,
        | case when destZoneCode = 'null' then null else destZoneCode end as destZoneCode,
        | case when destPlanReachTm = 'null' then null else cast(destPlanReachTm as bigint) end as destPlanReachTm,
        | case when destPlanDepartTm = 'null' then null else cast(destPlanDepartTm as bigint) end as destPlanDepartTm,
        | case when destActualDepartTm = 'null' then null else cast(destActualDepartTm as bigint) end as destActualDepartTm,
        | case when destPreDepartTm = 'null' then null else cast(destPreDepartTm as bigint) end as destPreDepartTm,
        | case when destPlanArriveTm = 'null' then null else cast(destPlanArriveTm as bigint) end as destPlanArriveTm,
        | case when destActualArriveTm = 'null' then null else cast(destActualArriveTm as bigint) end as destActualArriveTm,
        | case when destPreArriveTm = 'null' then null else cast(destPreArriveTm as bigint) end as destPreArriveTm,
        | case when insertTime = 'null' then null else cast(insertTime as timestamp) end as insertTime,
        | case when fullLoadWeight = 'null' then null else cast(fullLoadWeight as double) end as fullLoadWeight,
        | case when srcLoadContnrNos = 'null' then null else srcLoadContnrNos end as srcLoadContnrNos,
        | case when srcArriveContnrNos = 'null' then null else srcArriveContnrNos end as srcArriveContnrNos,
        | case when srcUnloadContnrNos = 'null' then null else srcUnloadContnrNos end as srcUnloadContnrNos,
        | case when secondLoadContnrNos = 'null' then null else secondLoadContnrNos end as secondLoadContnrNos,
        | case when secondArriveContnrNos = 'null' then null else secondArriveContnrNos end as secondArriveContnrNos,
        | case when secondUnloadContnrNos = 'null' then null else secondUnloadContnrNos end as secondUnloadContnrNos,
        | case when thirdLoadContnrNos = 'null' then null else thirdLoadContnrNos end as thirdLoadContnrNos,
        | case when thirdArriveContnrNos = 'null' then null else thirdArriveContnrNos end as thirdArriveContnrNos,
        | case when thirdUnloadContnrNos = 'null' then null else thirdUnloadContnrNos end as thirdUnloadContnrNos,
        | case when destLoadContnrNos = 'null' then null else destLoadContnrNos end as destLoadContnrNos,
        | case when destArriveContnrNos = 'null' then null else destArriveContnrNos end as destArriveContnrNos,
        | case when destUnloadContnrNos = 'null' then null else destUnloadContnrNos end as destUnloadContnrNos,
        | case when srcJobType = 'null' then null else srcJobType end as srcJobType,
        | case when secondJobType = 'null' then null else secondJobType end as secondJobType,
        | case when thirdJobType = 'null' then null else thirdJobType end as thirdJobType,
        | case when destJobType = 'null' then null else destJobType end as destJobType,
        | case when nextzonecodedynamicprearrivetime = 'null' then null else nextzonecodedynamicprearrivetime end as nextzonecodedynamicprearrivetime,
        | inc_day,
        |row_number() over(partition by requireId order by lastUpdateTm desc) rn from dm_heavy_cargo.rt_vehicle_task_monitor_for_not_send_detail4 a where inc_day between '""".stripMargin+startDay+"""' and '"""+endDay+"""' and (secondActualArriveTm >0 or thirdActualArriveTm>0 or destActualArriveTm >0))aa where aa.rn=1 and aa.carStatus in (3,4,5,6)"""

    val requireDetail = spark.sqlContext.sql(sql).drop("rn")
    requireDetail.createOrReplaceTempView("require_detail")
    val hasArrive1 = spark.sqlContext.sql(
      """select aa.*, adTable.car_no as car_no
        |from (select a.requireId as require_id,a.translevel,a.srcZoneCode as src_zone_code,a.srcActualDepartTm as send_time,a.secondZoneCode as dest_zone_code,
        |a.secondActualArriveTm as arrive_time,a.secondUnloadContnrnos as car_nos,a.carStatus as car_status
        |from require_detail a
        |join transit_info_tmp b
        |on a.secondZoneCode = b.deptCode
        |where a.secondZoneCode is not null and a.secondActualArriveTm > 0 and a.secondUnloadContnrnos is not null)aa LATERAL VIEW explode(split(car_nos, ',')) adTable AS car_no""".stripMargin)
    val hasArrive2 = spark.sqlContext.sql(
      """select aa.*, adTable.car_no as car_no
        |from (select a.requireId as require_id,a.translevel,a.secondZoneCode as src_zone_code,a.secondActualDepartTm as send_time,a.thirdZoneCode as dest_zone_code,
        |a.thirdActualArriveTm as arrive_time,a.thirdUnloadContnrnos as car_nos,a.carStatus as car_status
        |from require_detail a
        |join transit_info_tmp b on a.thirdZoneCode = b.deptCode
        |where a.thirdZoneCode is not null and a.thirdActualArriveTm > 0 and a.thirdUnloadContnrnos is not null)aa LATERAL VIEW explode(split(car_nos, ',')) adTable AS car_no""".stripMargin)
    val hasArrive3 = spark.sqlContext.sql(
      """select aa.*, adTable.car_no as car_no
        |from (select a.requireId as require_id,a.translevel,a.srcZoneCode as src_zone_code,a.srcActualDepartTm as send_time,a.destZoneCode as dest_zone_code,
        |a.destActualArriveTm as arrive_time,
        |case when a.destUnloadContnrnos is not null then a.destUnloadContnrnos when a.destArriveContnrnos is not null then a.destArriveContnrnos else a.carNo end as car_nos,a.carStatus as car_status
        |from require_detail a join transit_info_tmp b on a.destZoneCode = b.deptCode
        |where a.destZoneCode is not null and a.destActualArriveTm > 0 and a.secondZoneCode is null)aa LATERAL VIEW explode(split(car_nos, ',')) adTable AS car_no""".stripMargin)
    val hasArrive4 = spark.sqlContext.sql("""select aa.*, adTable.car_no as car_no from (select a.requireId as require_id,a.translevel,a.secondZoneCode as src_zone_code,a.secondActualDepartTm as send_time,a.destZoneCode as dest_zone_code,a.destActualArriveTm as arrive_time,case when a.destUnloadContnrnos is not null then a.destUnloadContnrnos  when a.destArriveContnrnos is not null then a.destArriveContnrnos else a.carNo end as car_nos,a.carStatus as car_status from require_detail a join transit_info_tmp b on a.destZoneCode = b.deptCode where a.destZoneCode is not null and a.destActualArriveTm > 0 and a.secondZoneCode is not null and a.thirdZoneCode is null)aa LATERAL VIEW explode(split(car_nos, ',')) adTable AS car_no""")
    val hasArrive5 = spark.sqlContext.sql("""select aa.*, adTable.car_no as car_no from (select a.requireId as require_id,a.translevel,a.thirdZoneCode as src_zone_code,a.thirdActualDepartTm as send_time,a.destZoneCode as dest_zone_code,a.destActualArriveTm as arrive_time,case when a.destUnloadContnrnos is not null then a.destUnloadContnrnos  when a.destArriveContnrnos is not null then a.destArriveContnrnos else a.carNo end as car_nos,a.carStatus as car_status from require_detail a join transit_info_tmp b on a.destZoneCode = b.deptCode where a.destZoneCode is not null and a.destActualArriveTm > 0 and a.thirdZoneCode is not null )aa LATERAL VIEW explode(split(car_nos, ',')) adTable AS car_no""")
    val allArrive = hasArrive1.union(hasArrive2).union(hasArrive3).union(hasArrive4).union(hasArrive5)

    val requireCarNoDetail = allArrive.rdd.filter(r=>r.getAs[String]("car_no")!= null && !r.getAs[String]("car_no").equals("") ).mapPartitions(f=>{
      val hbase = HbaseUtil.getInstance()
      f.map(r=>{
        val requireId = r.getAs[String]("require_id")
        val translevel = r.getAs[Integer]("translevel")
        val carNo = r.getAs[String]("car_no")
        val srcZoneCode = r.getAs[String]("src_zone_code")
        val destZoneCode = r.getAs[String]("dest_zone_code")
        val arriveTime = r.getAs[Long]("arrive_time")
        val sendTime = r.getAs[Long]("send_time")
        val carStatus = r.getAs[Integer]("car_status")
        val rowKey = carNo.reverse
        val rtContrnRes = hbase.getRow("rt_container_waybill_relation", Bytes.toBytes(rowKey))
        var listRow = new ArrayBuffer[Row]()
        if (rtContrnRes != null && !rtContrnRes.isEmpty) {
          val cells = rtContrnRes.listCells().asScala
          val getList = new util.ArrayList[String]()
          var columns = new util.HashMap[String,ColumnType]()
          for(cell <- cells){
            val waybillNo = Bytes.toString(CellUtil.cloneQualifier(cell))
            val waybillJson = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(cell)))
            val jobType = waybillJson.getString("jobType")
            if (StringUtils.isNotEmpty(waybillNo)&& waybillJson.getInteger("isDeleted") == 0){
              //val get = new Get(Bytes.toBytes(waybillNo.reverse))
              //get.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("packageMeterageWeightQty"))
              getList.add(waybillNo)
              columns.put("packageMeterageWeightQty",ColumnType.DOUBLE)
            }
          }
          //val result = hbase.getList("wb_info_data",getList.toList.asJava)
          val hrs = hbase.getListSpecialForWbInfo(getList, columns);
          for(waybillResult <- hrs.entrySet().asScala; if hrs!=null && !hrs.isEmpty) {
            var weight = 0D
            var ticket = 0L
            var waybillNo = ""
            if (waybillResult != null) {
              waybillNo = waybillResult.getKey
              val wCells = waybillResult.getValue
              for (wCell <- wCells.entrySet().asScala) {
                weight = wCell.getValue.asInstanceOf[Double]
                if (weight > 1000000D) {
                  weight = 1000000D
                }
                ticket = 1L
              }
            }
            listRow += Row(requireId, carNo, srcZoneCode, destZoneCode, sendTime, arriveTime, ticket, weight, waybillNo, translevel.toString, carStatus.toString)
           }
          }else{
              listRow += Row(requireId,carNo,srcZoneCode,destZoneCode,sendTime,arriveTime,0L,0D,null,translevel.toString,carStatus.toString)
          }
          listRow.toArray
        }).flatMap(r=>r.iterator)
      })
    val structFields = Array(StructField("require_id", StringType, true),
      StructField("car_no", StringType, true),
      StructField("src_zone_code", StringType, true),
      StructField("dest_zone_code", StringType, true),
      StructField("send_time", LongType, true),
      StructField("arrive_time", LongType, true),
      StructField("tickets", LongType, true),
      StructField("weight", DoubleType, true),
      StructField("waybill_no", StringType, true),
      StructField("translevel", StringType, true),
      StructField("car_status", StringType, true)
    )
    val id = UUID.randomUUID.toString
    val structType = StructType(structFields)
    val requireCarNoDetailDf = spark.createDataFrame(requireCarNoDetail, structType).dropDuplicates("waybill_no","dest_zone_code")
    val resultPre = requireCarNoDetailDf.groupBy("require_id","car_no","translevel","src_zone_code","dest_zone_code","send_time","arrive_time","car_status").agg(sum("tickets").as("tickets"),sum("weight").as("weight"))
    resultPre.createOrReplaceTempView("result_pre")
    val result = spark.sqlContext.sql("""select '"""+id+"""' as id, a.require_id,a.car_no,a.src_zone_code,a.dest_zone_code,a.send_time,a.arrive_time,a.tickets,a.weight,'"""+DateUtil.getDateString(0)+"""' as create_time,a.translevel,a.car_status from result_pre a """)
    spark.sqlContext.sql("set hive.merge.sparkFiles=true;")
    spark.sqlContext.sql("set mapred.max.split.size=268435456;")
    spark.sqlContext.sql("set mapred.min.split.size.per.node=268435456;")
    spark.sqlContext.sql("set mapred.min.split.size.per.rack=268435456;")
    spark.sqlContext.sql("set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;")
    spark.sqlContext.sql("set hive.exec.reducers.bytes.per.reducer=268435456;")
    spark.sqlContext.sql("set hive.exec.reducers.max=1099;")
    spark.sqlContext.sql("set hive.merge.size.per.task=268435456;")
    spark.sqlContext.sql("set hive.merge.smallfiles.avgsize=134217728;")
    result.repartition(1).createOrReplaceTempView("result")
    spark.sqlContext.sql("""insert overwrite table dm_heavy_cargo.ky_has_arrive_cargo_rb partition(inc_day='"""+endDay+"""') select * from result""")
  }
}
