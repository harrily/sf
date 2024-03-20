package com.sf.realtime.spark.batch.main.sf

import com.alibaba.fastjson.JSON
import com.sf.realtime.hbase.HbaseUtil
import com.sf.realtime.hbase.common.ColumnType
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * @Author 01419728
 * @Date 2022/6/15 18:03
 */
object KyHasArriveCargoYZByHbase2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("")
    val spark =  SparkSession.builder()
      .config(conf)
      .enableHiveSupport() //enableHiveSupport 操作hive表
      .getOrCreate()
//    val incday = args(0)
//    val incHour = args(1)
    val beginTime=args(0)
    val endTime=args(1)
    println("beginTime:"+beginTime)
    println("endTime:"+endTime)
//    val hasArriveTaskDf = CarInfo.getVehicleHasArriveTaskInfo(spark.sqlContext,DateUtil.getLastHoursTime(24),1)
    // 1根据参数查询最新分区 2过滤actualTimeT-1D
    // 更新版本，1直接查询全表，2过滤actualTimeT-1D
//    val hasArriveTaskDf = spark.sqlContext.sql(""" SELECT idKey ,requireId ,carNo,transLevel,carStatus,srcZoneCode,destZoneCode,actualTime FROM tmp_ordi_predict.dmpdsp_vt_has_arrive_cars_hf where inc_day = '"""+ incday + """' and inc_hour='"""+ incHour + """' and actualTime > unix_timestamp('"""+ beginTime + """') * 1000	and actualTime <= unix_timestamp('"""+ endTime + """') * 1000  """.stripMargin)
    val hasArriveTaskDf = spark.sqlContext.sql(""" SELECT idKey ,requireId ,carNo,transLevel,carStatus,srcZoneCode,destZoneCode,actualTime FROM tmp_ordi_predict.dmpdsp_vt_has_arrive_cars where  actualTime > unix_timestamp('"""+ beginTime + """') * 1000	and actualTime <= unix_timestamp('"""+ endTime + """') * 1000  """.stripMargin)
    val f1 = hasArriveTaskDf.rdd.mapPartitions(f => {
      val hbase = HbaseUtil.getInstance()
      f.map(r => {
        val idKey = r.getAs[String]("idKey")
        val requireId = r.getAs[String]("requireId")
        val carNo = r.getAs[String]("carNo")
        val translevel = r.getAs[Integer]("transLevel")
        val carStatus = r.getAs[Integer]("carStatus")
        val srcZoneCode = r.getAs[String]("srcZoneCode")
        val destZoneCode = r.getAs[String]("destZoneCode")
        val actualTime = r.getAs[Long]("actualTime")
        val rowKey = r.getAs[String]("carNo").reverse
        val rtContrnRes = hbase.getRow("rt_container_waybill_relation", Bytes.toBytes(rowKey))
        var listRow = new ArrayBuffer[Row]()
        if (rtContrnRes != null && !rtContrnRes.isEmpty) {
          val cells = rtContrnRes.listCells().asScala
          val getList = new util.ArrayList[String]()
          var columns = new util.HashMap[String, ColumnType]()
          for (cell <- cells) {
            val waybillNo = Bytes.toString(CellUtil.cloneQualifier(cell))
            val waybillJson = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(cell)))
            val jobType = waybillJson.getString("jobType")
            if (StringUtils.isNotEmpty(waybillNo) && waybillJson.getInteger("isDeleted") == 0) {
              getList.add(waybillNo)
              columns.put("packageMeterageWeightQty", ColumnType.DOUBLE)
            }
          }
          val hrs = hbase.getListSpecialForWbInfo(getList, columns);
          for (waybillResult <- hrs.entrySet().asScala; if hrs != null && !hrs.isEmpty) {
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
            listRow += Row(idKey,requireId, carNo, translevel, carStatus, srcZoneCode,destZoneCode, actualTime,  waybillNo, ticket, weight)
          }
        } else {
          listRow += Row(idKey,requireId, carNo, translevel, carStatus, srcZoneCode,destZoneCode, actualTime, null, 0L, 0D)
        }
        listRow.toArray
      }).flatMap(r => r.iterator)
    })
    val structFields = Array(StructField("idKey", StringType, true),StructField("requireId", StringType, true), StructField("carNo", StringType, true),
      StructField("transLevel", IntegerType, true),StructField("carStatus", IntegerType, true),StructField("srcZoneCode", StringType, true),
      StructField("destZoneCode", StringType, true),StructField("actualTime", LongType, true),StructField("waybillNo", StringType, true),
      StructField("ticket", LongType, true),StructField("weight", DoubleType, true))
    val structType = StructType(structFields)
    val f1Df = spark.createDataFrame(f1, structType)
    f1Df.repartition(400,f1Df("idKey")).createOrReplaceTempView("f1Df")

    // 结果写入临时表
    spark.sqlContext.sql("""drop table if exists tmp_ordi_predict.tmp_ky_has_arrive_cargo_yz_behind_hbase;""")
    spark.sqlContext.sql(
      """create table tmp_ordi_predict.tmp_ky_has_arrive_cargo_yz_behind_hbase
        |stored as parquet as
        | select idKey,requireId,carNo,transLevel,carStatus,srcZoneCode,destZoneCode,actualTime,waybillNo,ticket,weight
        | from f1Df""".stripMargin)
  }
}
