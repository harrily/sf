package com.sf.realtime.spark.batch.main.sf

import com.alibaba.fastjson.JSON
import com.sf.realtime.common.utils.DateUtil
import com.sf.realtime.hbase.HbaseUtil
import com.sf.realtime.hbase.common.ColumnType
import com.sf.realtime.spark.sql.CarInfo
import com.sf.realtime.spark.utils.{CloudTidbUtil, TidbUtil}
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}

import java.sql.Timestamp
import scala.collection.JavaConverters._
import java.util
import java.util.UUID
import scala.collection.mutable.ArrayBuffer

/**
 * @Author 01419728
 * @Date 2022/6/15 18:03
 */
object KyHasArriveCargoYZ {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("")
    //设置优雅关闭
    conf.set("spark.streaming.stopGracefullyOnShutdown","true")
    //开启反压
    conf.set("spark.streaming.backpressure.enabled","true")
    //提高shuffle并行度
    conf.set("spark.sql.shuffle.partitions", "300")
    //RDD任务的默认并行度
    conf.set("spark.default.parallelism", "300")
    //设置字段长度最大值
    conf.set("spark.debug.maxToStringFields", "100")
    conf.set("hive.exec.dynamici.partition", "true")
    conf.set("hive.exec.dynamic.partition.mode", "nonstrict")


    val spark =  SparkSession.builder()
      .config(conf)
      .enableHiveSupport() //enableHiveSupport 操作hive表
      .getOrCreate()
    val currentTime = args(0)
    val inc_day = args(1)

    val hasArriveTaskDf = CarInfo.getVehicleHasArriveTaskInfo(spark.sqlContext,DateUtil.getLastHoursTime(24),1)
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
    val result = spark.sql(" select idKey,requireId,carNo ,transLevel,carStatus,srcZoneCode ,destZoneCode, " +
      " sum(ticket) as ticket,sum(weight) as weight,actualTime, " +
      " '" + currentTime + "' as insertTime " +
      " from f1Df group by idKey,requireId,carNo,transLevel,carStatus,srcZoneCode,destZoneCode,actualTime ")
    // todo 写入tidb
    result
      .select("idKey","requireId", "carNo", "transLevel", "carStatus", "srcZoneCode", "destZoneCode", "ticket", "weight", "actualTime", "insertTime")
      .foreachPartition((p:Iterator[Row]) => {
      var connection = CloudTidbUtil.getConn()
      val sql = """replace into vt_has_arrive_cars (idKey,requireId,carNo,transLevel,carStatus,srcZoneCode ,destZoneCode,ticket,weight,actualTime,insertTime) values(?,?,?,?,?,?,?,?,?,?,?)"""
      val st = connection.prepareStatement(sql)
      p.foreach(r => {
        var i = 1
        for (field <- r.schema.fields) {
          val name = field.name
          val fType = field.dataType
          fType match {
            case StringType => st.setString(i, r.getAs[String](name))
            case LongType => st.setLong(i, r.getAs[Long](name))
            case DoubleType => st.setDouble(i, r.getAs[Double](name))
            case IntegerType => st.setInt(i, r.getAs[Integer](name))
            case TimestampType => st.setTimestamp(i, r.getAs[Timestamp](name))
          }
          i = i + 1
        }
        st.executeUpdate()
      })
      st.close()
      connection.close()
    })

/*
    val hiveDate = spark.sql("SELECT require_id,car_no,translevel,car_status,src_zone_code,dest_zone_code,ticket,weight,arrive_time,create_time " +
      " FROM dm_heavy_cargo.dm_has_arrive_cargo_yz_dtl_df where inc_day ='" + inc_day + "'")
    result.union(hiveDate).repartition(1).createOrReplaceTempView("alldayresult")

    spark.sqlContext.sql("set hive.merge.sparkFiles=true;")
    spark.sqlContext.sql("set mapred.max.split.size=268435456;")
    spark.sqlContext.sql("set mapred.min.split.size.per.node=268435456;")
    spark.sqlContext.sql("set mapred.min.split.size.per.rack=268435456;")
    spark.sqlContext.sql("set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;")
    spark.sqlContext.sql("set hive.exec.reducers.bytes.per.reducer=268435456;")
    spark.sqlContext.sql("set hive.exec.reducers.max=1099;")
    spark.sqlContext.sql("set hive.merge.size.per.task=268435456;")
    spark.sqlContext.sql("set hive.merge.smallfiles.avgsize=134217728;")
    spark.sql("" +
      " insert overwrite table dm_heavy_cargo.dm_has_arrive_cargo_yz_dtl_df partition (inc_day='"+inc_day+"') " +
      " select require_id,car_no,translevel,car_status,src_zone_code,dest_zone_code,ticket,weight,arrive_time,create_time from alldayresult" +
      "" )*/
  }
}
