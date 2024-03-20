package com.sf.realtime.spark.batch.main.forecast

import java.sql.Timestamp
import java.util
import com.alibaba.fastjson.JSON
import com.sf.realtime.common.utils.DateUtil
import com.sf.realtime.hbase.HbaseUtil
import com.sf.realtime.hbase.common.ColumnType
import com.sf.realtime.spark.context.Context
import com.sf.realtime.spark.sql.{CarInfo, DeptInfo}
import com.sf.realtime.spark.utils.{CacheUtil, KgMysqlUtil, TidbUtil}
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
object InRoadCargoTask {
// 2023-07-12  preArriveTm is not null 判断
  def main(args: Array[String]): Unit = {
    val spark = Context.getContext(true)
    //1.获取中转场信息
    val transitDf = DeptInfo.getKyNewDeptInfo(spark.sqlContext)
    var count = 0L
    transitDf.createOrReplaceTempView("transit_info_tmp")
    val last30Day = args(0)
    val currentDay = args(1)
    val last3Day = args(2)
    //2.获取车辆信息前48小时车辆任务
    //old
    //val requireTaskDf1 = CarInfo.getVehicleNotSendTaskInfo(spark.sqlContext,DateUtil.getLastHoursTime(24),1).drop("currentOpZoneCode")
    //val requireTaskDf1 = CarInfo.getVehicleNotSendTaskInfo(spark.sqlContext,DateUtil.getLastHoursTime(24),1,true).drop("currentOpZoneCode")
    val requireTaskDf1 = spark.sqlContext.sql("""select
case when idKey = 'null' then null else idKey end as idKey,
case when requireId = 'null' then null else requireId end as requireId,
case when translevel = 'null' then null else cast(translevel as int) end as translevel,
case when carNo = 'null' then null else carNo end as carNo,
case when lineCode = 'null' then null else lineCode end as lineCode,
case when carStatus = 'null' then null else cast(carStatus as int) end as carStatus,
case when lastUpdateTm = 'null' then null else cast(lastUpdateTm as bigint) end as lastUpdateTm,
case when srcZoneCode = 'null' then null else srcZoneCode end as srcZoneCode,
case when srcPlanReachTm = 'null' then null else cast(srcPlanReachTm as bigint) end as srcPlanReachTm,
case when srcPlanDepartTm = 'null' then null else cast(srcPlanDepartTm as bigint) end as srcPlanDepartTm,
case when srcActualDepartTm = 'null' then null else cast(srcActualDepartTm as bigint) end as srcActualDepartTm,
case when srcPreDepartTm = 'null' then null else cast(srcPreDepartTm as bigint) end as srcPreDepartTm,
case when srcPlanArriveTm = 'null' then null else cast(srcPlanArriveTm as bigint) end as srcPlanArriveTm,
case when srcActualArriveTm = 'null' then null else cast(srcActualArriveTm as bigint) end as srcActualArriveTm,
case when srcPreArriveTm = 'null' then null else cast(srcPreArriveTm as bigint) end as srcPreArriveTm,
case when secondZoneCode = 'null' then null else secondZoneCode end as secondZoneCode,
case when secondPlanReachTm = 'null' then null else cast(secondPlanReachTm as bigint) end as secondPlanReachTm,
case when secondPlanDepartTm = 'null' then null else cast(secondPlanDepartTm as bigint) end as secondPlanDepartTm,
case when secondActualDepartTm = 'null' then null else cast(secondActualDepartTm as bigint) end as secondActualDepartTm,
case when secondPreDepartTm = 'null' then null else cast(secondPreDepartTm as bigint) end as secondPreDepartTm,
case when secondPlanArriveTm = 'null' then null else cast(secondPlanArriveTm as bigint) end as secondPlanArriveTm,
case when secondActualArriveTm = 'null' then null else cast(secondActualArriveTm as bigint) end as secondActualArriveTm,
case when secondPreArriveTm = 'null' then null else cast(secondPreArriveTm as bigint) end as secondPreArriveTm,
case when thirdZoneCode = 'null' then null else thirdZoneCode end as thirdZoneCode,
case when thirdPlanReachTm = 'null' then null else cast(thirdPlanReachTm as bigint) end as thirdPlanReachTm,
case when thirdPlanDepartTm = 'null' then null else cast(thirdPlanDepartTm as bigint) end as thirdPlanDepartTm,
case when thirdActualDepartTm = 'null' then null else cast(thirdActualDepartTm as bigint) end as thirdActualDepartTm,
case when thirdPreDepartTm = 'null' then null else cast(thirdPreDepartTm as bigint) end as thirdPreDepartTm,
case when thirdPlanArriveTm = 'null' then null else cast(thirdPlanArriveTm as bigint) end as thirdPlanArriveTm,
case when thirdActualArriveTm = 'null' then null else cast(thirdActualArriveTm as bigint) end as thirdActualArriveTm,
case when thirdPreArriveTm = 'null' then null else cast(thirdPreArriveTm as bigint) end as thirdPreArriveTm,
case when destZoneCode = 'null' then null else destZoneCode end as destZoneCode,
case when destPlanReachTm = 'null' then null else cast(destPlanReachTm as bigint) end as destPlanReachTm,
case when destPlanDepartTm = 'null' then null else cast(destPlanDepartTm as bigint) end as destPlanDepartTm,
case when destActualDepartTm = 'null' then null else cast(destActualDepartTm as bigint) end as destActualDepartTm,
case when destPreDepartTm = 'null' then null else cast(destPreDepartTm as bigint) end as destPreDepartTm,
case when destPlanArriveTm = 'null' then null else cast(destPlanArriveTm as bigint) end as destPlanArriveTm,
case when destActualArriveTm = 'null' then null else cast(destActualArriveTm as bigint) end as destActualArriveTm,
case when destPreArriveTm = 'null' then null else cast(destPreArriveTm as bigint) end as destPreArriveTm,
case when insertTime = 'null' then null else cast(insertTime as timestamp) end as insertTime,
case when fullLoadWeight = 'null' then null else cast(fullLoadWeight as double) end as fullLoadWeight,
case when srcLoadContnrNos = 'null' then null else srcLoadContnrNos end as srcLoadContnrNos,
case when srcArriveContnrNos = 'null' then null else srcArriveContnrNos end as srcArriveContnrNos,
case when srcUnloadContnrNos = 'null' then null else srcUnloadContnrNos end as srcUnloadContnrNos,
case when secondLoadContnrNos = 'null' then null else secondLoadContnrNos end as secondLoadContnrNos,
case when secondArriveContnrNos = 'null' then null else secondArriveContnrNos end as secondArriveContnrNos,
case when secondUnloadContnrNos = 'null' then null else secondUnloadContnrNos end as secondUnloadContnrNos,
case when thirdLoadContnrNos = 'null' then null else thirdLoadContnrNos end as thirdLoadContnrNos,
case when thirdArriveContnrNos = 'null' then null else thirdArriveContnrNos end as thirdArriveContnrNos,
case when thirdUnloadContnrNos = 'null' then null else thirdUnloadContnrNos end as thirdUnloadContnrNos,
case when destLoadContnrNos = 'null' then null else destLoadContnrNos end as destLoadContnrNos,
case when destArriveContnrNos = 'null' then null else destArriveContnrNos end as destArriveContnrNos,
case when destUnloadContnrNos = 'null' then null else destUnloadContnrNos end as destUnloadContnrNos,
case when srcJobType = 'null' then null else srcJobType end as srcJobType,
case when secondJobType = 'null' then null else secondJobType end as secondJobType,
case when thirdJobType = 'null' then null else thirdJobType end as thirdJobType,
case when destJobType = 'null' then null else destJobType end as destJobType,
case when nextzonecodedynamicprearrivetime = 'null' then null else nextzonecodedynamicprearrivetime end as nextzonecodedynamicprearrivetime,
inc_day
from dm_heavy_cargo.rt_vehicle_task_monitor_for_not_send_detail4 where inc_day >= '"""+last30Day+"""'""")

    //val requireTaskDf2 = spark.sqlContext.sql("""select * from dm_heavy_cargo.rt_vehicle_task_monitor_for_not_send_detail where inc_day >= '"""+last30Day+"""'""")
    //val requireTaskDf = requireTaskDf1.union(requireTaskDf2)
    val requireTaskDf = requireTaskDf1
    //requireTaskDf.createOrReplaceTempView("car_require_task_info")
    //3.取车辆任务的最新状态
    import spark.implicits._
    val w = Window.partitionBy($"requireId").orderBy($"lastUpdateTm".desc)
    val newRequireTaskDf = requireTaskDf.withColumn("rn", row_number().over(w)).where($"rn" === 1).drop("rn")
    newRequireTaskDf.createOrReplaceTempView("new_require_task_info")
    spark.sqlContext.cacheTable("new_require_task_info")
    val countDate = DateUtil.getDateStringWithoutHMS(0)
    val countTime = DateUtil.getTimestamp(0)
    val requireTask301 = spark.sqlContext.sql("""select * from new_require_task_info where carStatus in (1,2,3,4,5) and srcActualDepartTm is null""")
    requireTask301.createOrReplaceTempView("require_task_301")
    spark.sqlContext.cacheTable("require_task_301")
    val result1 = spark.sqlContext.sql("""select requireId as requireId,carNo as carNo,translevel as transLevel,carStatus as carStatus,srcZoneCode as srcZoneCode,secondPlanArriveTm as preArriveTm,secondZoneCode as preArriveZoneCode,0 as tickets,0 as weight,1 as status,'"""+countTime+"""' as countTime,'"""+countDate+"""' as countDate from require_task_301 a join transit_info_tmp b on secondZoneCode = b.deptCode where secondZoneCode is not null and secondJobType <> '1' and secondPlanArriveTm is not null """)
    val result4 = spark.sqlContext.sql("""select requireId as requireId,carNo as carNo,translevel as transLevel,carStatus as carStatus,srcZoneCode as srcZoneCode,destPlanArriveTm as preArriveTm,destZoneCode as preArriveZoneCode,0 as tickets,0 as weight,1 as status,'"""+countTime+"""' as countTime,'"""+countDate+"""' as countDate from require_task_301 a join transit_info_tmp b on destZoneCode = b.deptCode where destZoneCode is not null and destPlanArriveTm is not null """)
    val result6 = spark.sqlContext.sql("""select requireId as requireId,carNo as carNo,translevel as transLevel,carStatus as carStatus,srcZoneCode as srcZoneCode,thirdPlanArriveTm as preArriveTm,thirdZoneCode as preArriveZoneCode,0 as tickets,0 as weight,1 as status,'"""+countTime+"""' as countTime,'"""+countDate+"""' as countDate from require_task_301 a join transit_info_tmp b on thirdZoneCode = b.deptCode where thirdZoneCode is not null and thirdJobType <> '1' and thirdPlanArriveTm is not null """)
    val lastResult1 = result1.union(result4).union(result6).dropDuplicates("requireId","srcZoneCode","preArriveZoneCode")
    val requireTask302 = spark.sqlContext.sql("""select * from new_require_task_info where carStatus in (1,2,3,4,5) and srcActualDepartTm >0 and secondActualDepartTm is null""")
    requireTask302.createOrReplaceTempView("require_task_302")
    spark.sqlContext.cacheTable("require_task_302")
    val result21 = spark.sqlContext.sql("""select requireId as requireId,carNo as carNo,translevel as transLevel,carStatus as carStatus,secondZoneCode as srcZoneCode,thirdPlanArriveTm as preArriveTm,thirdZoneCode as preArriveZoneCode,0 as tickets,0 as weight,1 as status,'"""+countTime+"""' as countTime,'"""+countDate+"""' as countDate from require_task_302 a join transit_info_tmp b on thirdZoneCode = b.deptCode where secondZoneCode is not null and thirdZoneCode is not null and thirdJobType <> '1' and thirdPlanArriveTm is not null """)
    val result23 = spark.sqlContext.sql("""select requireId as requireId,carNo as carNo,translevel as transLevel,carStatus as carStatus,secondZoneCode as srcZoneCode,destPlanArriveTm as preArriveTm,destZoneCode as preArriveZoneCode,0 as tickets,0 as weight,1 as status,'"""+countTime+"""' as countTime,'"""+countDate+"""' as countDate from require_task_302 a join transit_info_tmp b on destZoneCode = b.deptCode where secondZoneCode is not null and destZoneCode is not null and destPlanArriveTm is not null """)
    val lastResult2 = result21.union(result23).dropDuplicates("requireId","srcZoneCode","preArriveZoneCode")
    val requireTask303 = spark.sqlContext.sql("""select * from new_require_task_info where carStatus in (1,2,3,4,5) and srcActualDepartTm >0 and secondActualDepartTm >0 and thirdActualDepartTm is null""")
    requireTask303.createOrReplaceTempView("require_task_303")
    val result31 = spark.sqlContext.sql("""select requireId as requireId,carNo as carNo,translevel as transLevel,carStatus as carStatus,thirdZoneCode as srcZoneCode,destPlanArriveTm as preArriveTm,destZoneCode as preArriveZoneCode,0 as tickets,0 as weight,1 as status,'"""+countTime+"""' as countTime,'"""+countDate+"""' as countDate from require_task_303 a join transit_info_tmp b on destZoneCode = b.deptCode where thirdZoneCode is not null and destZoneCode is not null and destPlanArriveTm is not null""")
    val lastResult3 = result31.dropDuplicates("requireId","srcZoneCode","preArriveZoneCode")
    val lr = lastResult1.union(lastResult2).union(lastResult3).where($"srcZoneCode" =!= $"preArriveZoneCode")
    lr.show(10)
    val count1 = lr.count()
    CacheUtil.updateTMonitorDetailData("t_monitor_in_road_cargo_new",new Timestamp(System.currentTimeMillis()),1)
    TidbUtil.write("t_monitor_in_road_cargo_new",lr,SaveMode.Overwrite)
    spark.sqlContext.uncacheTable("require_task_301")
    spark.sqlContext.uncacheTable("require_task_302")
    println("0:1 as status  preArriveTm is not null ")
    println("1:未发车辆计算完毕")
    println("2:开始计算在途")
    val requireTaskInRoad = requireTaskDf1.where("inc_day >= '"+last3Day+"'" ).withColumn("rn", row_number().over(w)).where($"rn" === 1).drop("rn")
    println("---1")
    requireTaskInRoad.createOrReplaceTempView("new_require_task_info2")
    println("---2")
    val inRoad = spark.sqlContext.sql("select * from new_require_task_info2 where carStatus in (1,2,3,4,5) and lastUpdateTm > " + DateUtil.getTime(7))
    println("---3")
    inRoad.createOrReplaceTempView("in_road_cars")
    println("---4")
    spark.sqlContext.cacheTable("in_road_cars")
    println("---5")
    val inFirst = spark.sqlContext.sql("select * from in_road_cars where srcActualDepartTm >0 and secondZoneCode is not null and secondActualArriveTm is null and thirdActualArriveTm is null and destActualArriveTm is null and secondJobType <> '1'")
    println("---6")
    inFirst.createOrReplaceTempView("in_road_cars_first")
    println("---7")
    val inFirstCarNos = spark.sqlContext.sql("select requireId, adTable.carNo as carNo,translevel,carStatus,srcZoneCode,secondZoneCode as preArriveZoneCode,coalesce(nextzonecodedynamicprearrivetime,secondPreArriveTm,secondPlanArriveTm) as preArriveTm from in_road_cars_first LATERAL VIEW explode(split(secondArriveContnrNos, ',')) adTable AS carNo")
    println("---8")
    val inSecond = spark.sqlContext.sql("select * from in_road_cars where srcActualDepartTm >0 and secondZoneCode is null and secondActualArriveTm is null and thirdActualArriveTm is null and destActualArriveTm is null and destZoneCode is not null")
    println("---9")
    inSecond.createOrReplaceTempView("in_road_cars_second")
    println("---10")
    val inSecondCarNos = spark.sqlContext.sql("select requireId, adTable.carNo as carNo,translevel,carStatus,srcZoneCode,destZoneCode as preArriveZoneCode ,coalesce(nextzonecodedynamicprearrivetime,destPreArriveTm,destPlanArriveTm) as preArriveTm from in_road_cars_second LATERAL VIEW explode(split(destArriveContnrNos, ',')) adTable AS carNo")
    println("---11")
    val inThird = spark.sqlContext.sql("select * from in_road_cars where srcActualDepartTm >0 and secondZoneCode is not null and secondActualDepartTm > 0 and thirdZoneCode is not null and thirdActualArriveTm is null and destActualArriveTm is null and thirdJobType <> '1'")
    println("---12")
    inThird.createOrReplaceTempView("in_road_cars_third")
    println("---13")
    val inThirdCarNos = spark.sqlContext.sql("select requireId, adTable.carNo as carNo,translevel,carStatus,secondZoneCode as srcZoneCode,thirdZoneCode as preArriveZoneCode,coalesce(nextzonecodedynamicprearrivetime,thirdPreArriveTm,thirdPlanArriveTm) as preArriveTm from in_road_cars_third LATERAL VIEW explode(split(thirdArriveContnrNos, ',')) adTable AS carNo")
    println("---14")
    val inForth = spark.sqlContext.sql("select * from in_road_cars where srcActualDepartTm >0 and secondZoneCode is not null and secondActualDepartTm > 0 and thirdZoneCode is null and destActualArriveTm is null and destZoneCode is not null")
    println("---15")
    inForth.createOrReplaceTempView("in_road_cars_forth")
    println("---16")
    val inForthCarNos = spark.sqlContext.sql("select requireId, adTable.carNo as carNo,translevel,carStatus,secondZoneCode as srcZoneCode,destZoneCode as preArriveZoneCode ,coalesce(nextzonecodedynamicprearrivetime,destPreArriveTm,destPlanArriveTm) as preArriveTm from in_road_cars_forth LATERAL VIEW explode(split(destArriveContnrNos, ',')) adTable AS carNo")
    println("---17")
    val inFiveth = spark.sqlContext.sql("select * from in_road_cars where srcActualDepartTm >0 and thirdZoneCode is not null and thirdActualDepartTm > 0  and destActualArriveTm is null and destZoneCode is not null")
    println("---18")
    inFiveth.createOrReplaceTempView("in_road_cars_five")
    println("---19")
    val inFivethCarNos = spark.sqlContext.sql("select requireId, adTable.carNo as carNo,translevel,carStatus,thirdZoneCode as srcZoneCode,destZoneCode as preArriveZoneCode ,coalesce(nextzonecodedynamicprearrivetime,destPreArriveTm,destPlanArriveTm) as preArriveTm from in_road_cars_five LATERAL VIEW explode(split(destArriveContnrNos, ',')) adTable AS carNo")
    println("---20")
    val inAllCarNos = inFirstCarNos.union(inSecondCarNos).union(inThirdCarNos).union(inForthCarNos).union(inFivethCarNos)
    println("---21")
    inAllCarNos.createOrReplaceTempView("in_all_car_nos")
    println("---22")
    val kyAllCarNos = spark.sqlContext.sql("""select a.* from in_all_car_nos a join transit_info_tmp b on preArriveZoneCode = b.deptCode where carNo is not null and carNo <> '' """)
    println("---23")
    val inNotCarNos = spark.sqlContext.sql("""select a.* from in_all_car_nos a join transit_info_tmp b on preArriveZoneCode = b.deptCode where carNo is null or carNo = '' """)
    println("---24")
    val f1 = kyAllCarNos.rdd.mapPartitions(f=>{
      val hbase = HbaseUtil.getInstance()
      f.map(r=>{
        val requireId = r.getAs[String]("requireId")
        val carNo = r.getAs[String]("carNo")
        val translevel = r.getAs[Integer]("translevel")
        val carStatus = r.getAs[Integer]("carStatus")
        val srcZoneCode = r.getAs[String]("srcZoneCode")
        val preArriveZoneCode = r.getAs[String]("preArriveZoneCode")
        val preArriveTm = r.getAs[Long]("preArriveTm")
        val rowKey = r.getAs[String]("carNo").reverse
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
            if (StringUtils.isNotEmpty(waybillNo)&&jobType.equals("30") && waybillJson.getInteger("isDeleted") == 0){
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
            if(waybillResult !=null){
              waybillNo = waybillResult.getKey
              val wCells = waybillResult.getValue
              for(wCell <- wCells.entrySet().asScala) {
                weight = wCell.getValue.asInstanceOf[Double]
                if(weight > 1000000D){
                  weight = 1000000D
                }
                ticket = 1L
              }
            }
            listRow += Row(requireId,carNo,translevel,carStatus,srcZoneCode,preArriveTm,preArriveZoneCode,waybillNo,ticket,weight)
          }
        }else{
          listRow += Row(requireId,carNo,translevel,carStatus,srcZoneCode,preArriveTm,preArriveZoneCode,null,0L,0D)
        }
        listRow.toArray
      }).flatMap(r=>r.iterator)
    })
    println("---25")
    val structFields = Array(StructField("requireId", StringType, true), StructField("carNo", StringType, true),
      StructField("transLevel", IntegerType, true),StructField("carStatus", IntegerType, true),StructField("srcZoneCode", StringType, true),StructField("preArriveTm", LongType, true),StructField("preArriveZoneCode", StringType, true),StructField("waybillNo", StringType, true),StructField("tickets", LongType, true),StructField("weight", DoubleType, true))
    println("---26")
    val structType = StructType(structFields)
    println("---27")
    val f1Df = spark.createDataFrame(f1, structType)
    println("---28")
    f1Df.createOrReplaceTempView("f1Df")
    println("---29")

    //获取快管修改车辆预计到达时间

    val kgTableName = "(select require_id,zone_code,next_zone_code,prologis_in_tm from t_transportct_info where prologis_in_tm is not null ) as t"
    println("---30")
    val kgData = KgMysqlUtil.read(kgTableName,spark.sqlContext)
    println("---31")
    kgData.createOrReplaceTempView("kg_data")
    println("---32")

    val f1DfAfterProcess = spark.sqlContext.sql("select * from (select requireId,transLevel,carStatus,srcZoneCode,preArriveTm,preArriveZoneCode,sum(tickets) over(partition by requireId,preArriveZoneCode) as tickets,sum(weight) over(partition by requireId,preArriveZoneCode) as weight,row_number() over(partition by requireId,preArriveZoneCode order by preArriveTm desc) as num from f1Df )t where t.num = 1").drop("num")
    println("---33")
    f1DfAfterProcess.createOrReplaceTempView("in_road_total")
    println("---34")
    val inTotal = spark.sqlContext.sql("""select requireId,"" as carNo, transLevel, carStatus, srcZoneCode, case when b.prologis_in_tm is null then preArriveTm else unix_timestamp(b.prologis_in_tm,'yyyy-MM-dd HH:mm:ss')*1000 end as preArriveTm, preArriveZoneCode, tickets, weight, 2 as status, '"""+countTime+"""' as countTime,'"""+countDate+"""' as countDate from in_road_total a left join kg_data b on requireId = b.require_id and srcZoneCode = b.zone_code and preArriveZoneCode = b.next_zone_code""")
    println("---35")
    //    val inTotal = spark.sqlContext.sql("""select requireId,"" as carNo, transLevel, carStatus, srcZoneCode, preArriveTm, preArriveZoneCode, tickets, weight, 2 as status, '"""+countTime+"""' as countTime,'"""+countDate+"""' as countDate from in_road_total """)
    inNotCarNos.createOrReplaceTempView("in_not_carno_cars")
    println("---36")
    val inTotal2 = spark.sqlContext.sql("""select requireId,"" as carNo, translevel as transLevel, carStatus, srcZoneCode, case when b.prologis_in_tm is null then preArriveTm else unix_timestamp(b.prologis_in_tm,'yyyy-MM-dd HH:mm:ss')*1000 end as preArriveTm, preArriveZoneCode, 0 as tickets, 0 as weight, 2 as status, '"""+countTime+"""' as countTime,'"""+countDate+"""' as countDate from in_not_carno_cars  a left join kg_data b on requireId = b.require_id and srcZoneCode = b.zone_code and preArriveZoneCode = b.next_zone_code""").dropDuplicates("requireId","srcZoneCode", "preArriveZoneCode")
    println("---37")
    //    val inTotal2 = spark.sqlContext.sql("""select requireId,"" as carNo, translevel as transLevel, carStatus, srcZoneCode, preArriveTm, preArriveZoneCode, 0 as tickets, 0 as weight, 2 as status, '"""+countTime+"""' as countTime,'"""+countDate+"""' as countDate from in_not_carno_cars """).dropDuplicates("requireId","srcZoneCode", "preArriveZoneCode")
    val inTotalAll = inTotal.union(inTotal2).where($"srcZoneCode" =!= $"preArriveZoneCode")
    println("---38")
    val count2 = inTotalAll.count()
    println("---39")
    TidbUtil.write("t_monitor_in_road_cargo_new",inTotalAll,SaveMode.Append)
    println("---40")
    spark.sqlContext.uncacheTable("in_road_cars")
    println("---41")
    println("3:在途货量计算完毕")

    println("4:开始计算已到达")
    //    val hasArriveTaskDf = CarInfo.getVehicleHasArriveTaskInfo(spark.sqlContext,DateUtil.getLastHoursTime(24),1)
    //    hasArriveTaskDf.createOrReplaceTempView("has_arrive_cars")
    val hasArriveTaskDf = CarInfo.getVehicleHasArriveTaskInfo(spark.sqlContext,DateUtil.getLastHoursTime(24),1)
    hasArriveTaskDf.createOrReplaceTempView("has_arrive_cars_pre")
    val hasArriveTaskDf2 = spark.sql("""select * from (select *,row_number() over(partition by carNo order by actualTime) rn from has_arrive_cars_pre)t where t.rn = 1""").drop("rn")
    hasArriveTaskDf2.createOrReplaceTempView("has_arrive_cars")
    val hasArriveTotalPre = spark.sqlContext.sql("select * from (select requireId as requireId,transLevel as translevel,carStatus as carStatus,srcZoneCode as srcZoneCode,actualTime as preArriveTm,destZoneCode as preArriveZoneCode,sum(ticket) over(partition by requireId,destZoneCode) as tickets,sum(weight) over(partition by requireId,destZoneCode) as weight,row_number() over(partition by requireId,destZoneCode order by actualTime) as num from has_arrive_cars )t where t.num = 1").drop("num")
    hasArriveTotalPre.createOrReplaceTempView("has_arrive_total")
    val hasArriveTotal = spark.sqlContext.sql("""select requireId,"" as carNo, transLevel, carStatus, srcZoneCode, preArriveTm, preArriveZoneCode, tickets, weight, 3 as status, '"""+countTime+"""' as countTime,'"""+countDate+"""' as countDate from has_arrive_total """)
    //补充已到达车辆没有车标的情况
    requireTaskDf1.withColumn("rn", row_number().over(w)).where($"rn" === 1).drop("rn").createOrReplaceTempView("last_day_car_info")
    val hasArrive1 = spark.sqlContext.sql("""select requireId,"" as carNo, transLevel, carStatus,srcZoneCode,secondActualArriveTm as preArriveTm,secondZoneCode as preArriveZoneCode,0 as tickets, 0 as weight,3 as status, '"""+countTime+"""' as countTime,'"""+countDate+"""' as countDate from last_day_car_info where (carNo is null or carNo = '') and srcActualDepartTm > 0 and secondActualArriveTm > 0 and secondActualDepartTm is null""")
    val hasArrive2 = spark.sqlContext.sql("""select requireId,"" as carNo, transLevel, carStatus,secondZoneCode as srcZoneCode,thirdActualArriveTm as preArriveTm,thirdZoneCode as preArriveZoneCode,0 as tickets, 0 as weight,3 as status, '"""+countTime+"""' as countTime,'"""+countDate+"""' as countDate from last_day_car_info where (carNo is null or carNo = '') and secondActualDepartTm > 0 and thirdActualArriveTm > 0 and thirdActualDepartTm is null""")
    val hasArrive3 = spark.sqlContext.sql("""select requireId,"" as carNo, transLevel, carStatus,thirdZoneCode as srcZoneCode,destActualArriveTm as preArriveTm, destZoneCode as preArriveZoneCode,0 as tickets, 0 as weight,3 as status, '"""+countTime+"""' as countTime,'"""+countDate+"""' as countDate from last_day_car_info where (carNo is null or carNo = '') and thirdActualDepartTm > 0 and destActualArriveTm > 0 """)
    val hasArrive4 = spark.sqlContext.sql("""select requireId,"" as carNo, transLevel, carStatus,secondZoneCode as srcZoneCode,destActualArriveTm as preArriveTm,destZoneCode as preArriveZoneCode,0 as tickets, 0 as weight,3 as status, '"""+countTime+"""' as countTime,'"""+countDate+"""' as countDate from last_day_car_info where (carNo is null or carNo = '') and secondActualDepartTm > 0 and thirdZoneCode is null and destActualArriveTm > 0""")
    val hasArrive5 = spark.sqlContext.sql("""select requireId,"" as carNo, transLevel, carStatus,srcZoneCode,destActualArriveTm as preArriveTm, destZoneCode as preArriveZoneCode,0 as tickets, 0 as weight,3 as status, '"""+countTime+"""' as countTime,'"""+countDate+"""' as countDate from last_day_car_info where (carNo is null or carNo = '') and srcActualDepartTm > 0 and secondZoneCode is null and thirdZoneCode is null and destActualArriveTm > 0 """)
    val hasArriveTotalPre2 = hasArrive1.union(hasArrive2).union(hasArrive3).union(hasArrive4).union(hasArrive5).dropDuplicates("requireId","preArriveZoneCode")
    hasArriveTotalPre2.createOrReplaceTempView("has_arrive_total_pre2")
    val hasArriveTotal2 = spark.sqlContext.sql("""select a.* from has_arrive_total_pre2 a join transit_info_tmp b on preArriveZoneCode = b.deptCode""")
    hasArriveTotal.union(hasArriveTotal2).createOrReplaceTempView("has_arrive_all")
    val hasArriveResult = spark.sqlContext.sql("""select t.* from (select a.*,row_number() over(partition by requireId,preArriveZoneCode order by weight desc) as rn from has_arrive_all a)t where t.rn = 1 """).drop("rn").where($"srcZoneCode" =!= $"preArriveZoneCode")
    println("4:已到达货量计算完毕")
    val count3 = hasArriveResult.count()
    TidbUtil.write("t_monitor_in_road_cargo_new",hasArriveResult,SaveMode.Append)
    CacheUtil.updateTMonitorDetailData("t_monitor_in_road_cargo_new",new Timestamp(System.currentTimeMillis()),2)
    count = count1 + count2 + count3
    CacheUtil.insertTMonitorDataRows("t_monitor_in_road_cargo_new",new Timestamp(System.currentTimeMillis()),count)
  }
}


