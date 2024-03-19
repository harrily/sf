package com.sf.realtime.spark.batch.main.forecast

import com.sf.realtime.common.utils.DateUtil
import com.sf.realtime.spark.context.Context
import com.sf.realtime.spark.sql.{CarInfo, DeptInfo}
import com.sf.realtime.spark.utils.TidbUtil
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{lit, max, min, row_number, sum}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}


/**
 * 三个测试场地小件场地顺心车辆数据
 *
 * @Author 01419728
 * @Date 2022/4/19 14:26
 */
object SxVehicleInSfDept {
  def main(args: Array[String]): Unit = {
    val countTime = args(0) //业务时间,也就是每20分钟  统计时间"yyyy-MM-dd HH:mm:ss"
    val countDate = DateUtil.df2Todf3(countTime) //统计时间"yyyyMMdd"
    val incDay = DateUtil.df2Todf3(countTime)
    val incHour = countTime.substring(11, 13).concat(countTime.substring(14, 16))
    val last14Day = DateUtil.getLastDayStringdf3(countDate, 14)
    val next4Day = DateUtil.getLastStringTime(countTime,-5).substring(0, 10).concat(" 00:00:00") //统计时间"yyyy-MM-dd HH:mm:ss" 取00:00:00
    val last3Day = DateUtil.getLastStringTime(countTime,3).substring(0, 10).concat(" 00:00:00") //统计时间"yyyy-MM-dd HH:mm:ss"取00:00:00
    val last8Day = DateUtil.getLastDayStringdf3(DateUtil.df2Todf3(countTime), 4)
    val next8Day = DateUtil.getLastDayStringdf3(DateUtil.df2Todf3(countTime), -5)
    //1.读取SX车辆实时进港明细

    val spark = Context.getContext(true)
    val cargoInfoDf = spark.sqlContext.sql(
      """select * from ky.dm_freight.dwd_sx_vehicle_dtl_di where inc_day between '""" + last14Day + """' and '""" + countDate + """'""")
      .groupBy("shift_no", "site_code", "next_site_code")
      .agg( sum("total_out_weight") as ("total_out_weight"),
        min("first_unlaod_time") as ("first_unlaod_time"),
        max("standard_plan_arrival_time") as ("standard_plan_arrival_time"),
        max("in_confirm_time") as ("in_confirm_time"),
        max("transport_level") as  ("transport_level"),
        max("ewblist_send_time") as  ("ewblist_send_time"),
        max("vehicle_status") as ("vehicle_status"),
        max("total_out_ewbcount") as ("total_out_ewbcount"),
        max("total_out_piece") as ("total_out_piece"),
        max("total_out_vol") as ("total_out_vol")
      )
      .selectExpr("shift_no","site_code","next_site_code","total_out_weight","first_unlaod_time","standard_plan_arrival_time","in_confirm_time","transport_level",
        "ewblist_send_time","vehicle_status","total_out_ewbcount","total_out_piece","total_out_vol")

    //2.场地过滤  快运/小件场地
    var deptCodeDf: DataFrame = null
    var hasArriveTaskDf: Dataset[Row] = null
//    if ("dws_sx_vehicle_sum".equals(tableName)) {
      deptCodeDf = DeptInfo.getKyNewDeptInfo(spark.sqlContext)
      //3.顺丰车辆数据过滤
      hasArriveTaskDf = CarInfo.getVehicleHasArriveBatchTaskInfo(spark.sqlContext,DateUtil.getLastStringTime(countTime,30),30)
        .selectExpr("requireId")
        .dropDuplicates("requireId")
    /*} else if ("dws_sf_dept_sx_vehicle_sum".equals(tableName)) {
      deptCodeDf = DeptInfo.get155BigNewDeptInfo(spark.sqlContext)
      //3.顺丰车辆数据过滤
      hasArriveTaskDf = CarInfo.getVehicleBigHasArriveTaskInfo(spark.sqlContext, DateUtil.getLastHoursTime(24*30), 30)
        .selectExpr("requireId")
        .dropDuplicates("requireId")
    }*/
    import spark.implicits._
    //进行join,过滤场地,过滤顺丰包含的
      cargoInfoDf.join(deptCodeDf, deptCodeDf("deptCode").equalTo(cargoInfoDf("next_site_code")), "left")
      .where("deptCode is not null")
      .join(hasArriveTaskDf, hasArriveTaskDf("requireId").equalTo(cargoInfoDf("shift_no")), "left")
      .where("requireId is null")
      .selectExpr("shift_no", "site_code", "next_site_code", "total_out_weight", "total_out_ewbcount", "total_out_piece", "total_out_vol",
        "first_unlaod_time", "standard_plan_arrival_time", "in_confirm_time",
        " if( datediff(first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,if(datediff(in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,standard_plan_arrival_time,in_confirm_time),first_unlaod_time ) as pre_arrive_time",
        " date_format(if( datediff(first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,if(datediff(in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,standard_plan_arrival_time,in_confirm_time),first_unlaod_time ),'yyyy-MM-dd HH:mm') as arrTm",
        "cast(transport_level as int) transport_level", "ewblist_send_time", "vehicle_status")
        .where($"pre_arrive_time" between(last3Day,next4Day))
      .createOrReplaceTempView("sxNoBatch")

    //读取班次信息
    val batchInfo = spark.sqlContext.sql(""" select operate_zone_code,batch_code,batch_date, last_last_arrive_tm, last_arrive_tm from ky.dm_heavy_cargo.dm_arrive_batch_info_dtl_di  where inc_day between '"""+last8Day+"""' and '"""+next8Day+"""' """)
    batchInfo.createOrReplaceTempView("batch_info")

    val resultBatch = spark.sqlContext.sql(
      """
          select
              shift_no,site_code,next_site_code,total_out_weight,total_out_ewbcount,total_out_piece,total_out_vol,ewblist_send_time,in_confirm_time,first_unlaod_time,standard_plan_arrival_time,
              pre_arrive_time,
              transport_level, case vehicle_status when '0' then '1' when '1' then '2' else '3' end  status,
              batch_code,if(batch_date is not null,date_format(batch_date, 'yyyyMMdd'),batch_date)  as batch_date,'"""+countTime+"""' as count_time
         from
            (select
                    shift_no,site_code,next_site_code,total_out_weight,total_out_ewbcount,total_out_piece,total_out_vol,first_unlaod_time,standard_plan_arrival_time,in_confirm_time,
                    pre_arrive_time,transport_level,ewblist_send_time,vehicle_status,
                    max(batch_code) over(partition by shift_no,site_code,next_site_code) batch_code,
                    max(batch_date) over(partition by shift_no,site_code,next_site_code) batch_date,
                    row_number() over(partition by shift_no,site_code,next_site_code order by pre_arrive_time) rn
            from
                (select
                        shift_no,site_code,next_site_code,total_out_weight,total_out_ewbcount,total_out_piece,total_out_vol,first_unlaod_time,standard_plan_arrival_time,in_confirm_time,pre_arrive_time,arrTm,transport_level,ewblist_send_time,vehicle_status,
                        if((b.arrTm>=a.last_last_arrive_tm and b.arrTm<a.last_arrive_tm),batch_code,null) batch_code,
                        if((b.arrTm>=a.last_last_arrive_tm and b.arrTm<a.last_arrive_tm),batch_date,null) batch_date
                        from sxNoBatch b left join batch_info a
                        on b.next_site_code=a.operate_zone_code
                )t1
            )t2
         where rn=1""")

    resultBatch
      .dropDuplicates("shift_no", "site_code", "next_site_code")
      .repartition(1).createOrReplaceTempView("resultAll")
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
      """ insert overwrite table ky.dm_heavy_cargo.dm_cargo_quantity_sx_vehicle_dtl_rt
        | partition(inc_day='""".stripMargin+ incDay +"""', inc_hour='"""+ incHour +
        """')
          | select shift_no,site_code,next_site_code,total_out_weight,total_out_ewbcount,total_out_piece,total_out_vol,ewblist_send_time,in_confirm_time,first_unlaod_time,standard_plan_arrival_time,pre_arrive_time,transport_level,status,batch_code,batch_date,count_time
          | from resultAll""".stripMargin)
    //写入tidb
    /*//判断是版本号是哪个   版本控制表 pub_table_sync_version
    val versionDf = TidbUtil.read("pub_table_sync_version", spark.sqlContext)
    var versionId = versionDf.where("table_name='"+tableName+"' ")
      .collect()(0).getAs("version_id").asInstanceOf[Long]


    //println("表dws_sf_dept_sx_vehicle_sum写入前最新版本号是: "+versionId)
    var resultData = resultBatch
    //如果当前版本是1,  删除版本为2的数据 则写入数据为version=2
    if (1L == versionId) {
      versionId = 2L
    } else {
      versionId = 1L
    }

    resultData = resultBatch.withColumn("version_id", lit(versionId))
      .dropDuplicates("shift_no", "site_code", "next_site_code","version_id")


//    //"shift_no", "site_code", "next_site_code", "total_out_weight", "total_out_ewbcount", "total_out_piece", "total_out_vol",
//    //        "first_unlaod_time", "standard_plan_arrival_time", "in_confirm_time", "transport_level", "ewblist_send_time", "vehicle_status"
//
//    val structFields = Array(
//      StructField("shift_no", StringType, true),StructField("site_code", StringType, true),StructField("next_site_code", StringType, true),StructField("total_out_weight", DoubleType, true),
//      StructField("total_out_ewbcount", IntegerType, true), StructField("total_out_piece", IntegerType, true),StructField("total_out_vol", DoubleType, true),
//      StructField("first_unlaod_time", StringType, true),StructField("standard_plan_arrival_time", StringType, true),StructField("in_confirm_time", StringType, true),
//      StructField("transport_level", IntegerType, true),StructField("ewblist_send_time", StringType, true),StructField("vehicle_status", IntegerType, true),StructField("version_id", LongType, true))
//    val structType = StructType(structFields)
//    val f1Df: DataFrame = spark.createDataFrame(resultData.toJavaRDD, structType)

    val conn = TidbUtil.conn()
    val st = conn.createStatement()
    //循环删除老版本数据

//    val value = TidbUtil.read("(select count(*) aa from where FROM dmpdsp." + tableName + " WHERE version_id = " + versionId + ") as t", spark)
    val sql1 = "DELETE FROM dmpdsp."+tableName+" WHERE version_id = " + versionId + " "
    st.executeUpdate(sql1)
    //写入tidb   dws_sf_dept_sx_vehicle_sum  appendx写入

    TidbUtil.write(tableName, resultData)
    val sql2 = "update pub_table_sync_version set version_id = " + versionId + ",sync_time = current_timestamp() where table_name = '"+tableName+"' "
    st.executeUpdate(sql2)
    if (st != null) {
      st.close()
    }
    if (conn!=null) {
      conn.close()
    }*/
  }
}
