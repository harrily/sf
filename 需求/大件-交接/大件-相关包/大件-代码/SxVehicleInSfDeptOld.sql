package com.sf.realtime.spark.batch.main.forecast

import com.sf.realtime.common.utils.DateUtil
import com.sf.realtime.spark.context.Context
import com.sf.realtime.spark.sql.{CarInfo, DeptInfo}
import com.sf.realtime.spark.utils.{CacheUtil, TidbUtil}
import org.apache.spark.sql.functions.{lit, max, min, sum}
import org.apache.spark.sql.{DataFrame, Dataset, Row}


/**
 * 三个测试场地小件场地顺心车辆数据
 *
 * @Author 01419728
 * @Date 2022/4/19 14:26
 */
object SxVehicleInSfDeptOld {
  def main(args: Array[String]): Unit = {
    val countTime = args(0) //业务时间,也就是每20分钟  统计时间"yyyy-MM-dd HH:mm:ss"
    val tableName = args(1) //tableName
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
      hasArriveTaskDf = CarInfo.getVehicleHasArriveTaskInfo(spark.sqlContext,DateUtil.getLastHoursTime(24*14),14)
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
    val resultBatch = cargoInfoDf.join(deptCodeDf, deptCodeDf("deptCode").equalTo(cargoInfoDf("next_site_code")), "left")
      .where("deptCode is not null")
      .join(hasArriveTaskDf, hasArriveTaskDf("requireId").equalTo(cargoInfoDf("shift_no")), "left")
      .where("requireId is null")
      .selectExpr("shift_no", "site_code", "next_site_code", "total_out_weight", "total_out_ewbcount", "total_out_piece", "total_out_vol",
        "first_unlaod_time", "standard_plan_arrival_time", "in_confirm_time",
        "cast(transport_level as int) transport_level", "ewblist_send_time", "vehicle_status")

    //写入tidb
    //判断是版本号是哪个   版本控制表 pub_table_sync_version
    val versionDf = TidbUtil.read("pub_table_sync_version", spark.sqlContext)
    var versionId = versionDf.where("table_name='"+tableName+"' ")
      .collect()(0).getAs("version_id").asInstanceOf[Long]


    //println("表dws_sf_dept_sx_vehicle_sum写入前最新版本号是: "+versionId)
    //如果当前版本是1,  删除版本为2的数据 则写入数据为version=2
    if (1L == versionId) {
      versionId = 2L
    } else {
      versionId = 1L
    }

    var resultData = resultBatch.withColumn("version_id", lit(versionId))
      .dropDuplicates("shift_no", "site_code", "next_site_code","version_id")

    val conn = TidbUtil.conn()
    val st = conn.createStatement()
    //循环删除老版本数据
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
    }
  }
}
