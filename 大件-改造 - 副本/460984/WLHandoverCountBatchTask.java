package com.sf.realtime.spark.batch.main.forecast

import com.sf.realtime.common.utils.DateUtil
import com.sf.realtime.spark.context.Context
import com.sf.realtime.spark.utils.CloudTidbUtil
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._

import java.sql.Timestamp
import java.util.UUID

object WLHandoverCountBatchTask {

  def main(args: Array[String]): Unit = {
    val startDay = args(0)
    val endDay = args(1)
    val currentDate = args(2) //业务时间  $[time(yyyy-MM-dd HH:mm:ss)]
    val incDay = DateUtil.df2Todf3(currentDate)
    val incHour = currentDate.substring(11, 13).concat(currentDate.substring(14, 16))
    //可回刷使用lastUpdateTm进行控制
    val last8Day = DateUtil.getLastDayStringdf3(DateUtil.df2Todf3(currentDate), 4)
    val next8Day = DateUtil.getLastDayStringdf3(DateUtil.df2Todf3(currentDate), -5)
    val spark = Context.getContext(true)
    val detailData = spark.sql("""select * from (select *,row_number() over(partition by eventtype,tripid,packageno order by eventtime) as rn from bdp.dm_freight.op_tp_wl_handover_detail_info where inc_day between '"""+startDay+"""' and '"""+endDay+"""')t where t.rn = 1 """).drop("rn")
    detailData.createOrReplaceTempView("detail_data")
    //获取已到达的汇总数据
    val rs1 = spark.sql("""select eventtype as event_type,deptcode as dept_code,tripid as trip_id,min(eventtime) as event_time, sum(packageweight) as weight, count(packageno) as ticket from detail_data where eventtype = 'ARRIVE_END' group by eventtype,deptcode,tripid""")
    rs1.createOrReplaceTempView("rs1")
    val rs2 = spark.sql("""select eventtype as event_type,deptcode as dept_code,tripid as trip_id,min(eventtime) as event_time, sum(packageweight) as weight, count(packageno) as ticket from (select t1.* from (select * from detail_data where eventtype = 'TRANSPORT')t1 left join rs1 t2 on t1.tripid = t2.trip_id where t2.trip_id is null) t group by t.eventtype,t.deptcode,t.tripid""")
    val rs = rs1.union(rs2)
    val idUdf: ((String) => String) = (code: String) => {UUID.randomUUID().toString()}
    val addId = udf(idUdf)
    import spark.implicits._
    val resultPre = rs.withColumn("id_key", addId($"dept_code"))
    resultPre.createOrReplaceTempView("result_pre")
    val result = spark.sql("""select id_key,event_type,dept_code,FROM_UNIXTIME(int(event_time/1000)) as event_time,trip_id,weight,ticket,if(event_type='TRANSPORT',FROM_UNIXTIME(bigint(bigint(event_time)/1000)+(60*60*3)),FROM_UNIXTIME(int(event_time/1000))) as plan_arrive_time,if(event_type='TRANSPORT',FROM_UNIXTIME(bigint(bigint(event_time)/1000)+(60*60*3),'yyyy-MM-dd HH:mm'),FROM_UNIXTIME(int(event_time/1000),'yyyy-MM-dd HH:mm')) as arrTm,'"""+incDay+"""' as count_date,'"""+currentDate+"""' as count_time from result_pre """)
    result.createOrReplaceTempView("resultNoBatch")
    //归集班次
    //读取班次信息
    val batchInfo = spark.sqlContext.sql(""" select operate_zone_code,batch_code,batch_date, last_last_arrive_tm, last_arrive_tm from ky.dm_heavy_cargo.dm_arrive_batch_info_dtl_di  where inc_day between '"""+last8Day+"""' and '"""+next8Day+"""' """)
    batchInfo.createOrReplaceTempView("batch_info")


    val resultBatch = spark.sqlContext.sql(
      """ select
        |        id_key,event_type,event_time,plan_arrive_time,trip_id,weight,ticket,count_time,count_date,dept_code,
        |        batch_code,
        |        if(batch_date is not null,date_format(batch_date, 'yyyyMMdd'),batch_date)  as batch_date,count_time
        | from
        |    (select
        |            id_key,event_type,dept_code,event_time,plan_arrive_time,trip_id,weight,ticket,count_time,count_date,
        |            max(batch_code) over(partition by trip_id,dept_code,event_type) batch_code,
        |            max(batch_date) over(partition by trip_id,dept_code,event_type) batch_date,
        |            row_number() over(partition by trip_id,dept_code,event_type order by plan_arrive_time) rn
        |    from
        |        (select
        |                id_key,event_type,dept_code,event_time,plan_arrive_time,trip_id,weight,ticket,count_time,count_date,arrTm,
        |                if((b.arrTm>=a.last_last_arrive_tm and b.arrTm<a.last_arrive_tm),batch_code,null) batch_code,
        |                if((b.arrTm>=a.last_last_arrive_tm and b.arrTm<a.last_arrive_tm),batch_date,null) batch_date
        |                from resultNoBatch b left join batch_info a
        |                on b.dept_code=a.operate_zone_code
        |        )t1
        |    )t2
        | where rn=1""".stripMargin)

    resultBatch
      .selectExpr("dept_code","trip_id",
        "if(event_type='TRANSPORT',event_time,null) as depart_time",
        "plan_arrive_time",
        "if(event_type='ARRIVE_END',event_time,null) as arrive_time",
        "if(event_type='ARRIVE_END',event_time,plan_arrive_time) as pre_arrive_time",
        "weight","ticket",
        "if(event_type='ARRIVE_END','3','2') status",
        "batch_code","batch_date","count_time")
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
      """ insert overwrite table ky.dm_heavy_cargo.dm_cargo_quantity_handover_dtl_rt
        | partition(inc_day='""".stripMargin+ incDay +"""', inc_hour='"""+ incHour +
        """')
          | select dept_code,trip_id,depart_time,plan_arrive_time,arrive_time,pre_arrive_time,weight,ticket,status,batch_code,batch_date,count_time
          | from resultAll""".stripMargin)
   /* resultBatch.foreachPartition(p=>{
      var connection = CloudTidbUtil.getConn()
      val sql = """replace into op_tp_wl_handover_batch_count_rt(id_key,event_type,dept_code,event_time,plan_arrive_time,trip_id,weight,ticket,count_time,count_date,batch_code,batch_date) values(?,?,?,?,?,?,?,?,?,?,?,?)"""
      val st = connection.prepareStatement(sql)
      p.foreach(r=>{
        var i = 1
        for(field <- r.schema.fields){
          val name = field.name
          val fType = field.dataType
          fType match {
            case StringType => st.setString(i,r.getAs[String](name))
            case LongType => st.setLong(i,r.getAs[Long](name))
            case DoubleType => st.setDouble(i,r.getAs[Double](name))
            case IntegerType => st.setInt(i,r.getAs[Integer](name))
            case TimestampType => st.setTimestamp(i,r.getAs[Timestamp](name))
            case BooleanType => st.setBoolean(i,r.getAs[Boolean](name))
          }
          i = i+1
        }
        st.executeUpdate()
      }
      )
      st.close()
      connection.close()
    })*/
  }

}
