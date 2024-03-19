package com.sf.realtime.spark.batch.main.forecast

import java.sql.Timestamp
import java.util.UUID

import com.sf.realtime.common.utils.DateUtil
import com.sf.realtime.spark.context.Context
import com.sf.realtime.spark.utils.CloudTidbUtil
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, LongType, StringType, TimestampType}

object WLHandoverCountTask {

  def main(args: Array[String]): Unit = {
    val startDay = args(0)
    val endDay = args(1)
    val spark = Context.getContext(true)
    val detailData = spark.sql("""select * from (select *,row_number() over(partition by eventtype,tripid,packageno order by eventtime) as rn from dm_freight.op_tp_wl_handover_detail_info where inc_day between '"""+startDay+"""' and '"""+endDay+"""')t where t.rn = 1 """).drop("rn")
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
    val result = spark.sql("""select id_key,event_type,dept_code,event_time,trip_id,weight,ticket,'"""+DateUtil.getDateStringWithoutHMS(0)+"""' as count_date,'"""+DateUtil.getDateString(0)+"""' as count_time from result_pre """)
    result.foreachPartition(p=>{
      var connection = CloudTidbUtil.getConn()
      val sql = """replace into op_tp_wl_handover_count_rt(id_key,event_type,dept_code,event_time,trip_id,weight,ticket,count_date,count_time) values(?,?,?,?,?,?,?,?,?)"""
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
    })
  }

}
