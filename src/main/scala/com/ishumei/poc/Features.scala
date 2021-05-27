package com.ishumei.poc

import java.text.SimpleDateFormat

import com.ishumei.poc.utils.SparkUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/*
 * @Author: zhangmengnan
 * @Date 19:33 2021/5/27
 * @Description //TODO 
 *
 */

object Features {

  def tsToDT(ts: Long) = {
    (ts - 1546272000) / 86400
  }
  def tsToHour(ts: Long) = {
    (ts - 1546272000) / 3600
  }

  def dateToDT(date: String, patten: String): Long ={
    (new SimpleDateFormat(patten).parse(date).getTime/1000 - 1546272000) / 86400
  }




  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkUtils.getSpark()

    spark.udf.register("dateToDT", dateToDT _)
    spark.udf.register("tsToDT", tsToDT _)
    spark.udf.register("tsToHour", tsToHour _)

    val black_schema:StructType = StructType(
      Array(
        StructField("id",LongType,true),
        StructField("requestId",StringType,true),
        StructField("eventId",StringType,true),
        StructField("tokenId",StringType,true),
        StructField("timestamp",LongType,true),
        StructField("data",StringType,true),
        StructField("riskLevel",StringType,true),
        StructField("part_dt",StringType,true)
      )
    )
    //    val all_black_his: DataFrame = spark.read
    //      .option("header",true)
    //            .option("delimiter","\t")
    //            .schema(black_schema)
    //      .csv("C:\\workspace\\shumei\\file\\work\\交付\\20210520_建模数据\\data\\black")

    //    all_black_his.filter("get_json_object(data,'$.verfyEvent')='findLoginPassWD'").show(false)
    val df: DataFrame = spark.read
      .option("header",true)
      .option("delimiter","\t")
      //      .schema(black_schema)
      .csv("C:\\workspace\\shumei\\file\\work\\交付\\20210520_建模数据\\before.csv")

    //    df.repartition(1).write.option("header",true)
    //        .csv("poc")

    //    val df = dfs.union(dfs)
    df.printSchema()

    df.show(false)
    //    df.filter("timestamp >1700000000000").show(false)

    //    ff.foreach(x=>{println(x._1,x._2)})

    df.createOrReplaceTempView("t")

    spark.sql(
      """
        |select
        | *,
        | get_json_object(data,'$.deviceId') as deviceid,
        | tsToDT(cast(timestamp as bigint)/1000) as event_dt,
        | tsToHour(cast(timestamp as bigint)/1000) as event_hour,
        | get_json_object(data,'$.verfyEvent') as verfyEvent,
        | get_json_object(data,'$.verifyResult') as verifyResult,
        | get_json_object(data,'$.verifyType') as verifyType
        |from
        |t
      """.stripMargin)
      .createOrReplaceTempView("resource")

    //    val rig: DataFrame = spark.read
    //      .option("delimiter",",")
    //      .option("header",true)
    //      .csv("C:\\workspace\\shumei\\file\\work\\交付\\20210520_建模数据\\data\\firstReq.txt")
    ////
    ////    val rig: DataFrame = spark.createDataFrame()
    //    rig.createOrReplaceTempView("rig")
    //    rig.show(false)
    //
    //    spark.sql(
    //      """
    //        |select
    //        | rig.requestId as req,rig.timestamp as ts,
    //        | get_json_object(data,'$.deviceId') as deviceid,
    //        | tsToDT(cast(lef.timestamp as bigint)/1000) as event_dt,
    //        | tsToHour(cast(lef.timestamp as bigint)/1000) as event_hour,
    //        | get_json_object(data,'$.verfyEvent') as verfyEvent,
    //        | get_json_object(data,'$.verifyResult') as verifyResult,
    //        | get_json_object(data,'$.verifyType') as verifyType,
    //        | lef.*
    //        |from
    //        |(select * from t) as lef
    //        |left join
    //        |rig
    //        |on lef.requestId=rig.requestId
    //      """.stripMargin)
    //      .createOrReplaceTempView("resource")
    //      .show(100,false)


    //    spark.sql(
    //      """
    //        |select lef.*,ts,req,tsToDT(cast(timestamp as bigint)/1000) as event_dt,tsToHour(cast(timestamp as bigint)/1000) as event_hour,
    //        |get_json_object(data,'$.verfyEvent') as verfyEvent,
    //        |get_json_object(data,'$.verifyResult') as verifyResult,
    //        |get_json_object(data,'$.verifyType') as verifyType
    //        |
    //        | from (
    //        |select * from t where tokenid in ('c00328417699', 'c00386091734')
    //        |) as lef left join rig on lef.tokenid = rig.tokenid
    //      """.stripMargin)
    ////      .drop("timestamp")
    ////      .withColumnRenamed("timestamps", "timestamp")
    //      .createOrReplaceTempView("resource")
    //
    spark.sql(
      """
        |select * from resource
      """.stripMargin).show(false)

    spark.sql(
      """
        |select * from resource where verfyEvent='findLoginPassWD' and verifyType='CARD'
      """.stripMargin).show(false)

    // 最大
    spark.sql(
      """
        |select
        |tokenid,
        |max(findloginpasswd_by_card_ratio_largest_7d_per_tokenid) as findloginpasswd_by_card_ratio_largest_7d_per_tokenid,
        |max(findloginpasswd_by_card_ratio_largest_1d_per_tokenid) as findloginpasswd_by_card_ratio_largest_1d_per_tokenid,
        |max(findloginpasswd_by_card_ratio_largest_1h_per_tokenid) as findloginpasswd_by_card_ratio_largest_1h_per_tokenid
        |from
        |(
        |select
        | tokenid,timestamp,flag,event_dt,event_hour,
        |count(if(verifyType=='CARD' and verfyEvent=='findLoginPassWD',1,null)) over(partition by tokenid order by event_dt range between 6 preceding and 0 following)/count(if(verfyEvent=='findLoginPassWD',1,null)) over(partition by tokenid order by event_dt range between 6 preceding and 0 following)  as findloginpasswd_by_card_ratio_largest_7d_per_tokenid,
        |count(if(verifyType=='CARD' and verfyEvent=='findLoginPassWD',1,null)) over(partition by tokenid order by event_dt range between 0 preceding and 0 following)/count(if(verfyEvent=='findLoginPassWD',1,null)) over(partition by tokenid order by event_dt range between 0 preceding and 0 following)  as findloginpasswd_by_card_ratio_largest_1d_per_tokenid,
        |count(if(verifyType=='CARD' and verfyEvent=='findLoginPassWD',1,null)) over(partition by tokenid order by event_hour range between 0 preceding and 0 following)/count(if(verfyEvent=='findLoginPassWD',1,null)) over(partition by tokenid order by event_dt range between 0 preceding and 0 following)  as findloginpasswd_by_card_ratio_largest_1h_per_tokenid
        |from
        |resource
        |) group by tokenid
      """.stripMargin
    ).filter("findloginpasswd_by_card_ratio_largest_7d_per_tokenid>0").show(200,false)

    // 最近
    spark.sql(
      """
        |select * from
        |(
        |select
        | tokenid,timestamp,flag,event_dt,event_hour,
        |count(if(verifyType=='CARD' and verfyEvent=='findLoginPassWD',1,null)) over(partition by tokenid order by event_dt range between 6 preceding and 0 following)/count(if(verfyEvent=='findLoginPassWD',1,null)) over(partition by tokenid order by event_dt range between 6 preceding and 0 following)  as findloginpasswd_by_card_ratio_latest_7d_per_tokenid,
        |count(if(verifyType=='CARD' and verfyEvent=='findLoginPassWD',1,null)) over(partition by tokenid order by event_dt range between 0 preceding and 0 following)/count(if(verfyEvent=='findLoginPassWD',1,null)) over(partition by tokenid order by event_dt range between 0 preceding and 0 following)  as findloginpasswd_by_card_ratio_latest_1d_per_tokenid,
        |count(if(verifyType=='CARD' and verfyEvent=='findLoginPassWD',1,null)) over(partition by tokenid order by event_hour range between 0 preceding and 0 following)/count(if(verfyEvent=='findLoginPassWD',1,null)) over(partition by tokenid order by event_dt range between 0 preceding and 0 following)  as findloginpasswd_by_card_ratio_latest_1h_per_tokenid
        |from
        |resource
        |) f
        |where flag='1'
      """.stripMargin
    ).filter("findloginpasswd_by_card_ratio_latest_7d_per_tokenid>0").show(200,false)




  }

}
