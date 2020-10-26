package com.bohai.app

import com.bohai.pojo.Task
import com.bohai.util.{ConfigurationManager, JdbcDatabaseUtils}
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Minutes, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  *
  * bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic bobiao_task --from-beginning
  * Created by liukai on 2020/10/12.
  */
object DemoApp {

  val kafka_server: String = ConfigurationManager.properties.getProperty("kafka.server")
  val kafkaTopicName: String = ConfigurationManager.properties.getProperty("kafka.topic")
  val checkPointPath: String = ConfigurationManager.properties.getProperty("spark.chechpoint")
  val spark: SparkSession = SparkSession.builder()
    .appName("DemoApp")
    .master("local[*]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .enableHiveSupport()
    .getOrCreate()

  val kafkaParams: Map[String, Object] = Map[String, Object](
    "bootstrap.servers" -> kafka_server,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "baobiao",
    "auto.offset.reset" -> "latest")

  def main(args: Array[String]): Unit = {
    val ssc = createStreamContext(spark, kafkaTopicName, kafkaParams)
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 配置streamingcontext上下文环境
    *
    * @param spark
    * @param kafkaTopicName
    * @param kafkaParams
    * @return
    */
  def createStreamContext(spark: SparkSession, kafkaTopicName: String, kafkaParams: Map[String, Object]): StreamingContext = {
    val ssc = new StreamingContext(spark.sparkContext, Minutes(1))
    ssc.checkpoint(checkPointPath)
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](Array(kafkaTopicName), kafkaParams)
    )
    val resultDStream = stream.map(_.value()).repartition(1).foreachRDD {
      rdd =>
        rdd.foreachPartition { partition =>
          partition.foreach(runTask(_))
        }
    }
    ssc
  }

  /**
    * 执行相应任务
    *
    * @param taskName
    */
  private def runTask(taskName: String): Unit = {
    val tasks = readTasks()
    val size = tasks.filter(_.task_name == taskName).size
    if (size > 0) {
      loadData(spark, "edw.ldm_card_pos_tran")
      //loadDataRDD(spark)
    } else {
      println("没有找到相应的匹配任务")
    }
  }

  /**
    * 从数据库读取任务配置信息
    *
    * @return
    */
  private def readTasks(): Seq[Task] = {
    val buffer = new ArrayBuffer[Task]()
    val sql = new StringBuilder
    sql ++= s"select task_id,task_name,table_name,dep_tables from task"
    JdbcDatabaseUtils.queryAll(sql.toString) { rs =>
      val task = new Task()
      task.task_id = rs.getLong(1)
      task.task_name = rs.getString(2)
      task.table_name = rs.getString(3)
      task.dep_tables = rs.getString(4)
      buffer += task
    }(JdbcDatabaseUtils.buildDataSource)
    buffer.toSeq
  }

  /**
    * 从hive加载数据把处理后的数据落地到hive仓库
    *
    * @param spark
    * @param tableName
    */
  private def loadData(spark: SparkSession, tableName: String) = {
    import spark.implicits._
    spark.sqlContext.sql("use edw")
    //spark.sqlContext.sql("show tables").show()
    val toIntUDF = udf((name: String) => name.toInt)
    spark.udf.register("to_number", toIntUDF)
    val v_org_branch_tmp = spark.sqlContext.sql("select * from edw.v_org_branch").select("org_id", "org_name", "org_upperid", "org_uppername").distinct()
    v_org_branch_tmp.createOrReplaceTempView("v_org_branch_tmp")
    val rps_card_info_tmp = spark.sqlContext.sql("select * from edw.o_rps_bad_cardinfo_his where CRD_STS <> 2 and CRD_KND <> '00000061'")
      .select("CRD_NO", "CRD_TYP", "CRD_KND", "AC", "CINO", "OPN_BR")
    //rps_card_info_tmp.show(10)
    val rps_card_knd_tmp = spark.sqlContext.sql("select * from edw.o_rps_bad_cardinfo_his").select("CRD_KND", "CRD_KND_NM").distinct()
    //rps_card_knd_tmp.show(10)

    rps_card_info_tmp.createOrReplaceTempView("rps_card_info_tmp")
    rps_card_knd_tmp.createOrReplaceTempView("rps_card_knd_tmp")

    val ldm_card_info_tmp1 = spark.sqlContext.sql("select a.CRD_NO as CRD_NO," +
      "a.CRD_TYP as CRD_TYP," +
      "a.CRD_KND as CRD_KND," +
      "b.CRD_KND_NM as CRD_KND_NM," +
      "a.AC as AC,a.CINO as CINO," +
      "a.OPN_BR as OPN_BR " +
      "from rps_card_info_tmp a " +
      "left join rps_card_knd_tmp b " +
      "on a.CRD_KND = b.CRD_KND")
    val crd_no_para = ldm_card_info_tmp1.select("CRD_NO")
    crd_no_para.createOrReplaceTempView("crd_no_para")
    ldm_card_info_tmp1.createOrReplaceTempView("ldm_card_info_tmp1")
    val m_mmp_fee_txn_tmp = spark.sqlContext.sql("select" +
      " a.STL_DT," +
      " a.FIL_NAM," +
      " a.ACP_BR," +
      " a.SED_BR," +
      " a.TRC_NO," +
      " a.TXN_TM," +
      " a.HIS_DW_DATE," +
      " a.FIL_TYP," +
      " a.TXN_REC," +
      " a.PRC_FLG," +
      " a.PAN," +
      " a.TXN_AMT," +
      " a.PTL_AMT," +
      " a.TXN_FEE," +
      " a.MSG_TYP," +
      " a.PRO_CD," +
      " a.MEC_TYP," +
      " a.TRM_ID," +
      " a.MRT_ID," +
      " a.REF_NO," +
      " a.POS_CD," +
      " a.ATH_CD," +
      " a.RCV_BR," +
      " a.ORI_TXN_NO," +
      " a.RSP_CD," +
      " a.PST_MD," +
      " a.GET_FEE," +
      " a.TKE_FEE," +
      " a.TXN_PRT_FEE," +
      " a.TXN_FLG," +
      " a.CRD_SEQ," +
      " a.ORI_TXN_TM," +
      " a.ISS_BR," +
      " a.TXN_RGN," +
      " a.TRM_TYP," +
      " a.ECI_FLG," +
      " a.PRT_FEE," +
      " a.PRT_NM," +
      " a.ORD_ID," +
      " a.PAY_MTH," +
      " a.TOKEN" +
      " from edw.m_mmp_fee_txn a" +
      " where ((a.MSG_TYP in ('0200', '0210') and substr(a.PRO_CD, 0, 2) in ('00') and  POS_CD in ('00', '08', '64', '65'))" +
      " or (a.MSG_TYP in ('0200', '0210', '0220', '0230') and substr(a.PRO_CD, 0, 2) in ('00') and POS_CD in ('06', '18'))" +
      " or (a.MSG_TYP in ('0220', '0230') and substr(a.PRO_CD, 0, 2) in ('20') and POS_CD in ('00', '69', '08', '64'))" +
      " or (a.MSG_TYP in ('0200', '0210', '0220') and substr(a.PRO_CD, 0, 2) in ('20') and POS_CD in ('00')))" +
      " and a.PAN in (select CRD_NO from crd_no_para)")

    m_mmp_fee_txn_tmp.createOrReplaceTempView("m_mmp_fee_txn_tmp")

    val m_mmp_fee_txn_tmp2 = spark.sqlContext.sql("select substr(a.TXN_PRT_FEE, 2) from m_mmp_fee_txn_tmp a")

    //m_mmp_fee_txn_tmp2.show(10)

    val m_mmp_fee_txn_tmp1 =
      spark.sqlContext.sql("select a.STL_DT as TX_DT," +
        "a.TXN_TM as TXN_TM," +
        "a.PAN as CRD_NO," +
        "b.CRD_TYP," +
        "b.CRD_KND," +
        "b.CRD_KND_NM," +
        "b.AC," +
        "b.CINO," +
        "b.OPN_BR," +
        "a.TRC_NO," +
        "case when a.MSG_TYP = '0200' and substr(a.PRO_CD, 0, 2) in ('00') and POS_CD in ('00', '08', '64', '65') then '1' " +
        "when a.MSG_TYP in ('0200', '0220') and substr(a.PRO_CD, 0, 2) in ('00') and POS_CD in ('06', '18') then '2' " +
        "when (a.MSG_TYP in ('0220', '0230') and substr(a.PRO_CD, 0, 2) in ('20') and POS_CD in ('00', '69', '08', '64') or a.MSG_TYP in ('0200', '0210', '0220') and substr(a.PRO_CD, 0, 2) in ('20') and POS_CD in ('00')) then '3' end as MSG_TYP," +
        "to_number(a.TXN_AMT) as TXN_AMT," +
        "case when a.Token is null then '0' else '1' end as TOKEN," +
        "case when a.SED_BR = '00010344' then '1' else '0' end as SED_BR," +
        "substr(a.ACP_BR, -4) as ACP_BR," +
        "a.MEC_TYP as MEC_TYP," +
        "case when substr(a.TXN_PRT_FEE, 0, 1) = 'C' then substr(a.TXN_PRT_FEE, 2) + a.GET_FEE else to_number(a.GET_FEE) end as GET_FEE," +
        "case when substr(a.TXN_PRT_FEE, 0, 1) = 'D' then substr(a.TXN_PRT_FEE, 2) + a.TKE_FEE else to_number(a.TKE_FEE) end as TKE_FEE " +
        "from m_mmp_fee_txn_tmp a left join ldm_card_info_tmp1 b on a.PAN =b.CRD_NO")

    println("-----------m_mmp_fee_txn_tmp1-------------")
    //m_mmp_fee_txn_tmp1.show(10)
    m_mmp_fee_txn_tmp1.createOrReplaceTempView("m_mmp_fee_txn_tmp1")

    val ldm_card_pos_tran_mid = spark.sqlContext.sql("select a.TX_DT," +
      " a.TXN_TM," +
      " a.CRD_NO," +
      " a.CRD_TYP," +
      " a.CRD_KND," +
      " a.CRD_KND_NM," +
      " a.AC," +
      " c.OPN_DT," +
      " a.CINO," +
      " a.OPN_BR," +
      " d.org_name," +
      " d.org_upperid," +
      " d.ORG_UPPERNAME," +
      " a.TRC_NO," +
      " a.MSG_TYP," +
      " a.TXN_AMT," +
      " a.Token," +
      " a.SED_BR," +
      " a.ACP_BR," +
      " a.MEC_TYP," +
      " a.GET_FEE," +
      " a.TKE_FEE" +
      " from m_mmp_fee_txn_tmp1 a" +
      " left join edw.m_rps_bad_depositacct c on a.ac = c.ac" +
      " left join v_org_branch_tmp d on a.OPN_BR = d.org_id")
      .withColumn("HIS_DW_DATE", lit("2020-02-28"))
    ldm_card_pos_tran_mid.createOrReplaceTempView("ldm_card_pos_tran_mid")

    val mp_onltranrechis_source_tmp = spark.sqlContext.sql("select CARDNO, SRVSTAN, DEVTRANSDATETIME, ADDDATA2, POSINPUTTYPE from m_mp_onltranrechis").distinct()
    mp_onltranrechis_source_tmp.createOrReplaceTempView("mp_onltranrechis_source_tmp")

    val ldm_card_pos_tran = spark.sqlContext.sql("select HIS_DW_DATE," +
      "TX_DT," +
      "TXN_TM," +
      "CRD_NO," +
      "CRD_TYP," +
      "CRD_KND," +
      "CRD_KND_NM," +
      "AC," +
      "OPN_DT," +
      "CINO," +
      "OPN_BR," +
      "ORG_NAME," +
      "ORG_UPPERID," +
      "ORG_UPPERNAME," +
      "TRC_NO," +
      "MSG_TYP," +
      "TXN_AMT," +
      "TOKEN," +
      "SED_BR," +
      "ACP_BR," +
      "MEC_TYP," +
      "GET_FEE," +
      "TKE_FEE," +
      "ADDDATA2," +
      "POSINPUTTYPE" +
      " from ldm_card_pos_tran_mid a left join mp_onltranrechis_source_tmp e on trim(a.Crd_No)=trim(e.Cardno) and trim(a.TRC_NO)=trim(e.SRVSTAN) and trim(a.TXN_TM)=trim(e.DEVTRANSDATETIME)"
    )
    ldm_card_pos_tran.checkpoint()
    //ldm_card_pos_tran.show(10)
    ldm_card_pos_tran.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
  }

  def loadDataRDD(spark: SparkSession): Unit = {
    import spark.implicits._
    spark.sqlContext.sql("use edw")
    val v_org_branch_tmp = spark.sqlContext.sql("select * from edw.v_org_branch")
      .select("org_id", "org_name", "org_upperid", "org_uppername")
      .distinct().cache()
    val branchs = v_org_branch_tmp.rdd.filter(row => row.getAs[String](0) == "01001022").collect()
    for (row: Row <- branchs) {
      println("v_org_branch: " + row.getString(0) + "," + row.getString(1) + "," + row.getString(2) + "," + row.getString(3))
    }
    val org_upperid = v_org_branch_tmp.rdd.mapPartitionsWithIndex({ (index, rdd) =>
      val buffer = new ListBuffer[(String)]()
      while (rdd.hasNext) {
        val org_upperid = rdd.next().getString(2)
        buffer += index + "_" + org_upperid
      }
      buffer.iterator
    }, true).persist(StorageLevel.MEMORY_AND_DISK)
    org_upperid.foreach(println(_))
    val m_mmp_fee_txn = spark.sqlContext.sql("select * from edw.m_mmp_fee_txn").distinct().rdd
    val rps_card_info_tmp = spark.sqlContext.sql("select * from edw.o_rps_bad_cardinfo_his where CRD_STS <> 2 and CRD_KND <> '00000061'")
      .select("CRD_NO", "CRD_TYP", "CRD_KND", "AC", "CINO", "OPN_BR")
    val rps_card_knd_tmp = spark.sqlContext.sql("select * from edw.o_rps_bad_cardinfo_his").select("CRD_KND", "CRD_KND_NM").distinct()
    rps_card_info_tmp.createOrReplaceTempView("rps_card_info_tmp")
    rps_card_knd_tmp.createOrReplaceTempView("rps_card_knd_tmp")
    val crd_no_para = spark.sparkContext.broadcast[Array[String]](
      spark.sqlContext.sql("select a.CRD_NO as CRD_NO " +
        "from rps_card_info_tmp a " +
        "left join rps_card_knd_tmp b " +
        "on a.CRD_KND = b.CRD_KND").rdd.collect().map(_.getString(0)))
    val m_mmp_fee_txn01 = m_mmp_fee_txn.filter { row =>
      var result = false
      var flagOne = false
      val msg_type = row.getAs[String]("msg_typ")
      val pro_cd = row.getAs[String]("pro_cd").substring(0, 2)
      val pos_cd = row.getAs[String]("pos_cd")
      val pan = row.getAs[String]("pan")
      if (Array[String]("0200", "0210").contains(msg_type) && pro_cd == "00" && Array[String]("00", "08", "64", "65").contains(pos_cd)) {
        flagOne = true
      }
      if (Array[String]("0200", "0210", "0220", "0230").contains(msg_type) && pro_cd == "00" && Array[String]("06", "18").contains(pos_cd)) {
        flagOne = true
      }
      if (Array[String]("0220", "0230").contains(msg_type) && pro_cd == "20" && Array[String]("00", "69", "08", "64").contains(pos_cd)) {
        flagOne = true
      }
      if (Array[String]("0200", "0210", "0220").contains(msg_type) && pro_cd == "20" && pos_cd == "00") {
        flagOne = true
      }
      if (flagOne && crd_no_para.value.contains(pan)) {
        result = true
      }
      result
    }
    println(m_mmp_fee_txn.count() + "," + m_mmp_fee_txn01.count())
  }

}
