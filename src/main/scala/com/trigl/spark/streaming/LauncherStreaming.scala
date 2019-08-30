package com.trigl.spark.streaming

import java.text.SimpleDateFormat

import com.trigl.spark.util.{DataUtil, LauncherMultipleTextOutputFormat}
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

import scala.collection.mutable.ArrayBuffer

/**
  *Flume+Kafka+SparkStreaming进行实时日志分析
  * @author 邵洋
  */
object LauncherStreaming {
  val HDFS_DIR = "/changmi/launcher/click"
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkConf = new SparkConf().setAppName("LauncherStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(60))//每60秒一个批次
    // 从Kafka中读取数据
    val kafkaStream = KafkaUtils.createStream(
      ssc,
      "hxf:2181,cfg:2181,jqs:2181,jxf:2181,sxtb:2181", // Kafka集群使用的zookeeper
      "launcher-streaming", // 该消费者使用的group.id
      Map[String, Int]("launcher_click" -> 0, "launcher_click" -> 1), // 日志在Kafka中的topic及其分区
      StorageLevel.MEMORY_AND_DISK_SER).map(_._2) // 获取日志内容
    kafkaStream.foreachRDD((rdd: RDD[String], time: Time) => {
      val result = rdd.map(log => parseLog(log)) // 分析处理原始日志
        .filter(t => StringUtils.isNotBlank(t._1) && StringUtils.isNotBlank(t._2))
      result.saveAsHadoopFile(HDFS_DIR, classOf[String], classOf[String], classOf[LauncherMultipleTextOutputFormat[String, String]])
    })
    ssc.start()
    // 等待实时流
    ssc.awaitTermination()
  }

  // 分析日志
  def parseLog(log: String): (String, String) = {

    var data = log
    var key = ""
    try {
      val sb = new StringBuilder
      var idx = 0
      // 接收服务器获取的IP
      val ip = data.substring(0, log.indexOf("|"))
      data = data.substring(log.indexOf("|") + 1)

      var tss = data.substring(0, data.indexOf("|"))
      tss = tss.replaceAll("\\.", "")
      // 服务器时间戳，13位
      val serverTimestamp = tss.toLong
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      // 服务器时间
      val serverTime = df.format(serverTimestamp)
      data = data.substring(data.indexOf("|") + 1)
      val keyDf = new SimpleDateFormat("yyyy/MM/dd/")
      key = keyDf.format(serverTimestamp)

      // 主体数据：imei列表&品牌&机型|包名&文件crc&时间戳列表;包名&文件crc&时间戳列表;...
      data = data.replaceAll("\\*", "\\/")
      data = DataUtil.unzip(data)

      // 解压后的数据格式
      // 865066038753146-865066038753153&meizu&m5note|com.android.dialer&1c5441a9&1493109297305,1493110895807,1493112513536,1493112521588,1493112580015,1493112744680,1493138770296;cn.kuwo.player&d1d03a24&1493109460580;com.android.alarmclock&3a6bc24a&1493109495248,1493113574037,1493147103188;com.android.settings&e49b42d4&1493109801783,1493109835854,1493110147845,1493110230365,1493110320930,1493110344154,1493110350860,1493110796087,1493112348524,1493112850520,1493113933383,1493114212423,1493146022049,1493146963011;
      val dataArr = data.split("\\|")
      val phoneData = dataArr(0).split("&")
      // imei列表
      val imeis = phoneData(0).split("-").mkString(";")
      // 品牌
      val phoneBrand = phoneData(1)
      // 机型
      val phoneModel = phoneData(2)

      val appData = dataArr(1).split(";")
      val appArr = ArrayBuffer[String]()
      for (app <- appData) {
        val appDetail = app.split("&")
        // 时间戳列表
        val timestamps = appDetail(2).split(",")
        // 点击次数
        val clickTimes = timestamps.length
        appArr += app + "&" + clickTimes
      }

      // imei有数据，app有数据
      if (imeis.length > 0 && appArr.length > 0) {
        sb.append(imeis)
        sb.append("|")
        sb.append(if (StringUtils.isNotBlank(ip)) ip else " ")
        sb.append("|")
        sb.append(serverTimestamp)
        sb.append("|")
        sb.append(if (StringUtils.isNotBlank(serverTime)) serverTime else " ")
        sb.append("|")
        sb.append(if (StringUtils.isNotBlank(phoneBrand)) phoneBrand else " ")
        sb.append("|")
        sb.append(if (StringUtils.isNotBlank(phoneModel)) phoneModel else " ")
        sb.append("|")
        sb.append(appArr.mkString(";"))
      }

      data = sb.toString()

    } catch {
      case e: Exception =>
        e.printStackTrace()
        data = ""
    }

    (key, data)
  }

}



