package iot.spark.streaming

import iot.common.{Config, Log}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONObject
import iot.command.{DeviceResp, HwPara, Status}
import iot.repository.DeviceRepository
import org.apache.spark.streaming.mqtt.MQTTUtils

/**
  * Created by kiddlee on 17-3-19.
  */
object CommandReceiver  extends Config with Log {
  def main(args: Array[String]): Unit = {
    val mqtturl = getConfig("mqtt.url")
    val topic = getConfig("mqtt.topic")
    val evn = getConfig("env.name")
    val appName = evn + getConfig("spark.appname")
    DeviceRepository init()
    val conf = new SparkConf()
      .setAppName(appName)
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
    val ssc = new StreamingContext(conf, Seconds(5))

    val streamRdd = MQTTUtils.createStream(ssc, mqtturl, topic)

    val messages = streamRdd.filter(_.contains("\"code\":2170"))
    //messages.persist(StorageLevel.MEMORY_ONLY)

    try {
      val pairs = messages.map(x => {
        println(x)
        info("Get Message: " + x, getClass.getName)
        val message = JSON.parse(x).asInstanceOf[JSONObject]
        var key = "errorMessage"
        if(message != None) key = message.get("cmd").toString
        else error("Error Message: " + x, getClass.getName)
        (key, message)
      }).filter(x => x._1 != "errorMessage")
      pairs.persist(StorageLevel.MEMORY_ONLY)
      val groupRdd = pairs.groupByKey
      groupRdd.persist(StorageLevel.MEMORY_ONLY)
      groupRdd.foreachRDD(rdd => ({
        rdd.foreach(record => ({
          info("Receive Message: " + record._2, getClass.getName)
          record._1 match {
            case "status" => Status.process(record._2)
            case "device-resp" => DeviceResp.process(record._2)
            case "hw-para" => HwPara.process(record._2)
            case _ => info("Nothing for message: " + record._2, getClass.getName)
          }
        }))
      }))
    } catch {
      case ex: Exception => error(ex.getMessage, getClass.getName)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
