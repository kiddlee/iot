package iot.spark.streaming

import iot.common.{Config, Log}
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONObject
import iot.command.Status

/**
  * Created by kiddlee on 17-3-19.
  */
object CommandReceiver  extends Config with Log {
  def main(args: Array[String]): Unit = {
    val zookeeper = getConfig("kafka.zookeeper")
    val broker = getConfig("kafka.broker")
    val group = getConfig("kafka.group")
    val topic = getConfig("kafka.topic")
    val hadoopAddress = getConfig("hadoop.address")
    val numThreads = getConfig("kafka.topic." + topic + ".numThreads").toInt
    val appName = getConfig("spark.appname")
    val conf = new SparkConf()
      .setAppName(appName)
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
    // Create a StreamingContext with a 1 second batch size
    val ssc = new StreamingContext(conf, Seconds(5))
    //    ssc.sparkContext.setLogLevel("OFF")
    // Create a map of topics to number of receiver threads to use

    // TODO: High-level
    val topics = Map(topic -> numThreads)
    //ssc.checkpoint("hdfs://" + hadoopAddress + "/commandstreaming/checkpoint")
    val streamRdd = KafkaUtils.createStream(ssc, zookeeper, group, topics)

    // TODO: Low-level
    /*
    val topics = Array(topic).toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> broker,
      "group.id" -> group,
      "zookeeper.connect" -> zookeeper
    )
    val streamRdd = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    */
    /*
    val storedOffset = readOffsets(topic, group, zookeeper)
    println("begin offset: " + storedOffset.toString)
    val streamRdd = storedOffset match {
      case None =>
        // start from the latest offsets
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
      case Some(fromOffsets) =>
        // start from previously saved offsets
        val messageHandler = (mmd:MessageAndMetadata[String, String]) => (mmd.key, mmd.message)
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    }
    */

    val messages = streamRdd.map(_._2).filter(_.contains("\"code\":5001"))
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
            case _ => info("Nothing for message: " + record._2, getClass.getName)
          }
        }))
      }))
      //saveOffsets(topic, group, zookeeper, streamRdd)
    } catch {
      case ex: Exception => error(ex.getMessage, getClass.getName)
    }

    // start our streaming context and wait for it to "finish"
    ssc.start()
    ssc.awaitTermination()
  }

  private def saveOffsets(topic:String, group: String, zookeeper: String, messages: InputDStream[(String, String)]): Unit = {
    var offsetRanges = Array[OffsetRange]()
    val topicDirs = new ZKGroupTopicDirs(group, topic)
    val sessionTimeoutMs = 10000
    val connectionTimeoutMs = 10000
    val zkClient: ZkClient = new ZkClient(zookeeper, connectionTimeoutMs, sessionTimeoutMs, ZKStringSerializer)
    messages.transform{rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.map(msg => msg._2).foreachRDD{rdd =>
      for(o <- offsetRanges) {
        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
        ZkUtils.updatePersistentPath(zkClient, zkPath, o.fromOffset.toString)  //将该 partition 的 offset 保存到 zookeeper
        info(s"@@@@@@ topic  ${o.topic}  partition ${o.partition}  fromoffset ${o.fromOffset}  untiloffset ${o.untilOffset} #######", getClass.getName)
        println(s"@@@@@@ topic  ${o.topic}  partition ${o.partition}  fromoffset ${o.fromOffset}  untiloffset ${o.untilOffset} #######")
      }
    }
    println("end offset: " + offsetRanges.toString)
  }

  private def readOffsets(topic:String, group: String, zookeeper: String): Option[Map[TopicAndPartition, Long]] = {
    val topicDirs = new ZKGroupTopicDirs(group, topic)
    val sessionTimeoutMs = 10000
    val connectionTimeoutMs = 10000
    val zkClient: ZkClient = new ZkClient(zookeeper, connectionTimeoutMs, sessionTimeoutMs, ZKStringSerializer)
    val children = zkClient.countChildren(s"${topicDirs.consumerOffsetDir}")
    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    println("zk count children: " + children)
    if(children > 0) {
      for(i <- 0 until children) {
        val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
        val tp = TopicAndPartition(topic, i)
        fromOffsets += (tp -> partitionOffset.toLong)
        println("@@@@@@ topic[" + topic + "] partition[" + i + "] offset[" + partitionOffset + "] @@@@@@")
        info("@@@@@@ topic[" + topic + "] partition[" + i + "] offset[" + partitionOffset + "] @@@@@@", getClass.getName)
      }
    }
    Option(fromOffsets)
  }
}
