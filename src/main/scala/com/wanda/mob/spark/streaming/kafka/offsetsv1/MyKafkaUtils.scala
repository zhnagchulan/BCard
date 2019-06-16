package com.wanda.mob.spark.streaming.kafka.offsetsv1

import java.util

import com.wanda.mob.spark.streaming.kafka.KafkaHelper
import kafka.common._
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{KafkaUtils, OffsetRange}
/**
  *
  * kafka offset 管理
  */
object MyKafkaUtils {
  /**
    * 根据判断逻辑创建kafkaStream：从zookeeper中程序维护的偏移量创建stream 或 从分区中的最小/大偏移量创建stream
    * 注: 对比zookeeper中维护的各offset值与对应kafka partition中最小offset值的大小。
    * （try catch OffsetOutOfRangeException 无法解决）
    *
    * @param ssc
    * @param kafkaParams
    * @param client
    * @param group
    * @param topic
    * @return
    */

  def createKafkaStream(ssc: StreamingContext, kafkaParams: Map[String, String], client: ZkClient, group: String, topic: String,
                        brokerList: String): InputDStream[(String, String)] = {

    val storedOffsets: Map[TopicAndPartition, Long] = readStoredOffsets(client, group, topic)
    var flag = storedOffsets.isEmpty

    val list = new util.ArrayList[String]()

    topic.split(",").foreach(topic=> list.add(topic))

    if (!flag) { // false : zookeeper中人工维护了offsets
//      val earliestOffsets = MyKafkaOffsetUtils.getInstance().getEarliestOffset(brokerList, list, group)
//      val lastOffsets = MyKafkaOffsetUtils.getInstance().getLastOffset(brokerList, list, group)

      // 判断维护的 offset 值是否在各 kafka partition 当前的最小和最大 offset 范围内
      storedOffsets.foreach(item => {
        val topicAndPartition = item._1
        val offset = item._2
//        println("里面的是"+topicAndPartition,offset)
//        if (offset < earliestOffsets(topicAndPartition) || offset > lastOffsets(topicAndPartition))
          flag = true
      })
    }

    if (flag) { // 从kafka各partition的最小offset创建stream
      println("start from the smallest offsets")
      //      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topic.split(",").toSet)
      KafkaHelper.loadTopicAndMessageFromKafka(
        ssc,
        topic,
        kafkaParams)
    } else { // 从保存在zookeeper中的offset创建stream
      println("start from previously saved offsets")
      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message)
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder,
        (String, String)](ssc, kafkaParams, storedOffsets, messageHandler)
    }
  }

  /**
    * 获取保存在 zookeeper 中的偏移量
    *
    * @param client
    * @param group
    * @param topic
    * @return Map[TopicAndPartition, Long]，若保存有记则非空。若无保存记录，则返回空Map
    */
  private def readStoredOffsets(client: ZkClient, group: String, topic: String): Map[TopicAndPartition, Long] = {
    var storedOffsets: Map[TopicAndPartition, Long] = Map()

    topic.split(",").foreach(topic => {
      val topicDirs = new ZKGroupTopicDirs(group, topic)
      val topicPath = s"${topicDirs.consumerOffsetDir}"
      val partitionIDs = client.countChildren(topicPath)

      if (partitionIDs > 0)
        for (i <- 0 until partitionIDs) {
          val partitionOffset = client.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
          val tp = TopicAndPartition(topic, i)
          storedOffsets += (tp -> partitionOffset.toLong)
        }
    })
    storedOffsets
  }

  /**
    * 维护偏移量，保存fromOffset到zookeeper
    *
    * @param client
    * @param group
    * @param topic
    * @param offsetsRanges
    */
  def saveFromOffsets(client: ZkClient, group: String, topic: String, offsetsRanges: Array[OffsetRange]): Unit = {

    //...
    val topicDirs = new ZKGroupTopicDirs(group, topic)

    //    val offsetsRangesStr = offsetsRanges.
    //      map(offsetRange => s"${offsetRange.partition}:${offsetRange.fromOffset}").
    //      mkString(",")
    //    ZkUtils.updatePersistentPath(client, path, offsetsRangesStr)

    for (offset <- offsetsRanges) {
      val path = s"${topicDirs.consumerOffsetDir}/${offset.partition}"
      ZkUtils.updatePersistentPath(client, path, offset.fromOffset.toString) //将该 RDD partition 的 fromOffset 保存到 zookeeper
    }
    //...
  }

  /**
    * 维护偏移量，保存untilOffset到zookeeper
    *
    * @param client
    * @param group
    * @param topic
    * @param offsetsRanges
    */
  def saveUntilOffsets(client: ZkClient, group: String, topic: String, offsetsRanges: Array[OffsetRange]): Unit = {
    println("update offset ")
    for (offset <- offsetsRanges) {
      val topicDirs = new ZKGroupTopicDirs(group, offset.topic)
      val path = s"${topicDirs.consumerOffsetDir}/${offset.partition}"
      ZkUtils.updatePersistentPath(client, path, offset.untilOffset.toString)
//      println(s"@@@@@@ topic  ${offset.topic}  partition ${offset.partition}  fromoffset ${offset.fromOffset}  untiloffset ${offset.untilOffset} #######")
    }
  }

}