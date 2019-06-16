package com.wanda.mob.spark.streaming.kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{KafkaCluster, KafkaUtils}

/**
  * Created by siyuan.tao.wb on 2017/10/18.
  */
object KafkaHelper {

  def loadTopicAndMessageFromKafka(ssc: StreamingContext,
                                   topics: String,
                                   kafkaParams: Map[String, String]
                                  ): InputDStream[(String, String)] = {

      val consumerOffsets = getFromOffsets(
        new KafkaCluster(kafkaParams),
        kafkaParams,
        topics.split(",").toSet)
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
        ssc,
        kafkaParams,
        consumerOffsets,
        //提取topic
        (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message))
  }

  private def getFromOffsets(kc: KafkaCluster,
                             kafkaParams: Map[String, String],
                             topics: Set[String]): Map[TopicAndPartition, Long] = {
    val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase)
    val result = for {
      topicPartitions <- kc.getPartitions(topics).right
      // largest/smallest
      leaderOffsets <- (if (reset == Some("smallest")) {
        kc.getEarliestLeaderOffsets(topicPartitions)
      } else {
        kc.getLatestLeaderOffsets(topicPartitions)
      }).right
    } yield {
      leaderOffsets.map { case (tp, lo) =>
        (tp, lo.offset)
      }
    }
    KafkaCluster.checkErrors(result)
  }
}
