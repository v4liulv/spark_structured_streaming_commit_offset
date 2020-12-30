package com.tencent.pomp.structured.streaming

import java.util
import java.util.Properties

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import lombok.extern.slf4j.Slf4j
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener._
import org.slf4j.LoggerFactory

/**
 * @author liulv
 * @since 1.0.0
 *
 *        说明：
 */
@Slf4j
class KafkaOffsetCommitter(brokers: String, group: String) extends StreamingQueryListener{

  // Kafka配置
  val properties= new Properties()
  properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  properties.put(ConsumerConfig.GROUP_ID_CONFIG, group)
  properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  val kafkaConsumer = new KafkaConsumer[String, String](properties)

  def onQueryStarted(event: QueryStartedEvent): Unit = {}

  def onQueryTerminated(event: QueryTerminatedEvent): Unit = {}

  // 提交Offset
  def onQueryProgress(event: QueryProgressEvent): Unit = {
    val log = LoggerFactory.getLogger(this.getClass)

    // 遍历所有Source
    event.progress.sources.foreach(source=>{

      val objectMapper = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(DeserializationFeature.USE_LONG_FOR_INTS, true)
        .registerModule(DefaultScalaModule)

      val endOffset = objectMapper.readValue(source.endOffset,classOf[Map[String, Map[String, Long]]])

      // 遍历Source中的每个Topic
      for((topic,topicEndOffset) <- endOffset){
        val topicPartitionsOffset = new util.HashMap[TopicPartition, OffsetAndMetadata]()

        //遍历Topic中的每个Partition
        for ((partition,offset) <- topicEndOffset) {
          val topicPartition = new TopicPartition(topic, partition.toInt)
          val offsetAndMetadata = new OffsetAndMetadata(offset)
          topicPartitionsOffset.put(topicPartition,offsetAndMetadata)
        }

        log.warn(s"提交偏移量... Topic: $topic Group: $group Offset: $topicEndOffset")
        kafkaConsumer.commitSync(topicPartitionsOffset)
      }
    })
  }
}
