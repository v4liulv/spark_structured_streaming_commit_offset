<h1>spark_structured_streaming_commit_offset</h1>

# 背景需求

当我们使用SparkStructuredStreaming消费Kafka数据时，已不通过Kafka维护Offset, 通过自定义的checkpoint自己维护offset。但是我们
希望可以通过Kafka监控其Offset, 消费情况等。那么就需要SparkStructuredStreaming消费Kafka数据时能提交offset，那么Kafka就能监控
到消费情况。

# 设计实现

通过StreamingQueryListener实现，StreamingQueryListener即监听StreamingQuery各种事件的接口，如下:

```scala
abstract class StreamingQueryListener {

  import StreamingQueryListener._

  // 查询开始时调用
  def onQueryStarted(event: QueryStartedEvent): Unit

  // 查询过程中状态发生更新时调用
  def onQueryProgress(event: QueryProgressEvent): Unit

  // 查询结束时调用
  def onQueryTerminated(event: QueryTerminatedEvent): Unit
}
```

可以在onQueryProgress事件（查询过程中状态发生更新时调用）拿到每个Source消费的Offset的。。因此，基于StreamingQueryListener，
可以将消费的offset的提交到kafka集群，进而实现对Kafka Lag的监控。

## 基于StreamingQueryListener向Kafka提交Offset

监控Kafka Lag的关键是能够向Kafka集群提交消费的Offset。

Java版本

```scala
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

```

然后在Spark调用下面监控即可

```scala
   spark.streams().addListener(new KafkaOffsetCommitterJava(kafkaBrokers, kafkaGroup));
```


 
