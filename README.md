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

```java
package com.tencent.pomp.structured.streaming;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.sql.streaming.SourceProgress;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author liulv
 * @since 1.0.0
 */
public class KafkaOffsetCommitterJava extends StreamingQueryListener {

    /**
     * logger.
     */
    private static final Logger logger = LoggerFactory.getLogger(KafkaOffsetCommitterJava.class);

    /**
     * 初始化KafkaConsumer.
     */
    private static KafkaConsumer kafkaConsumer = null;

    public KafkaOffsetCommitterJava(String brokers, String group){
        Properties properties= new Properties();
        {
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            kafkaConsumer = new KafkaConsumer(properties);
        }

    }

    @Override
    public void onQueryStarted(QueryStartedEvent event) {
        logger.info("Started query with id : {}, name: {},runId : {}", event.id(), event.name(), event.runId().toString());
    }

    @Override
    public void onQueryProgress(QueryProgressEvent event) {
        logger.info("Streaming query made progress: {}", event.progress().prettyJson());
        for (SourceProgress sourceProgress : event.progress().sources()) {
            Map<String, Map<String, String>> endOffsetMap = JSONObject.parseObject(sourceProgress.endOffset(), Map.class);
            for (String topic : endOffsetMap.keySet()) {
                Map<TopicPartition, OffsetAndMetadata> topicPartitionsOffset = new HashMap<>();
                Map<String, String> partitionMap = endOffsetMap.get(topic);
                for (String partition : partitionMap.keySet()) {
                    TopicPartition topicPartition = new TopicPartition(topic, Integer.parseInt(partition));
                    OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(Long.parseLong(String.valueOf(partitionMap.get(partition))));
                    topicPartitionsOffset.put(topicPartition, offsetAndMetadata);
                }
                logger.info("【commitSync offset】topicPartitionsOffset={}", topicPartitionsOffset);
                kafkaConsumer.commitSync(topicPartitionsOffset);
            }
        }
    }

    @Override
    public void onQueryTerminated(QueryTerminatedEvent event) {
        logger.info("Stream exited due to exception : {}, id : {}, runId: {}", event.exception().toString(), event.id(), event.runId());
    }

}
```

然后在Spark调用下面监控即可

```scala
   spark.streams().addListener(new KafkaOffsetCommitterJava(kafkaBrokers, kafkaGroup));
```


 
