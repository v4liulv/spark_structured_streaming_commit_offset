package com.tencent.pomp.structured.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

/**
 * @author liulv
 * @since 1.0.0
 *
 *        说明：
 */
object ReadKafkaApp {
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {

    val kafkaBrokers="hadoop01:9092"
    val kafkaGroup="read_kafka_c2"
    val kafkaTopics1="topic01, topic02"
    val kafkaTopics2="test_3"
    val checkpointDir="D://spark/structured_streaming/checkpoint/"
    val queryName="read_kafka"

    val spark = SparkSession.builder().master("local[2]").appName(this.getClass.getSimpleName).getOrCreate()
    import spark.implicits._

    // 添加监听器
    val kafkaOffsetCommitter = new KafkaOffsetCommitter(kafkaBrokers, kafkaGroup)
    spark.streams.addListener(kafkaOffsetCommitter)

    // Kafka数据源1
    val inputTable1=spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",kafkaBrokers )
      .option("subscribe",kafkaTopics1)
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .select($"value")

    // Kafka数据源2
    val inputTable2=spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",kafkaBrokers )
      .option("subscribe",kafkaTopics2)
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .select($"value")

    // 结果表
    val resultTable = inputTable1.union(inputTable2)

    // 启动Query
    val query: StreamingQuery = resultTable
      .writeStream
      .format("console")
      .option("truncate","false")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .queryName(queryName)
      .option("checkpointLocation", checkpointDir)
      .start()

    spark.streams.awaitAnyTermination()

  }
}
