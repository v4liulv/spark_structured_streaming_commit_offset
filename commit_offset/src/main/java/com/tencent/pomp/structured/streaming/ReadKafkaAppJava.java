package com.tencent.pomp.structured.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author liulv
 * @since 1.0.0
 * <p>
 * 说明：
 */
public class ReadKafkaAppJava {
    private static final Logger logger = LoggerFactory.getLogger(ReadKafkaAppJava.class);

    public static AtomicLong atomic = new AtomicLong(1);

    public static void main(String[] args) throws Exception {
        String kafkaBrokers = "hadoop01:9092";
        String kafkaGroup = "read_kafka_c2";
        String kafkaTopic = "topic02";
        String queryName="read_kafka";
        String checkpointDir="D://spark/structured_streaming/checkpoint2/";
        final SparkSession spark = SparkSession.builder().master("local[2]").appName(ReadKafkaAppJava.class.getSimpleName()).getOrCreate();

        //spark.udf().register("cleanData", new CleanDataFunction());
        spark.streams().addListener(new CustomStreamingQueryListenerJava(kafkaBrokers, kafkaGroup));
        Dataset<Row> lines = spark
                .readStream()
                .format("kafka")
                .option("subscribe", kafkaTopic)
                .option("kafka.bootstrap.servers", kafkaBrokers)
                .load();

        lines.writeStream()
                .format("console")
                .option("truncate","false")
                .outputMode("append")
                .trigger(Trigger.ProcessingTime("2 seconds"))
                .queryName(queryName)
                .option("checkpointLocation", checkpointDir)
                .start();

        spark.streams().awaitAnyTermination();
    }

}
