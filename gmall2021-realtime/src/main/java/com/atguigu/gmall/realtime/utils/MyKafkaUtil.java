package com.atguigu.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class MyKafkaUtil {
    private static String brolerList = "hadoop102:9092,hadoop103:9092";
    private static String defaultTopic = "DWD_DEFAULT_TOPIC";

    //TODO 获取卡夫卡生产者
    public static FlinkKafkaProducer<String> getProducer(String topic) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brolerList);
        return new FlinkKafkaProducer<String>(
            topic,
            new SimpleStringSchema(),
                props
        );
    }

    public static <T> FlinkKafkaProducer<T> getProducer(KafkaSerializationSchema kafkaSerializationSchema) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brolerList);
        return new FlinkKafkaProducer<T>(
                defaultTopic,
                kafkaSerializationSchema,
                props,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }


    //TODO 获取kafka消费者
    public static FlinkKafkaConsumer<String> getConsumer(String topic ,String groupId) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brolerList);
        return new FlinkKafkaConsumer<String>(
                topic,
                new SimpleStringSchema(),
                props
        );
    }
}
