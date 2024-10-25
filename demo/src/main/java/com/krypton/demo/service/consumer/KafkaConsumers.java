package com.krypton.demo.service.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Service
public class KafkaConsumers {

    private Logger log = LoggerFactory.getLogger(this.getClass());

    @KafkaListener(topics = {"letscreatenewtopic"}, groupId = "group1") // abstraction over consumer api
    public void consumeMessages(ConsumerRecord consumerRecord){
        log.info("consumed message for topic: {}, message: {}",consumerRecord.topic(),consumerRecord.value());
    }
    @KafkaListener(topics = {"letscreatenewtopic"}, groupId = "group1")
    public void consumeMessages2(ConsumerRecord consumerRecord){
        log.info("consumed message2 for topic: {}, message: {}",consumerRecord.topic(),consumerRecord.value());
    }

//    void customConsumer(){
//        Properties props = new Properties();
//        props.put("bootstrap.servers", "localhost:9092");
//        props.put("group.id", "my-group");
//        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//
//        // Create Kafka consumer
//        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
//
//        // Subscribe to the topic
//        consumer.subscribe(Collections.singletonList("my_topic"));
//
//        // Poll for new data
//        while (true) {
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
//            for (ConsumerRecord<String, String> record : records) {
//                System.out.printf("Consumed record with key %s and value %s from partition %d at offset %d%n",
//                        record.key(), record.value(), record.partition(), record.offset());
//            }
//        }
//    }
}

