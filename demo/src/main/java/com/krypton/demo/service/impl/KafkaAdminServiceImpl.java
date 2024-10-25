package com.krypton.demo.service.impl;

import com.krypton.demo.bean.Broker;
import com.krypton.demo.bean.KafkaTopic;
import com.krypton.demo.bean.KafkaView;
import com.krypton.demo.bean.Partition;
import com.krypton.demo.service.KafkaAdminService;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;

@Service
public class KafkaAdminServiceImpl implements KafkaAdminService {
    @Autowired
    KafkaAdmin kafkaAdmin; //abstraction over admin api
    private Logger log = LoggerFactory.getLogger(this.getClass());


    public String createTopic(String topic, Integer replicationFactor, Integer partitions){
        NewTopic newTopic= new NewTopic(topic,partitions,replicationFactor.shortValue());
        kafkaAdmin.createOrModifyTopics(newTopic);
        return "topic created";
    }
    public String deleteTopic(String topicName) {
        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            // Delete the topic
            adminClient.deleteTopics(Collections.singletonList(topicName)).all().get();
            log.info("Topic deleted successfully: " + topicName);
            return "Topic deleted successfully: " + topicName;
        } catch (ExecutionException | InterruptedException e) {
            log.error("Failed to delete topic: " + e.getMessage());
            Thread.currentThread().interrupt();
            return "Failed to delete topic: " + e.getMessage();
        }
    }

//    void customAdmin(){
//        Properties props = new Properties();
//        props.put("bootstrap.servers", "localhost:9092");
//
//        // Create AdminClient
//        AdminClient adminClient = AdminClient.create(props);
//
//        // Create a new topic
//        NewTopic newTopic = new NewTopic("my_new_topic", 1, (short) 1);
//        adminClient.createTopics(Arrays.asList(newTopic)).all().get();
//
//        log.info("Topic created successfully!");
//
//        // Close the admin client
//        adminClient.close();
//    }

    public KafkaView displayKafkaInfo() throws ExecutionException, InterruptedException {
        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            KafkaView kafkaView= new KafkaView();

            DescribeClusterResult clusterResult = adminClient.describeCluster();
            log.info("Cluster ID: " + clusterResult.clusterId().get());
            kafkaView.setClusterId(clusterResult.clusterId().get());
            List<Broker> brokerList = new ArrayList<>();
            List<KafkaTopic> topics =new ArrayList<>();

            log.info("Number of Brokers: " + clusterResult.nodes().get().size());
            kafkaView.setBrokerCount(clusterResult.nodes().get().size());
            for (Node broker : clusterResult.nodes().get()) {
                log.info("\tBroker ID: " + broker.id() + ", Host: " + broker.host() + ", Port: " + broker.port());
                Broker broker1= new Broker();
                broker1.setBrokerId(broker.id());
                broker1.setHost(broker.host());
                broker1.setPort(broker.port());
                brokerList.add(broker1);

            }
            kafkaView.setBrokers(brokerList);

            ListTopicsResult listTopicsResult = adminClient.listTopics();
            for (String topicName : listTopicsResult.names().get()) {
                log.info("\nTopic: " + topicName);
                KafkaTopic kafkaTopic=new KafkaTopic();
                kafkaTopic.setName(topicName);

                DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(listTopicsResult.names().get());
                TopicDescription topicDescription = describeTopicsResult.values().get(topicName).get();
                List<Partition> partitions=new ArrayList<>();

                for (TopicPartitionInfo partitionInfo : topicDescription.partitions()) {
                    Partition partition =new Partition();
                    log.info("\tPartition: " + partitionInfo.partition());
                    log.info("\tLeader: " + (partitionInfo.leader()!=null ? partitionInfo.leader().id(): "NA"));
                    log.info("\tReplicas: " + partitionInfo.replicas().size());
                    log.info("\tIsr: " + partitionInfo.isr().size());
                    partition.setId(partitionInfo.partition());
                    partition.setLeader((partitionInfo.leader()!=null ? partitionInfo.leader().id(): -1));
                    partition.setReplica(partitionInfo.replicas().size());
                    partition.setIsr(partitionInfo.isr().size());
                    partitions.add(partition);


                }
                kafkaTopic.setPartitions(partitions);
                topics.add(kafkaTopic);

            }
            kafkaView.setKafkaTopics(topics);
            kafkaView.setTopicCount(topics.size());
            return kafkaView;
        }
    }
}
