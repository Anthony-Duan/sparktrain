package com.anthony.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @ Description: KafkaConsumer 实现
 * @ Date: Created in 10:46 2018/4/2
 * @ Author: Anthony_Duan
 */
public class KafkaConsumer extends Thread{

    private String topic;

    public KafkaConsumer(String topic){
        this.topic = topic;

    }

    private ConsumerConnector consumerConnector(){

        Properties properties = new Properties();
        properties.put("zookeeper.connect",KafkaProperties.ZK);
        properties.put("group.id",KafkaProperties.GROUP_ID);

        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));

    }

    @Override
    public void run() {
        ConsumerConnector consumer = consumerConnector();

        Map<String,Integer> topicCountMap = new HashMap<String, Integer>();

        topicCountMap.put(topic,1);

        //这里通过进入源码看返回值类型
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(topicCountMap);
        //通过上面的类型推断出这个类型
        KafkaStream<byte[], byte[]> stream = messageStreams.get(topic).get(0);

        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();

        while (iterator.hasNext()){
            String message = new String(iterator.next().message());
            System.out.println("rec:"+ message);
        }
    }
}
