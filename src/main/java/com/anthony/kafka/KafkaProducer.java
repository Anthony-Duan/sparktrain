package com.anthony.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * @ Description: KafkaProducer 实现
 * @ Date: Created in 08:54 2018/4/1
 * @ Author: Anthony_Duan
 */
public class KafkaProducer extends Thread {

    private String topic;

    private Producer<Integer,String> producer;

    public KafkaProducer(String topic){
        this.topic = topic;

        Properties properties = new Properties();
        properties.put("metadata.broker.list",KafkaProperties.BROLER_LIST);
        properties.put("serializer.class","kafka.serializer.StringEncoder");
        properties.put("request.required.acks","1");

        producer = new Producer<Integer, String>(new ProducerConfig(properties));
    }

    @Override
    public void run() {
        int messageNo = 1;

        while(true) {
            String message = "message_" + messageNo;
            producer.send(new KeyedMessage<Integer, String>(topic, message));
            System.out.println("Sent: " + message);

            messageNo ++ ;

            try{
                Thread.sleep(2000);
            } catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}
