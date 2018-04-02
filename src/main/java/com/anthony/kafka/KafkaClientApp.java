package com.anthony.kafka;

/**
 * @ Description:
 * @ Date: Created in 09:58 2018/4/1
 * @ Author: Anthony_Duan
 */
public class KafkaClientApp {

        public static void main(String[] args) {
            new KafkaProducer(KafkaProperties.TOPIC).start();

//        new KafkaConsumer(KafkaProperties.TOPIC).start();

        }
}
