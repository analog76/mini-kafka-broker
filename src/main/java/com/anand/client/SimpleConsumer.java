package com.anand.client;


import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;


import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * Created by anand.ranganathan on 9/20/15.
 */

public class SimpleConsumer extends Thread{

    final static String clientId = "SimpleConsumerDemoClient";
    String TOPIC = null;
    ConsumerConnector consumerConnector;
    String zookeeper= null;


    public SimpleConsumer(String zookeeper, String topic){
        this.zookeeper = zookeeper;
        this.TOPIC = topic;
        Properties properties = new Properties();
        properties.put("zookeeper.connect",zookeeper);
        properties.put("group.id", "test-group");
        properties.put("auto.offset.reset", "smallest");

        ConsumerConfig consumerConfig = new ConsumerConfig(properties);
        consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);


    }

    public static void main(String[] argv) throws UnsupportedEncodingException {
        String TOPIC = "test-topic";
        String zookeeper= "localhost:2181";

        Thread T = new Thread(new HelloKafkaConsumer(zookeeper,TOPIC));
        T.start();

     }

    public SimpleConsumer(){
        Properties properties = new Properties();
        properties.put("zookeeper.connect",zookeeper);
        properties.put("group.id","test-group");
        properties.put("auto.offset.reset", "smallest");
        ConsumerConfig consumerConfig = new ConsumerConfig(properties);
        consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
    }

    @Override
    public void run() {

        System.out.println(" beginning to consume messages");
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(TOPIC, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream =  consumerMap.get(TOPIC).get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();


        while(it.hasNext()) {
            System.out.println("consumer reading " + new String(it.next().message()));
        }

        System.out.println(" Exiting and will not read any messsage ");

    }


}
